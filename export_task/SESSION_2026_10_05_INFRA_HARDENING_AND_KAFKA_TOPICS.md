# SESSION EXPORT — Infra Hardening, Credential Sync & Kafka Topic Provisioning
**Date:** 2026-10-05  
**Branch:** 03102026_debug_infra  
**Scope:** `infra_dataops_finance_trading/docker-compose.yml`, `.gitignore`, `docs/`, `dataops_batch_api/cli/ingestion/sources/tcbs_client.py`  
**References:**
- `export_task/SESSION_2026_10_04_DOCKER_DEBUG_AND_TEST_PLAN.md`
- `github/copilotinstructions/SKILL.md`
- `github/agent/realtime-streaming-engineer.agent.md`

---

## 1. MỤC TIÊU SESSION

1. Đồng bộ toàn bộ credentials trong `docker-compose.yml` khớp với `.env`.
2. Hardening `.gitignore` — block `.env` và `docker-compose.yml` khỏi git.
3. Điều chỉnh RAM limit tổng ~4 GiB cho toàn bộ stack.
4. Rewrite `docs/TEST_PLAN_BINANCE_E2E.md` — bổ sung ma trận container bật/tắt, xử lý TCBS-optional.
5. Tạo `docs/TCBS_AUTH_GUIDE.md` — hướng dẫn lấy JWT token từ TCBS iFlash API.
6. Fix `dataops-redis` unhealthy — healthcheck `$$REDIS_PASSWORD` không expand trong container.
7. Thiết kế và tự động hóa Kafka topic provisioning với retention policy qua `kafka-init` service.

---

## 2. FIXES ĐÃ THỰC HIỆN

### FIX-1 — `docker-compose.yml`: Credential Sync với `.env`

**11 điểm sai được tìm thấy và sửa:**

| # | Service | Lỗi | Fix |
|---|---|---|---|
| A | `mysql` healthcheck | `-p$$dataops_fintrading123` hardcoded | `-p$$MYSQL_ROOT_PASSWORD` (shell expand) |
| B | `mysql` environment | Thiếu `MYSQL_USER`, `MYSQL_PASSWORD` | Thêm 2 field, default khớp `.env` |
| C | `redis` command | Không có password | `--requirepass ${REDIS_PASSWORD:-...}` |
| D | `redis` healthcheck | `redis-cli ping` no auth | `redis-cli -a ${REDIS_PASSWORD:-...} ping \| grep PONG` |
| E | `influxdb` | `INFLUXDB_USER/-PASSWORD` sai var name | `DOCKER_INFLUXDB_INIT_USERNAME/PASSWORD` |
| F | `influxdb` | `INFLUXDB_TOKEN` default sai | Map `DOCKER_INFLUXDB_INIT_ADMIN_TOKEN` |
| G | `grafana` healthcheck | `localhost:8080/api/v1/health` (backend!) | `localhost:3000/api/health` |
| H | `grafana` env | `GRAFANA_USER/PASSWORD` sai var name | `GF_SECURITY_ADMIN_USER/PASSWORD` |
| I | `stream-producer` | `REDIS_PASSWORD: ""` | `${REDIS_PASSWORD:-dataops_fintrading@123}` |
| J | `backend-api` | `MYSQL_DNS: root:root123@...` (root user!) | `${MYSQL_DNS}` (prod user từ `.env`) |
| K | `batch-api` | `AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER:-minioadmin}` | `${AWS_ACCESS_KEY_ID:-prod_minio_admin}` |

---

### FIX-2 — `docker-compose.yml`: RAM Limits ~4 GiB tổng

Dựa trên `docker stats --no-stream` thực tế (WSL2 host 4.8 GiB RAM, 2 CPU):

| Service | Actual RAM | New Limit |
|---|---|---|
| kafka1 | 471 MiB | 512M |
| kafka2 | 98 MiB | 256M |
| kafka3 | 472 MiB | 512M |
| schema-registry | 146 MiB | 256M |
| kafka-ui | 255 MiB | 256M |
| minio | 92 MiB | 128M |
| mysql | 369 MiB | 448M |
| redis | 8 MiB | 96M |
| influxdb | 65 MiB | 128M |
| grafana | 78 MiB | 96M |
| spark-master | 213 MiB | 384M |
| spark-worker | 246 MiB | 512M |
| stream-producer | ~100M est | 160M |
| backend-api | ~150M est | 192M |
| batch-api | ~200M est | 256M |
| **TOTAL** | **≈2.9 GiB actual** | **4092 MiB limit** |

---

### FIX-3 — `.gitignore`: Block secrets khỏi git

File `.gitignore` cũ bị **comment out toàn bộ**. Rewrite hoàn toàn:

```gitignore
.env
.env.*
!.env.example
infra_dataops_finance_trading/docker-compose.yml
*.pyc / __pycache__/ / .venv/ ...
*.exe / *.test / *.out  (Go artifacts)
```

`git rm --cached infra_dataops_finance_trading/docker-compose.yml` — untrack file đã tracked từ trước.

**Verify:**
```
.gitignore:4  → .env                                        ✅ ignored
.gitignore:9  → infra_dataops_finance_trading/docker-compose.yml  ✅ ignored
```

---

### FIX-4 — `dataops-redis` UNHEALTHY: `$$REDIS_PASSWORD` không expand

**Root cause:**
```yaml
# SAI — $$REDIS_PASSWORD không phải env var của Redis process
test: ["CMD-SHELL", "redis-cli -a $$REDIS_PASSWORD ping | grep PONG"]

# ĐÚNG — Docker Compose expand ${...} lúc parse file
test: ["CMD-SHELL", "redis-cli -a ${REDIS_PASSWORD:-dataops_fintrading@123} ping | grep PONG"]
```

**Lesson learned:**
- `$$VAR` trong Docker Compose YAML = escape `$` → shell trong container nhận `$VAR` literal.
- Nhưng container Redis **không inject `REDIS_PASSWORD` vào env** của chính nó → expand thành rỗng → auth fail.
- Phải dùng `${VAR:-default}` để **Docker Compose** expand trước khi truyền vào healthcheck shell.

---

### FIX-5 — `dataops_batch_api/tcbs_client.py`: `token_refresh_threshold_sec` quá nhỏ

**Bug:** `from_env()` không truyền `token_refresh_threshold_sec` → default 300s (5 phút).
→ Token inject từ `.env` bị coi là expired sau 5 phút dù TTL thật là **8 giờ**.

**Fix:** Thêm tham số vào `from_env()`:
```python
token_refresh_threshold_sec=int(os.getenv("TCBS_TOKEN_THRESHOLD_SEC", "7200")),
```

**So sánh sau fix:**

| | Go client | Python client |
|---|---|---|
| Token TTL thực | 8 giờ | 8 giờ |
| Refresh threshold | 3600s (1h) | **7200s** (2h) — đã fix |
| Env override | — | `TCBS_TOKEN_THRESHOLD_SEC` |

---

### FIX-6 — `docker-compose.yml`: Kafka topic provisioning tự động

**Vấn đề:**
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` → topics tạo ngẫu nhiên, không có retention → OOM khi test.
- Không có cơ chế tạo topics với đúng partition/replication/retention từ đầu.

**Giải pháp: `kafka-init` service** — init container chạy 1 lần, tạo tất cả topics, rồi exit:

```yaml
kafka-init:
  image: confluentinc/cp-kafka:7.6.0
  restart: on-failure
  depends_on:
    kafka1: { condition: service_healthy }
    kafka2: { condition: service_healthy }
    kafka3: { condition: service_healthy }
  entrypoint: ["/bin/bash", "-c"]
  command:
    - | <shell script tạo tất cả topics>
```

**Topics được tạo tự động:**

| Topic | Partitions | Retention | Ghi chú |
|---|:---:|---|---|
| `binance.trade.ticks` | 6 | 2h | High-freq, compression snappy |
| `binance.orderbook.snapshot` | 6 | 30m | Snapshot stale nhanh |
| `binance.liquidation.events` | 3 | 24h | |
| `binance.funding.rates` | 3 | 48h | |
| `binance.ohlcv.batch` | 3 | 7d | Batch offline |
| `tcbs.market.snapshot` | 3 | 1h | Poll mỗi phút giờ giao dịch |
| `tcbs.bsa.events` | 3 | 24h | EOD |
| `tcbs.intraday.ticks` | 3 | 24h | EOD |
| Alert topics (3x) | 3 | 24h | |
| DLQ topics (7x) | 1 | 7d | Giữ lâu để debug/replay |

**Thay đổi dependency chain:**
```
kafka1/2/3 healthy → kafka-init (exit 0) → stream-producer start
```

`stream-producer` đổi `depends_on` từ `kafka1: service_started` → `kafka-init: service_completed_successfully`.

**Tắt auto-create** trên cả 3 brokers:
```yaml
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
```

---

## 3. TÀI LIỆU MỚI TẠO

| File | Nội dung |
|---|---|
| `docs/TEST_PLAN_BINANCE_E2E.md` | Rewrite: ma trận container bật/tắt, TCBS-optional steps, credentials từ `.env`, lệnh rebuild |
| `docs/TCBS_AUTH_GUIDE.md` | Hướng dẫn đầy đủ TCBS auth flow, cách lấy JWT token, refresh script, decode TTL |

---

## 4. LESSON LEARNED

### [L1] Docker Compose `${}` vs `$$` trong healthcheck

| Pattern | Expand bởi | Kết quả |
|---|---|---|
| `${VAR:-default}` | Docker Compose (parse time) | Password thực được inject vào shell |
| `$$VAR` | Shell trong container (runtime) | `$VAR` — nhưng container không có env var này → rỗng |

**Rule:** Healthcheck command cần password → dùng `${VAR:-default}` (Compose expand), không dùng `$$VAR`.

### [L2] Redis healthcheck với password

```yaml
# Pattern chuẩn
test: ["CMD-SHELL", "redis-cli -a ${REDIS_PASSWORD:-<default>} ping | grep PONG"]
```

Redis process không có env var `REDIS_PASSWORD` trong namespace của container. Password chỉ được pass qua `command:` khi start.

### [L3] Kafka init container pattern

```
restart: on-failure        # retry nếu Kafka chưa ready
--if-not-exists            # idempotent — chạy lại nhiều lần an toàn
service_completed_successfully  # downstream chỉ start sau khi init xong
```

Kafka KRaft cần tất cả 3 brokers `healthy` trước khi tạo topics (quorum voting cần ít nhất 2/3).

### [L4] TCBS Auth — chỉ cần 1 API Key

- TCBS **không có secret key** như Binance HMAC — chỉ cần `TCBS_API_KEY`.
- JWT Token lấy qua: `POST /gaia/v1/oauth2/openapi/token` với `{apiKey, otp}` (Smart OTP từ TCInvest app).
- Token TTL = **8 giờ** — cần refresh thủ công mỗi ~7 giờ trong môi trường container headless.
- Python client bug: `token_refresh_threshold_sec=300` (5 phút) coi token expired quá sớm → fix thành 7200s.

### [L5] `.gitignore` không hoạt động với file đã tracked

Nếu file đã được commit trước khi thêm vào `.gitignore`:
```bash
git rm --cached <file>   # untrack khỏi git index
# Sau đó commit "remove tracked file" 
```

### [L6] `docker stats` — đọc actual RAM trước khi set limit

Không set limit dựa trên ước đoán. Luôn chạy `docker stats --no-stream` để lấy actual usage, sau đó set limit = actual + 20-30% headroom.

---

## 5. TRẠNG THÁI CUỐI SESSION

### Services

| Service | Trạng thái | Ghi chú |
|---|---|---|
| kafka1/2/3 | ✅ healthy | `AUTO_CREATE=false`, topics tạo qua kafka-init |
| kafka-init | ✅ exit 0 | Chạy 1 lần khi stack start, tạo 15 topics |
| schema-registry | ✅ healthy | |
| redis | ✅ healthy | Sau khi fix healthcheck `${REDIS_PASSWORD}` |
| mysql | ✅ healthy | `CMD-SHELL` + `MYSQL_USER/PASSWORD` đúng |
| influxdb | ✅ healthy | `DOCKER_INFLUXDB_INIT_*` vars đúng |
| minio | ✅ healthy | |
| spark-master | ✅ healthy | |
| spark-worker | ✅ running | |
| grafana | ✅ healthy | Sau khi fix healthcheck URL |
| dataops-stream-producer | ⚠️ cần rebuild | Image chưa build lại với BUG-1 fix (splitCSV) |
| dataops-backend-api | ⚠️ cần rebuild | Image chưa build lại |
| dataops-batch-api | ⚠️ cần rebuild | Image chưa build lại với `ENV PYTHONPATH=/app` |

### Files thay đổi tổng hợp

```
infra_dataops_finance_trading/
  docker-compose.yml          EDITED — 11 credential fixes, RAM limits, kafka-init service,
                                        KAFKA_AUTO_CREATE=false, redis healthcheck fix

dataops_batch_api/
  cli/ingestion/sources/tcbs_client.py   EDITED — token_refresh_threshold_sec=7200s

.gitignore                    REWRITE — block .env, docker-compose.yml, artifacts

docs/
  TEST_PLAN_BINANCE_E2E.md    REWRITE — container matrix, TCBS-optional, correct credentials
  TCBS_AUTH_GUIDE.md          CREATED — TCBS auth flow, token guide, refresh script
```

---

## 6. VIỆC CẦN LÀM TIẾP THEO

### [BLOCKING] Rebuild 3 app services (chưa làm trong session này)

```bash
cd ~/DataOps_Backend_Financial_Trading/infra_dataops_finance_trading

# Rebuild images
docker compose --env-file ../.env build --no-cache \
  dataops-stream-producer \
  dataops-batch-api \
  dataops-backend-api

# Force recreate — kafka-init sẽ tự chạy và tạo topics
docker compose --env-file ../.env up -d --force-recreate \
  dataops-stream-producer \
  dataops-batch-api \
  dataops-backend-api

# Verify kafka-init đã tạo topics
docker logs dataops-kafka-init
```

### [PLANNED] Test E2E theo `docs/TEST_PLAN_BINANCE_E2E.md`

1. Verify `binance.trade.ticks` nhận messages trong Kafka UI `:8082`
2. Test Bronze Binance: `docker exec dataops-batch-api python -c "from cli.flows.serving.bronze_flow import run_binance_bronze; print(run_binance_bronze(inst_type='SPOT', inst_ids=['BTCUSDT'], limit=10))"`
3. Verify `GET /api/v1/health` → `{"status":"ok"}`

### [PLANNED] TCBS integration test (sau khi có API Key + JWT Token)

Theo `docs/TCBS_AUTH_GUIDE.md` — refresh token thủ công mỗi ~7h.
