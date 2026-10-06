# SESSION EXPORT — Stream Producer Crash Loop & Kafka Healthcheck Fix
**Date:** 2026-10-06
**Branch:** 03102026_debug_infra
**Scope:** `infra_dataops_finance_trading/docker-compose.yml`, `dataops_stream_producer/Dockerfile`
**References:**
- `export_task/SESSION_2026_10_05_KAFKA_CLUSTER_FIX_AND_DEBUG.md`
- `github/agent/realtime-streaming-engineer.agent.md`
- `github/agent/devops-infra-engineer.agent.md`

---

## 1. MỤC TIÊU SESSION

1. Debug `dataops-stream-producer` restart loop liên tục.
2. Fix kafka broker healthcheck không pass dù broker đã STARTED.
3. Fix `kafka-init` bị stuck không tạo được topics.
4. Build thành công image `dataops-stream-producer`.

---

## 2. ROOT CAUSE ANALYSIS

### RCA-1 — `dataops-stream-producer`: Image stale, không phải code bug

**Triệu chứng:**
```
panic: [Kafka Producer] create sarama producer:
kafka: client has run out of available brokers to talk to:
dial tcp: address kafka1:29092,kafka2:29092,kafka3:29092: too many colons in address
```

**Chẩn đoán ban đầu (sai):** Gordon (Docker Desktop AI) chẩn đoán do Sarama không parse CSV broker string. → **SAI** — `splitCSV()` đã được implement đúng trong `pkg/configuration/configuration.go:59`.

**Root cause thực tế:** Docker đang chạy **image cũ** (build trước khi `splitCSV` được thêm vào). Sarama nhận nguyên chuỗi `"kafka1:29092,kafka2:29092,kafka3:29092"` như 1 địa chỉ → "too many colons".

**Fix:** Rebuild image với `--no-cache`.

---

### RCA-2 — Kafka brokers `unhealthy`: Healthcheck tool không tồn tại trong image

**Lịch sử tiến hóa healthcheck trong session này:**

| Iteration | Tool | Kết quả | Lý do fail |
|---|---|---|---|
| v1 | `cub kafka-ready -b localhost:9092 1 30` | unhealthy | Port sai (EXTERNAL, không phải PLAINTEXT) + JVM 25s timeout |
| v2 | `cub kafka-ready -b localhost:29092 1 30` | unhealthy | JVM cold-start > timeout, race condition |
| v3 | `kafka-broker-api-versions.sh --bootstrap-server localhost:29092` | **NOT FOUND** | Script không tồn tại trong `confluentinc/cp-kafka:7.6.0` |
| v4 | `kafka-broker-api-versions --bootstrap-server kafka1:29092` | ✅ healthy | Binary tồn tại (không có `.sh` extension) |

**Fix cuối cùng (hoạt động):**
```yaml
healthcheck:
  test: ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server kafkaX:29092 > /dev/null 2>&1"]
  interval: 20s
  timeout: 15s
  retries: 10
  start_period: 60s
```

**Lesson learned — Port healthcheck:**
- `localhost:9092` / `localhost:9095` / `localhost:9094` = EXTERNAL listener → **SAI** cho healthcheck trong container
- `localhost:29092` / `kafkaX:29092` = PLAINTEXT internal → **ĐÚNG**
- Cả 3 brokers đều dùng `29092` cho PLAINTEXT → healthcheck command **đồng nhất** cả 3

---

### RCA-3 — `kafka-init` stuck: Cùng tool không tồn tại

**Triệu chứng:** `kafka-init` bị stuck ở `=== [kafka-init] Creating Binance topics ===` — WARNING xuất hiện nhưng không tạo được topic, không exit.

**Root cause:**
1. `cub kafka-ready -b $$B 3 120` → `cub` cần JVM cold-start 25s × mỗi retry → timeout
2. Sau khi đổi sang `kafka-broker-api-versions.sh` → **script không tồn tại** trong image → loop không bao giờ exit

**Fix cuối cùng:**
```bash
until kafka-broker-api-versions --bootstrap-server kafka1:29092 > /dev/null 2>&1; \
  do echo "[kafka-init] waiting for brokers..."; sleep 3; done
```

---

### RCA-4 — `go mod download` fail khi build

**Triệu chứng:**
```
go: github.com/klauspost/compress@v1.20.1: read "https://proxy.golang.org/...": 
stream error: stream ID 3; INTERNAL_ERROR; received from peer
```

**Root cause:** HTTP/2 stream error từ `proxy.golang.org` — mạng WSL2 không ổn định với HTTP/2.

**Fix:** Thêm `ARG GOPROXY` vào `dataops_stream_producer/Dockerfile` với fallback chain:
```dockerfile
ARG GOPROXY=https://proxy.golang.org,https://goproxy.io,direct
ENV GOPROXY=${GOPROXY}
```

---

## 3. FIXES ĐÃ THỰC HIỆN

### FIX-1 — `docker-compose.yml`: Kafka broker healthcheck

```yaml
# TRƯỚC (sai — cub + port EXTERNAL)
test: ["CMD", "cub", "kafka-ready", "-b", "localhost:9092", "1", "30"]
timeout: 30s

# SAU (đúng — binary native, port PLAINTEXT)
# kafka1:
test: ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server kafka1:29092 > /dev/null 2>&1"]
# kafka2:
test: ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server kafka2:29092 > /dev/null 2>&1"]
# kafka3:
test: ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server kafka3:29092 > /dev/null 2>&1"]
interval: 20s
timeout: 15s
retries: 10
start_period: 60s
```

### FIX-2 — `docker-compose.yml`: kafka-init wait command

```bash
# TRƯỚC (sai — cub JVM timeout / script not found)
cub kafka-ready -b $$B 3 120

# SAU (đúng — loop native binary)
until kafka-broker-api-versions --bootstrap-server kafka1:29092 > /dev/null 2>&1; \
  do echo "[kafka-init] waiting for brokers..."; sleep 3; done
```

### FIX-3 — `dataops_stream_producer/Dockerfile`: GOPROXY fallback

```dockerfile
# Thêm vào đầu Stage 1, trước RUN go mod download
ARG GOPROXY=https://proxy.golang.org,https://goproxy.io,direct
ENV GOPROXY=${GOPROXY}
```

---

## 4. LESSON LEARNED

### [L1] `confluentinc/cp-kafka:7.6.0` — tool availability

| Tool | Tồn tại | Ghi chú |
|---|---|---|
| `cub` | ✅ | Confluent Utility Belt — cần JVM cold-start 25s |
| `kafka-broker-api-versions` | ✅ | Binary native, instant |
| `kafka-broker-api-versions.sh` | ❌ | Không tồn tại trong 7.6.0 |
| `kafka-topics` | ✅ | Dùng trong kafka-init |
| `nc` (netcat) | ✅ | TCP check đơn giản nhất |

**Rule:** Với `confluentinc/cp-kafka:7.6.0`, dùng `kafka-broker-api-versions` (không có `.sh`) hoặc `nc -z host port` cho healthcheck.

### [L2] Kafka healthcheck — port rule

```
EXTERNAL listener  → port 9092/9094/9095 → dùng cho client từ HOST machine
PLAINTEXT listener → port 29092          → dùng cho inter-broker + healthcheck trong container
CONTROLLER         → port 9093           → chỉ dùng cho KRaft quorum voting
```

Healthcheck chạy **bên trong container** → luôn dùng PLAINTEXT port `29092`.

### [L3] `cub kafka-ready` — khi nào dùng được

`cub` chỉ phù hợp khi:
- Container có RAM ≥ 1GB (JVM cold-start cần ~300MB)
- `timeout` ≥ 60s (25s JVM + 10s check + 25s buffer)
- Không phải môi trường WSL2 có RAM limit thấp

Trên WSL2 với limit 256-512MB/broker: **không dùng `cub` cho healthcheck**.

### [L4] Docker image stale — khi nào cần rebuild

`docker compose up -d` **không tự rebuild** khi source code thay đổi. Phải dùng:
```bash
docker compose build --no-cache <service>
docker compose up -d --force-recreate <service>
```

Dấu hiệu đang chạy image stale: error message mô tả behavior của code **cũ** dù source đã fix.

### [L5] `go mod download` fail trên WSL2

HTTP/2 stream error từ `proxy.golang.org` thường xảy ra trên WSL2 khi network không ổn định.
Fix: thêm fallback GOPROXY chain trong Dockerfile.

```dockerfile
ARG GOPROXY=https://proxy.golang.org,https://goproxy.io,direct
ENV GOPROXY=${GOPROXY}
```

Override lúc build nếu cần:
```bash
docker compose build --build-arg GOPROXY=direct <service>
```

---

## 5. TRẠNG THÁI CUỐI SESSION

| Service | Trạng thái | Ghi chú |
|---|---|---|
| dataops-kafka1 | ✅ healthy | Healthcheck: `kafka-broker-api-versions` |
| dataops-kafka2 | ✅ healthy | Healthcheck: `kafka-broker-api-versions` |
| dataops-kafka3 | ✅ healthy | Healthcheck: `kafka-broker-api-versions` |
| dataops-kafka-init | ✅ Started | Topics được tạo thành công |
| dataops-stream-producer | ⚠️ cần verify | Rebuild với GOPROXY fix — chưa confirm log cuối |

---

## 6. VIỆC CẦN LÀM TIẾP THEO

### [BLOCKING] Verify stream-producer sau rebuild

```bash
cd ~/DataOps_Backend_Financial_Trading/infra_dataops_finance_trading

docker compose --env-file ../.env build --no-cache dataops-stream-producer
docker compose --env-file ../.env up -d --force-recreate dataops-stream-producer
docker logs dataops-stream-producer -f
```

Kết quả mong đợi:
```
[DEBUG] cfg.Kafka.Brokers=[kafka1:29092 kafka2:29092 kafka3:29092]
[Producer] Initialized with 3 brokers, SchemaRegistry=http://schema-registry:8081
[data-stream-producer] Running — topics: [binance.trade.ticks ...]
```

### [PLANNED] E2E test theo `docs/TEST_PLAN_BINANCE_E2E.md`

```bash
# Verify topic nhận messages
docker exec dataops-kafka1 kafka-console-consumer \
  --bootstrap-server kafka1:29092 \
  --topic binance.trade.ticks \
  --from-beginning --max-messages 5 --timeout-ms 10000
```

### [PLANNED] Rebuild backend-api và batch-api

```bash
docker compose --env-file ../.env build --no-cache dataops-backend-api dataops-batch-api
docker compose --env-file ../.env up -d dataops-backend-api dataops-batch-api
```
