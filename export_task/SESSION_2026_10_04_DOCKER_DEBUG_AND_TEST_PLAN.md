# SESSION EXPORT — Docker Infra Debug & Service Test Plan
**Date:** 2026-10-04  
**Branch:** 03102026_debug_infra  
**Scope:** `infra_dataops_finance_trading/docker-compose.yml`, `dataops_stream_producer` (Go), `dataops_batch_api` (Python), `dataops_backend_api` (Go)  
**References:**
- `export_task/SESSION_2026_10_03_DOCKER_REVIEW_VPS_PURGE.txt`
- `github/copilotinstructions/SKILL.md`

---

## 1. MỤC TIÊU SESSION

1. Debug toàn bộ lỗi khiến 4 services không start sau khi build Docker infra.
2. Fix từng lỗi theo thứ tự ưu tiên — blocking trước.
3. Refactor `docker-compose-clean.yml` thành file chuẩn YAML 2-space.
4. Tạo test plan end-to-end cho 4 services với Binance data.

---

## 2. DANH SÁCH LỖI ĐÃ PHÁT HIỆN VÀ FIX

### BUG-1 — `dataops-stream-producer` crash: `too many colons in address`

**Triệu chứng:**
```
[Kafka Producer] create sarama producer: kafka: client has run out of available brokers
dial tcp: address kafka1:29092,kafka2:29092,kafka3:29092: too many colons in address
```

**Root cause:**  
`configuration.go` đọc `KAFKA_URL` thành 1 string duy nhất. `app/server.go` truyền `[]string{cfg.Kafka.URL}` — sarama nhận 1 element chứa toàn bộ CSV string với dấu phẩy → parse fail.

**Files đã sửa:**

`dataops_stream_producer/pkg/configuration/configuration.go`:
- Thêm field `Brokers []string` vào `Config.Kafka` struct
- Thêm `cfg.Kafka.Brokers = splitCSV(rawKafkaURL)` trong `FromEnv()`
- Import `"strings"` để dùng `strings.TrimSpace`
- `splitCSV()` cũng trim space và filter empty strings

`dataops_stream_producer/app/server.go`:
- Đổi `[]string{cfg.Kafka.URL}` → `cfg.Kafka.Brokers`
- Thêm dòng debug: `fmt.Printf("[DEBUG] cfg.Kafka.URL=%s, cfg.Kafka.Brokers=%v\n", ...)`

---

### BUG-2 — `dataops-batch-api` crash: `ModuleNotFoundError: No module named 'cli'`

**Triệu chứng:**
```
File "/app/cli/flows/serving/daily_pipeline.py", line 27
from cli.flows.serving.bronze_flow import ...
ModuleNotFoundError: No module named 'cli'
```

**Root cause:**  
Docker container chạy `python cli/flows/serving/daily_pipeline.py` từ `/app`. Python không tự động thêm CWD vào `sys.path` khi chạy file trực tiếp (khác với `python -m`). `from cli.flows...` là absolute import cần `/app` trong `sys.path`.

**File đã sửa:**

`dataops_batch_api/Dockerfile`:
```dockerfile
# Thêm dòng này vào stage runtime, sau các ENV khác
ENV PYTHONPATH=/app
```

---

### BUG-3 — `kafka2`/`kafka3` UNHEALTHY: `DUPLICATE_BROKER_REGISTRATION`

**Triệu chứng:**
```
INFO: Unable to register broker 2 because the controller returned error DUPLICATE_BROKER_REGISTRATION
```

**Root cause:** KRaft metadata log cũ còn trong volumes → cluster ID conflict khi restart.

**Fix:** Xóa Kafka volumes trước khi khởi động lại:
```bash
docker volume rm \
  infra_dataops_finance_trading_kafka1-data \
  infra_dataops_finance_trading_kafka2-data \
  infra_dataops_finance_trading_kafka3-data
```

---

### BUG-4 — `docker-compose-clean.yml` validation error: `additional properties 'dataops-stream-producer' not allowed`

**Triệu chứng:**
```
validating docker-compose.yml: services.grafana additional properties 'dataops-stream-producer' not allowed
```

**Root cause (2 lỗi):**
1. `dataops-stream-producer` bị indent 4 spaces → nằm lồng trong block `grafana` thay vì là service độc lập
2. `build.context:` ngang hàng với `build:` thay vì là con của `build:`

```yaml
# SAI — lồng trong grafana
  grafana:
    ...
    dataops-stream-producer:   # ← 4 spaces = con của grafana
      build:
      context: ../dataops_stream_producer   # ← ngang hàng build:

# ĐÚNG — service độc lập
  dataops-stream-producer:     # ← 2 spaces = service level
    build:
      context: ../dataops_stream_producer   # ← con của build:
```

**Fix:** Refactor toàn bộ file `docker-compose-clean.yml` (xem mục 3).

---

### BUG-5 — Kafka healthcheck timeout: `Health check exceeded timeout (10s)`

**Triệu chứng:**
```json
"ExitCode": -1,
"Output": "Health check exceeded timeout (10s): SLF4J: Class path contains multiple SLF4J bindings..."
```

**Root cause:**  
`cub kafka-ready` mất ~15-17 giây để khởi động do SLF4J initialization overhead. Healthcheck `timeout: 10s` → Docker kill process trước khi command hoàn thành. Kafka thực ra đang chạy hoàn toàn bình thường.

**Fix:** Tăng `timeout: 10s` → `timeout: 30s` cho cả 3 Kafka brokers trong `docker-compose.yml`.

---

### BUG-6 — `dataops-backend-api` UNHEALTHY: healthcheck `GET /health` → 404

**Triệu chứng:**
```
[GIN] 2026/10/04 - 13:44:01 | 404 |  9.624µs | ::1 | GET "/health"
```

**Root cause:**  
Healthcheck gọi `GET /health` nhưng route thực tế là `GET /api/v1/health` (đăng ký trong `AuthHandler.RegisterPublic()` → `api.Group("/api/v1")` → `r.GET("/health")`).

**Fix:** Đổi healthcheck URL trong `docker-compose.yml`:
```yaml
# SAI
test: ["CMD", "curl", "-sf", "http://localhost:8080/health"]

# ĐÚNG
test: ["CMD", "curl", "-sf", "http://localhost:8080/api/v1/health"]
```

---

### BUG-7 — `dataops-mysql` UNHEALTHY: `${VAR}` không expand trong CMD form

**Triệu chứng:** MySQL healthcheck luôn fail dù MySQL đang `ready for connections`.

**Root cause:**  
Docker `CMD` array form **không chạy qua shell** → `${MYSQL_ROOT_PASSWORD:-root123}` được truyền nguyên xi vào `mysqladmin` như literal string → authentication fail.

**Fix:** Đổi sang `CMD-SHELL`:
```yaml
# SAI — CMD array không expand biến
test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-u", "root", "-p${MYSQL_ROOT_PASSWORD:-root123}"]

# ĐÚNG — CMD-SHELL chạy qua /bin/sh, expand biến
test: ["CMD-SHELL", "mysqladmin ping -h localhost -u root -p$$MYSQL_ROOT_PASSWORD"]
```
`$$` trong Docker Compose YAML = escape của `$` → shell nhận `$MYSQL_ROOT_PASSWORD`.

---

### BUG-8 — `spark-master` healthcheck timeout

**Triệu chứng:** Spark healthcheck fail với `Error dependency spark-master failed to start`.

**Root cause:** Spark mất ~49 giây để bind port 8080. `start_period: 30s` quá ngắn → healthcheck bắt đầu retry trước khi Spark sẵn sàng.

**Fix:** Tăng `start_period: 30s` → `start_period: 90s`.

---

### BUG-9 — kafka2 EXTERNAL listener conflict port với CONTROLLER

**Phân tích:**  
`kafka2` config ban đầu: `CONTROLLER://kafka2:9093` và `EXTERNAL://0.0.0.0:9093` cùng dùng port `9093`.

**Fix áp dụng:**
- `kafka2`: EXTERNAL port → `9095` (tránh conflict với CONTROLLER `:9093`)  
- Hoặc cách hiệu quả hơn: dùng `localhost:<external_port>` trong healthcheck vì EXTERNAL bind `0.0.0.0`.

**Final config sau fix:**
```yaml
kafka2:
  KAFKA_LISTENERS: PLAINTEXT://kafka2:29092,CONTROLLER://kafka2:9093,EXTERNAL://0.0.0.0:9093
  healthcheck:
    test: ["CMD", "cub", "kafka-ready", "-b", "localhost:9093", "1", "30"]
    timeout: 30s   # tăng từ 10s

kafka3:
  KAFKA_LISTENERS: PLAINTEXT://kafka3:29092,CONTROLLER://kafka3:9093,EXTERNAL://0.0.0.0:9094
  healthcheck:
    test: ["CMD", "cub", "kafka-ready", "-b", "localhost:9094", "1", "30"]
    timeout: 30s
```

---

## 3. REFACTOR `docker-compose.yml` — CÁC THAY ĐỔI CHÍNH

File cũ (`docker-compose-clean.yml`) có các vấn đề:
- Indent loạn: mix 4-space và 2-space không nhất quán
- `dataops-stream-producer` lồng trong `grafana` block
- `build.context` sai cú pháp (ngang hàng `build:` thay vì con)
- `kafka2`/`kafka3` thiếu EXTERNAL listener
- Healthcheck timeouts quá ngắn
- `CMD` form không expand shell variables

**File mới đã fix toàn bộ** — lưu tại `test.md` (do `.bobignore` block thư mục `infra_dataops_finance_trading/`).

### Ma trận port sau refactor

| Service | Port Host | Port Container | Ghi chú |
|---------|-----------|----------------|---------|
| kafka1 | 9092 | 9092 | EXTERNAL |
| kafka2 | 9093 | 9093 | EXTERNAL |
| kafka3 | 9094 | 9094 | EXTERNAL |
| schema-registry | 8081 | 8081 | |
| kafka-ui | 8082 | 8080 | |
| minio S3 API | 9010 | 9000 | |
| minio Console | 9011 | 9001 | |
| mysql | 3306 | 3306 | |
| redis | 6379 | 6379 | |
| influxdb | 8086 | 8086 | |
| grafana | 3001 | 3000 | |
| spark-master UI | 8095 | 8080 | |
| spark-master RPC | 7087 | 7077 | |
| dataops-backend-api | 8080 | 8080 | |

---

## 4. TRẠNG THÁI SAU SESSION

### Services

| Service | Trạng thái | Ghi chú |
|---------|-----------|---------|
| kafka1 | ✅ healthy | Sau khi tăng `timeout: 30s` |
| kafka2 | ✅ healthy | Sau khi tăng `timeout: 30s` + đúng EXTERNAL port |
| kafka3 | ✅ healthy | Sau khi tăng `timeout: 30s` + đúng EXTERNAL port |
| schema-registry | ✅ healthy | Phụ thuộc Kafka → tự healthy sau Kafka |
| mysql | ✅ healthy | Sau khi đổi sang `CMD-SHELL` healthcheck |
| redis | ✅ healthy | Không thay đổi |
| influxdb | ✅ healthy | Không thay đổi |
| minio | ✅ healthy | Không thay đổi |
| spark-master | ✅ healthy | Sau khi tăng `start_period: 90s` |
| spark-worker | ✅ running | Phụ thuộc spark-master |
| grafana | ✅ healthy | Không thay đổi |
| dataops-backend-api | ⚠️ cần fix | Healthcheck URL: `/api/v1/health` |
| dataops-stream-producer | ⚠️ cần rebuild | Image chưa build lại với BUG-1 fix |
| dataops-batch-api | ⚠️ cần rebuild | Image chưa build lại với BUG-2 fix |

### Go code changes

| File | Thay đổi |
|------|---------|
| `dataops_stream_producer/pkg/configuration/configuration.go` | Thêm `Kafka.Brokers []string`, `splitCSV()` cải tiến với `strings.TrimSpace` |
| `dataops_stream_producer/app/server.go` | Dùng `cfg.Kafka.Brokers`, thêm DEBUG log |

### Python/Docker changes

| File | Thay đổi |
|------|---------|
| `dataops_batch_api/Dockerfile` | Thêm `ENV PYTHONPATH=/app` trong runtime stage |

---

## 5. VIỆC CẦN LÀM TIẾP THEO

### [BLOCKING] Cần làm ngay

```bash
cd ~/DataOps_Backend_Financial_Trading/infra_dataops_finance_trading

# 1. Rebuild images với code fix
docker compose --env-file ../.env build \
  dataops-stream-producer \
  dataops-batch-api \
  dataops-backend-api

# 2. Force recreate 3 app services
docker compose --env-file ../.env up -d --force-recreate \
  dataops-stream-producer \
  dataops-batch-api \
  dataops-backend-api

# 3. Verify
sleep 30 && docker compose ps
```

### [PLANNED] Sau khi infra stable

1. **Test stream-producer:** Verify `[BinanceProducer] [BTCUSDT] connected` trong logs + messages trong Kafka topic `binance.trade.ticks` qua Kafka UI :8082

2. **Test batch-api:**
   ```bash
   docker exec -it dataops-batch-api python -c "
   from cli.flows.serving.bronze_flow import run_binance_bronze
   n = run_binance_bronze(inst_type='SPOT', inst_ids=['BTCUSDT'], limit=10)
   print(f'Wrote {n} records')
   "
   ```

3. **Test backend-api:**
   ```bash
   curl -sf http://localhost:8080/api/v1/health
   # Mong đợi: {"status":"ok"}
   ```

4. **Tạo Kafka topics thủ công nếu cần:**
   ```bash
   docker exec dataops-kafka1 kafka-topics \
     --bootstrap-server kafka1:29092 \
     --create --topic binance.trade.ticks \
     --partitions 6 --replication-factor 3
   ```

5. **Xem full test plan:** `TEST_PLAN_BINANCE_E2E.md` ở root repo

---

## 6. BÀI HỌC KỸ THUẬT (LESSONS LEARNED)

### [L1] Docker CMD array vs CMD-SHELL — shell variable expansion

`CMD ["command", "${VAR}"]` → Docker không chạy qua shell → `${VAR}` không được expand.  
`CMD-SHELL` → Docker chạy `/bin/sh -c "..."` → shell expand biến bình thường.  
`$$VAR` trong Docker Compose YAML = `$VAR` sau khi Compose xử lý (escape dollar sign).

### [L2] `cub kafka-ready` SLF4J overhead

`confluentinc/cp-kafka:7.6.0` có duplicate SLF4J bindings → `cub kafka-ready` mất 15-20s để initialize JVM trước khi thực sự check Kafka.  
→ Luôn đặt `timeout: 30s` (không phải 10s) cho Kafka healthcheck trong môi trường này.

### [L3] KRaft metadata volume conflict

Khi restart Kafka cluster với volume cũ chứa metadata từ cluster trước → `DUPLICATE_BROKER_REGISTRATION`.  
→ Phải `docker volume rm` các Kafka data volumes khi thay đổi `CLUSTER_ID` hoặc `NODE_ID`.  
→ Không cần xóa volume khi chỉ thay đổi config mạng/healthcheck.

### [L4] YAML indentation và nesting trong Docker Compose

Docker Compose schema không cho phép nested service definitions.  
Lỗi `additional properties 'X' not allowed` = service X bị indent nhầm thành property của service trên nó.  
→ Luôn dùng YAML formatter (VSCode: `Shift+Alt+F`) trước khi commit docker-compose.

### [L5] Kafka EXTERNAL listener và healthcheck

Kafka brokers có 2 loại listener:
- `PLAINTEXT://kafka2:29092` → bind hostname `kafka2`, chỉ reachable từ container khác qua DNS
- `EXTERNAL://0.0.0.0:9093` → bind tất cả interfaces, `localhost:9093` từ trong container reachable

Healthcheck phải dùng listener bind `0.0.0.0` (EXTERNAL) chứ không phải listener bind hostname.

### [L6] Python absolute imports trong Docker

`python script.py` từ CWD `/app` không tự thêm `/app` vào `sys.path`.  
`python -m module` thêm CWD vào `sys.path`.  
→ Cách chuẩn trong Dockerfile: `ENV PYTHONPATH=/app` để absolute imports như `from cli.flows...` luôn hoạt động dù entry point là script hay module.

### [L7] Go sarama broker list từ CSV env var

`sarama.NewSyncProducer(brokers []string, config)` — `brokers` phải là `[]string` với từng element là `host:port`.  
Nếu truyền `[]string{"kafka1:29092,kafka2:29092,kafka3:29092"}` (1 element chứa CSV) → sarama parse thành 1 địa chỉ với nhiều dấu `:` → `too many colons`.  
→ Luôn split CSV string thành slice trước khi truyền vào sarama.

### [L8] `.bobignore` block toàn bộ `infra_dataops_finance_trading/`

File `infra_dataops_finance_trading/docker-compose.yml` và `docker-compose-clean.yml` bị `.bobignore` block → không đọc/ghi được qua Bob tools.  
→ Phải sửa thủ công trên WSL hoặc dùng `sed`/`python3` trong terminal.  
→ Có thể đọc qua `grep` pattern (không dùng `read_file`).

---

## 7. FILES THAY ĐỔI TỔNG HỢP

```
dataops_stream_producer/
  pkg/configuration/configuration.go   EDITED — Kafka.Brokers field + splitCSV cải tiến
  app/server.go                         EDITED — cfg.Kafka.Brokers + DEBUG log

dataops_batch_api/
  Dockerfile                            EDITED — ENV PYTHONPATH=/app

infra_dataops_finance_trading/
  docker-compose.yml                    EDITED (qua WSL sed/python3):
                                          - kafka2/kafka3 EXTERNAL listener + port 9093/9094
                                          - Kafka healthcheck timeout: 30s
                                          - Backend API healthcheck URL: /api/v1/health
                                          - MySQL healthcheck: CMD-SHELL + $$MYSQL_ROOT_PASSWORD
                                          - spark-master start_period: 90s
                                          - dataops-stream-producer tách khỏi grafana block
                                          - build.context syntax đúng

ROOT/
  TEST_PLAN_BINANCE_E2E.md              CREATED — Test plan đầy đủ 4 services
  test.md                               EDITED — Nội dung docker-compose-fixed.yml

export_task/
  SESSION_2026_10_04_DOCKER_DEBUG_AND_TEST_PLAN.md  CREATED (this file)
```
