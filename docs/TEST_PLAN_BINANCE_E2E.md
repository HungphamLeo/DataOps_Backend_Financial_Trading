# Test Plan — Binance Data End-to-End (4 Services)

> **Cập nhật:** Phản ánh đúng trạng thái sau session 2026-10-04.
> TCBS keys chưa có → Bước TCBS được đánh dấu **[SKIP — no TCBS key]**, toàn bộ Binance flow vẫn test được độc lập.

---

## 🗺️ Luồng dữ liệu tổng quan

```
Binance WebSocket              Binance REST API
(btcusdt@trade                 (/api/v3/klines)
 btcusdt@depth5)                     │
       │                             │
       ▼                             ▼
dataops-stream-producer      dataops-batch-api
(Go / IBM Sarama)            (Python / PySpark / Prefect)
       │                             │
       ▼                             ▼
Kafka Cluster                  MinIO Delta Lake
binance.trade.ticks            binance-datalake/
binance.orderbook.snapshot     (Bronze / Silver / Gold)
       │   │
       │   ▼
       │  Redis Cache
       │  (orderbook TTL 60s)
       │
       └──────────────────────┐
                              ▼
                      dataops-backend-api
                       (Go / Gin REST)
                       MySQL + InfluxDB
                              │
                              ▼
                    Client / Grafana Dashboard

dataops-protobuf (.proto schemas) — contract dùng chung cho cả 3 services
```

---

## 📦 Ma trận Container — Bật/Tắt theo từng test scenario

> Dùng bảng này để xác định **phải bật container nào** cho từng bài test.
> ✅ = bắt buộc bật | ⚙️ = cần nhưng tự start theo depends_on | ➖ = không cần | 🔶 = optional/skip nếu không có key

| Container | Test Infra | Test stream-producer | Test batch-api | Test backend-api | Test protobuf |
|---|:---:|:---:|:---:|:---:|:---:|
| `dataops-kafka1` | ✅ | ✅ | ➖ | ➖ | ➖ |
| `dataops-kafka2` | ✅ | ✅ | ➖ | ➖ | ➖ |
| `dataops-kafka3` | ✅ | ✅ | ➖ | ➖ | ➖ |
| `dataops-schema-registry` | ✅ | ✅ | ➖ | ➖ | ➖ |
| `dataops-kafka-ui` | ✅ | ✅ | ➖ | ➖ | ➖ |
| `dataops-redis` | ✅ | ✅ | ➖ | ✅ | ➖ |
| `dataops-mysql` | ✅ | ➖ | ➖ | ✅ | ➖ |
| `dataops-influxdb` | ✅ | ➖ | ➖ | ✅ | ➖ |
| `dataops-minio` | ✅ | ➖ | ✅ | ➖ | ➖ |
| `dataops-spark-master` | ✅ | ➖ | ✅ | ➖ | ➖ |
| `dataops-spark-worker` | ➖ | ➖ | ✅ | ➖ | ➖ |
| `dataops-grafana` | ➖ | ➖ | ➖ | ✅ | ➖ |
| `dataops-stream-producer` | ➖ | ✅ | ➖ | ⚙️ | ➖ |
| `dataops-batch-api` | ➖ | ➖ | ✅ | ⚙️ | ➖ |
| `dataops-backend-api` | ➖ | ➖ | ➖ | ✅ | ➖ |

### Lệnh bật nhanh theo từng scenario

```bash
cd ~/DataOps_Backend_Financial_Trading/infra_dataops_finance_trading

# [A] Chỉ test stream-producer (Binance → Kafka)
docker compose --env-file ../.env up -d \
  kafka1 kafka2 kafka3 schema-registry kafka-ui redis \
  dataops-stream-producer

# [B] Chỉ test batch-api (Binance REST → MinIO)
docker compose --env-file ../.env up -d \
  minio spark-master spark-worker \
  dataops-batch-api

# [C] Chỉ test backend-api (REST endpoints)
docker compose --env-file ../.env up -d \
  mysql redis influxdb \
  dataops-backend-api

# [D] Full stack (tất cả)
docker compose --env-file ../.env up -d
```

---

## ⚡ Bước 0 — Prerequisites & Rebuild (BLOCKING — làm trước tất cả)

### 0.1 Các fix đã áp dụng (cần rebuild để có hiệu lực)

| Fix | File | Nội dung |
|---|---|---|
| BUG-1 | `dataops_stream_producer/pkg/configuration/configuration.go` | `splitCSV()` tách `KAFKA_URL` CSV → `[]string` brokers |
| BUG-1 | `dataops_stream_producer/app/server.go` | Dùng `cfg.Kafka.Brokers` |
| BUG-2 | `dataops_batch_api/Dockerfile` | `ENV PYTHONPATH=/app` |
| FIX-CREDS | `docker-compose.yml` | Tất cả credentials khớp `.env` (Redis password, MySQL user, InfluxDB init vars) |
| FIX-HC | `docker-compose.yml` | Grafana healthcheck → `localhost:3000/api/health` |

### 0.2 Rebuild & restart chuẩn

```bash
cd ~/DataOps_Backend_Financial_Trading/infra_dataops_finance_trading

# Dừng tất cả
docker compose down

# Xóa Kafka volumes (stale KRaft metadata — BUG-3 từ session trước)
docker volume rm \
  infra_dataops_finance_trading_kafka1-data \
  infra_dataops_finance_trading_kafka2-data \
  infra_dataops_finance_trading_kafka3-data

# Rebuild 3 app images với code fix
docker compose --env-file ../.env build \
  dataops-stream-producer \
  dataops-batch-api \
  dataops-backend-api

# Khởi động Kafka cluster trước — đợi healthy (mất ~60-90s do KRaft init)
docker compose --env-file ../.env up -d kafka1 kafka2 kafka3
echo "Waiting 90s for Kafka KRaft cluster to initialize..."
sleep 90

# Verify 3 broker healthy trước khi tiếp tục
docker compose ps kafka1 kafka2 kafka3
# → tất cả phải là (healthy) trước khi chạy lệnh tiếp theo

# Khởi động toàn bộ
docker compose --env-file ../.env up -d
```

### 0.3 Verify toàn bộ infra healthy

```bash
docker compose ps
```

**Kết quả mong đợi:**
```
dataops-kafka1          healthy
dataops-kafka2          healthy
dataops-kafka3          healthy
dataops-schema-registry healthy (hoặc starting — chờ thêm 1-2 phút)
dataops-kafka-ui        running
dataops-mysql           healthy
dataops-redis           healthy
dataops-influxdb        healthy
dataops-minio           healthy
dataops-spark-master    healthy
dataops-spark-worker    running
dataops-grafana         healthy
dataops-stream-producer running     ← không crash/restart
dataops-batch-api       running     ← không crash/restart
dataops-backend-api     running     ← không crash/restart
```

---

## Bước 1 — Test `dataops-stream-producer` (Binance WebSocket → Kafka)

> **Containers cần bật:** kafka1, kafka2, kafka3, schema-registry, kafka-ui, redis, **dataops-stream-producer**
> **TCBS:** Service này có cả Binance WS và TCBS poller. TCBS poller sẽ log WARNING nếu không có key — **không crash**, Binance vẫn chạy bình thường.

### 1.1 Kiểm tra service startup

```bash
docker logs dataops-stream-producer --tail=40
```

**Log bình thường (Binance-only, không có TCBS key):**
```
[data-stream-producer] Starting
[DEBUG] cfg.Kafka.URL=kafka1:29092,kafka2:29092,kafka3:29092, cfg.Kafka.Brokers=[kafka1:29092 kafka2:29092 kafka3:29092]
[Producer] Initialized with 3 brokers, SchemaRegistry=http://schema-registry:8081
[BinanceProducer] Starting for symbols: [BTCUSDT ETHUSDT SOLUSDT BNBUSDT XRPUSDT]
[BinanceProducer] [BTCUSDT] connected to Binance WebSocket
[BinanceProducer] [ETHUSDT] connected to Binance WebSocket
[BinanceProducer] [SOLUSDT] connected to Binance WebSocket
[TCBSPoller] Starting — indexes=[HOSE VN30 HNX]
[TCBSPoller] WARNING: cả TCBS_JWT_TOKEN và TCBS_API_KEY đều rỗng. Public endpoints sẽ work nhưng private endpoints sẽ fail.
[TCBSPoller] Cron jobs started                           ← cron registered, chờ đúng giờ giao dịch
[data-stream-producer] Running — topics: [binance.trade.ticks ...]
```

> ⚠️ Dòng `WARNING: cả TCBS_JWT_TOKEN và TCBS_API_KEY đều rỗng` là **bình thường** khi chưa có key. Service không crash.

**Nếu vẫn thấy lỗi `too many colons`:** Image chưa rebuild.
```bash
docker compose --env-file ../.env up -d --build dataops-stream-producer
```

### 1.2 Verify Binance data vào Kafka — Kafka UI

Truy cập **http://localhost:8082**:
1. `Topics` → chọn `binance.trade.ticks` → tab **Messages**
2. Nếu thấy messages: ✅ Stream producer hoạt động
3. Key: `BTCUSDT` | Value: JSON hoặc Avro bytes (có prefix `0x00` nếu Schema Registry khả dụng)

### 1.3 Verify data vào Kafka — CLI

```bash
# Binance Trade Ticks
docker exec dataops-kafka1 kafka-console-consumer \
  --bootstrap-server kafka1:29092 \
  --topic binance.trade.ticks \
  --from-beginning \
  --max-messages 5

# Binance OrderBook Snapshot
docker exec dataops-kafka1 kafka-console-consumer \
  --bootstrap-server kafka1:29092 \
  --topic binance.orderbook.snapshot \
  --from-beginning \
  --max-messages 5
```

**Kết quả mong đợi:** JSON events hiển thị liên tục. `Ctrl+C` để dừng.

### 1.4 Verify Redis cache (orderbook TTL 60s)

```bash
# Cần authenticate với password từ .env
REDIS_PASS="dataops_fintrading@123"

# Kiểm tra keys
docker exec dataops-redis redis-cli -a $REDIS_PASS KEYS "binance_*"
# → binance_btcusdt_btcusdt@depth5, binance_btcusdt_btcusdt@trade, ...

# Xem TTL còn lại
docker exec dataops-redis redis-cli -a $REDIS_PASS TTL "binance_btcusdt_btcusdt@trade"
# → số giây còn lại (0–60)

# Xem giá trị
docker exec dataops-redis redis-cli -a $REDIS_PASS GET "binance_btcusdt_btcusdt@trade"
```

### 1.5 Liệt kê Kafka topics đã được tạo

```bash
docker exec dataops-kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --list
# Mong đợi thấy: binance.trade.ticks, binance.orderbook.snapshot, ...
```

### 1.6 [SKIP — no TCBS key] Verify TCBS poller (bỏ qua nếu chưa có key)

```bash
# Chỉ chạy khi có TCBS_API_KEY + TCBS_JWT_TOKEN trong .env
docker logs dataops-stream-producer | grep TCBSPoller
# Nếu có key: [TCBSPoller] pollMarketSnapshot — fetched N tickers
# Nếu không key: WARNING message (bình thường)
```

---

## Bước 2 — Test `dataops-batch-api` (Binance REST → Delta Lake)

> **Containers cần bật:** minio, spark-master, spark-worker, **dataops-batch-api**
> **TCBS:** `daily_pipeline.py` có Steps 3/4/5 gọi TCBS. Nếu không có key → steps đó fail/skip nhưng Steps 1/2 (Binance SPOT + FUTURES) vẫn chạy được.

### 2.1 Kiểm tra service startup

```bash
docker logs dataops-batch-api --tail=30
```

**Log bình thường (sau fix `ENV PYTHONPATH=/app`):**
```
=== daily_pipeline started at 2026-10-04T... ===
[daily_pipeline] Step 1/5 — Bronze Binance SPOT
[bronze_flow:binance] Starting SPOT ingestion — BTCUSDT, ETHUSDT
[bronze_flow:binance] BTCUSDT: wrote 300 records → s3a://binance-datalake/binance_candles_spot/
[daily_pipeline] Step 2/5 — Bronze Binance FUTURES
...
[daily_pipeline] Step 3/5 — Bronze TCBS Market (all configured indexes)
ERROR: TCBS_API_KEY is not set                          ← bình thường nếu không có key
```

**Nếu vẫn thấy `ModuleNotFoundError: No module named 'cli'`:** Rebuild image.
```bash
docker compose --env-file ../.env up -d --build dataops-batch-api
```

### 2.2 Test Bronze Binance thủ công (không cần TCBS key)

```bash
# Test Binance SPOT — ghi 10 candles BTCUSDT vào MinIO
docker exec -it dataops-batch-api python -c "
from cli.flows.serving.bronze_flow import run_binance_bronze
n = run_binance_bronze(inst_type='SPOT', inst_ids=['BTCUSDT'], limit=10)
print(f'[TEST] Wrote {n} records to Delta Bronze')
"
# Kết quả mong đợi: [TEST] Wrote 10 records to Delta Bronze

# Test Binance FUTURES
docker exec -it dataops-batch-api python -c "
from cli.flows.serving.bronze_flow import run_binance_bronze
n = run_binance_bronze(inst_type='FUTURES', inst_ids=['BTCUSDT'], limit=5)
print(f'[TEST] Wrote {n} records to Delta Bronze FUTURES')
"
```

### 2.3 Test full daily pipeline — Binance steps only

```bash
# Chạy toàn bộ pipeline — TCBS steps sẽ fail gracefully nếu không có key
docker exec -it dataops-batch-api python cli/flows/serving/daily_pipeline.py
```

### 2.4 Verify data trong MinIO Console

Truy cập **http://localhost:9011**:
- Login: `prod_minio_admin` / `dataops_fintrading@123` *(từ `.env`)*
- Bucket: `binance-datalake`
- Kiểm tra paths:
  - `binance_candles_spot/` → có Parquet files
  - `binance_candles_futures/` → có Parquet files

```bash
# Hoặc kiểm tra qua MinIO CLI trong container
docker exec dataops-minio sh -c \
  "mc alias set local http://localhost:9000 prod_minio_admin dataops_fintrading@123 2>/dev/null && \
   mc ls local/binance-datalake/"
```

### 2.5 [SKIP — no TCBS key] Test TCBS Bronze

```bash
# Chỉ chạy khi có TCBS_API_KEY + TCBS_JWT_TOKEN trong .env
docker exec -it dataops-batch-api python -c "
from cli.flows.serving.bronze_flow import run_tcbs_market_bronze
n = run_tcbs_market_bronze(indexes=['HOSE'])
print(f'[TEST] Wrote {n} TCBS market records')
"
```

---

## Bước 3 — Test `dataops-backend-api` (Go REST API)

> **Containers cần bật:** mysql, redis, influxdb, **dataops-backend-api**
> **Lưu ý:** Endpoints `/crypto/orderbook` cần `dataops-stream-producer` đang push vào Redis.

### 3.1 Kiểm tra service startup

```bash
docker logs dataops-backend-api --tail=20
```

**Log bình thường:**
```
[API] Starting REST API
[API] MySQL connected
[API] Redis connected
[API] InfluxDB connected
[API] Routes registered
HTTP server running port=8080
```

**Nếu crash `connection refused`:** Kiểm tra MySQL/Redis/InfluxDB đã healthy chưa.
```bash
docker compose ps mysql redis influxdb
```

### 3.2 Health check

```bash
# WSL/Bash
curl -sf http://localhost:8080/api/v1/health
# → {"status":"ok"}

# PowerShell
Invoke-RestMethod http://localhost:8080/api/v1/health
```

### 3.3 Lấy JWT token

```bash
# WSL/Bash — dùng credentials mặc định (xem constants/defaults.go trong source)
curl -s -X POST http://localhost:8080/api/v1/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}' | python3 -m json.tool

# Lưu token vào biến
TOKEN=$(curl -s -X POST http://localhost:8080/api/v1/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
echo "TOKEN=$TOKEN"
```

```powershell
# PowerShell
$body = @{ username = "admin"; password = "admin123" } | ConvertTo-Json
$resp = Invoke-RestMethod -Method POST `
  -Uri http://localhost:8080/api/v1/login `
  -Body $body -ContentType "application/json"
$token = $resp.access_token
Write-Host "TOKEN=$token"
```

### 3.4 Test Crypto endpoints (Binance data)

```bash
# OrderBook realtime từ Redis (cần stream-producer đang chạy & push data)
curl -sf -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8080/api/v1/crypto/orderbook?symbol=BTCUSDT"

# OHLCV historical từ InfluxDB (cần batch-api đã ghi data)
curl -sf -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8080/api/v1/crypto/ohlcv?symbol=BTCUSDT&interval=1h"

# VWAP từ InfluxDB
curl -sf -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8080/api/v1/crypto/vwap?symbol=BTCUSDT&interval=5m"
```

**Kết quả mong đợi:**
| Response | Ý nghĩa |
|---|---|
| JSON data | ✅ Service hoạt động và có data |
| `{"error":"no data"}` hoặc `[]` | Service chạy nhưng chưa có data (batch-api chưa ghi / stream-producer chưa push) |
| `401 Unauthorized` | JWT expired hoặc sai token |
| `400 unsupported symbol` | Symbol không có trong `constants/symbols.go` |
| `500` | Database connection issue |

### 3.5 [SKIP — no TCBS key] Test VNStock endpoints

```bash
# Chỉ có data nếu TCBS poller đang chạy với key hợp lệ
curl -sf -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8080/api/v1/vnstock/quote?symbol=VCB"
```

---

## Bước 4 — Test `dataops-protobuf` (Schema Contracts)

> **Containers cần bật:** Không cần container nào — test local thuần túy.

### 4.1 Validate proto syntax

```bash
# Cài protoc nếu chưa có
# Ubuntu/WSL: sudo apt install protobuf-compiler

cd ~/DataOps_Backend_Financial_Trading

# Validate từng proto file
for f in dataops_protobuf/**/*.proto; do
  echo "Checking $f..."
  protoc --proto_path=dataops_protobuf "$f" --descriptor_set_out=/dev/null \
    && echo "  ✅ OK" || echo "  ❌ FAIL"
done
```

### 4.2 Kiểm tra field consistency

So sánh fields giữa 3 nguồn — phải nhất quán:
- [`dataops_protobuf/market-data/market_data.proto`](../dataops_protobuf/market-data/market_data.proto)
- [`dataops_stream_producer/pkg/producer/types.go`](../dataops_stream_producer/pkg/producer/types.go) — `TradeEvent`, `OrderBookEvent`
- [`dataops_stream_producer/pkg/schemas/`](../dataops_stream_producer/pkg/schemas/) — `binance_trade_event.avsc`

Fields cần khớp: `inst_id`, `trade_id`, `price`, `size`, `side`, `ts_ms`, `produced_ms`, `inst_type`

---

## Bước 5 — Verify Grafana Dashboard

> **Containers cần bật:** influxdb, grafana

Truy cập **http://localhost:3001**:
- Login: `admin` / `dataops_fintrading@123` *(từ `.env`)*
- Kiểm tra datasource InfluxDB đã kết nối
- Mở dashboard `okx-vps-main` → verify panels load (có thể empty nếu chưa có data từ batch-api)

---

## 📋 Checklist tóm tắt — Thứ tự thực hiện

| # | Việc cần làm | Blocking? | Status |
|---|---|---|---|
| **0a** | `docker compose down` + xóa Kafka volumes | ✅ Yes | |
| **0b** | `docker compose build` 3 app services | ✅ Yes | |
| **0c** | Start Kafka cluster, đợi `healthy` | ✅ Yes | |
| **0d** | `docker compose up -d` toàn bộ | ✅ Yes | |
| **1a** | Verify `dataops-stream-producer` logs: không có `too many colons` | ✅ Yes | |
| **1b** | Kafka UI → `binance.trade.ticks` có messages | ✅ Yes | |
| **1c** | Redis `KEYS binance_*` có data | ✅ Yes | |
| **2a** | `dataops-batch-api` logs: không có `ModuleNotFoundError` | ✅ Yes | |
| **2b** | `docker exec ... run_binance_bronze(limit=10)` → `Wrote 10 records` | ✅ Yes | |
| **2c** | MinIO Console → `binance-datalake/binance_candles_spot/` có files | ✅ Yes | |
| **3a** | `curl /api/v1/health` → `{"status":"ok"}` | ✅ Yes | |
| **3b** | Login → lấy JWT token | ✅ Yes | |
| **3c** | `GET /crypto/orderbook?symbol=BTCUSDT` → có data | ⚠️ Cần stream-producer | |
| **4a** | `protoc` validate tất cả `.proto` files | ➖ No | |
| **5a** | Grafana → datasource InfluxDB kết nối | ➖ No | |
| **T1** | [SKIP] TCBS poller verify | 🔶 Cần TCBS key | |
| **T2** | [SKIP] `run_tcbs_market_bronze()` | 🔶 Cần TCBS key | |
| **T3** | [SKIP] `GET /vnstock/quote?symbol=VCB` | 🔶 Cần TCBS key | |

---

## 🌐 Web UI Access (credentials từ `.env`)

| Service | URL | Username | Password |
|---|---|---|---|
| Kafka UI | http://localhost:8082 | — | — |
| MinIO Console | http://localhost:9011 | `prod_minio_admin` | `dataops_fintrading@123` |
| Spark Master UI | http://localhost:8095 | — | — |
| Grafana | http://localhost:3001 | `admin` | `dataops_fintrading@123` |
| InfluxDB | http://localhost:8086 | `prod_admin` | `dataops_fintrading@123` |
| Schema Registry | http://localhost:8081/subjects | — | — |
| Backend API Health | http://localhost:8080/api/v1/health | — | — |

---

## 🔧 Lệnh debug hữu ích

```bash
# Xem log realtime của bất kỳ service
docker compose logs -f --tail=50 <service_name>

# Rebuild 1 service sau khi sửa code
docker compose --env-file ../.env up -d --build <service_name>

# Exec vào container để debug
docker exec -it dataops-stream-producer sh     # Go Alpine container
docker exec -it dataops-batch-api bash          # Python container
docker exec -it dataops-backend-api sh          # Go Alpine container

# Kiểm tra healthcheck log chi tiết (xem vì sao unhealthy)
docker inspect <container_name> --format '{{json .State.Health.Log}}' | python3 -m json.tool

# Xem resource usage realtime
docker stats --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"

# Tạo Kafka topics thủ công nếu auto-create không hoạt động
docker exec dataops-kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --create --topic binance.trade.ticks \
  --partitions 6 --replication-factor 3

# Liệt kê tất cả topics
docker exec dataops-kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 --list

# Reset toàn bộ stack + volumes (WARNING: mất data)
docker compose down -v
```

---

## ⚠️ Lưu ý về TCBS Auth

### Cơ chế auth (từ diagram official TCBS iFlash)

```
① Đăng ký → nhận API Key (1 lần duy nhất, không đổi — không có secret key)
③ POST /gaia/v1/oauth2/openapi/token
   Body: { "apiKey": "<key>", "otp": "<Smart OTP từ TCInvest app>" }
④ Nhận Access Token (JWT, TTL = 8 giờ)
⑤ Gọi tất cả API với: Authorization: Bearer <token>
   Token hết hạn 8h → cần Smart OTP mới để lấy token mới
```

### Set vào `.env` — chỉ cần 2 biến

```bash
# Chỉ cần API_KEY — không có secret key, không có password khác
TCBS_API_KEY=<api_key_từ_TCInvest>

# Lấy JWT token thủ công bằng Smart OTP từ TCInvest app:
TCBS_JWT_TOKEN=<jwt_token_lấy_được>   # TTL 8 giờ kể từ lúc lấy
```

### Cách lấy JWT token thủ công

```bash
# Bước 1: Mở TCInvest app → lấy Smart OTP (6 chữ số, valid ~30s, dùng 1 lần)
# Bước 2: Gọi auth endpoint
curl -s -X POST https://openapi.tcbs.com.vn/gaia/v1/oauth2/openapi/token \
  -H "Content-Type: application/json" \
  -d '{"apiKey":"<TCBS_API_KEY>","otp":"<OTP_6_DIGITS>"}' | python3 -m json.tool
# → { "token": "eyJ..." }  ← copy vào .env TCBS_JWT_TOKEN
```

### Restart services sau khi cập nhật token

```bash
docker compose --env-file ../.env up -d --force-recreate \
  dataops-stream-producer \
  dataops-batch-api
```

### Token TTL & Threshold

| | Go client | Python client |
|---|---|---|
| Token TTL thực (TCBS) | **8 giờ** | **8 giờ** |
| Refresh threshold (cảnh báo) | 3600s (1h trước khi hết) | **7200s** (2h trước khi hết) |
| Env override threshold | — | `TCBS_TOKEN_THRESHOLD_SEC` |

> **Quan trọng:** Cả 2 client **không tự lấy token mới** — container headless không thể generate Smart OTP.
> Cần refresh thủ công mỗi ~6-7 giờ: lấy OTP mới → gọi curl → cập nhật `.env` → restart 2 services.
