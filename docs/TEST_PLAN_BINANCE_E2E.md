# Test Plan — Binance Data End-to-End (4 Services)

## Luồng dữ liệu tổng quan

```
Binance WebSocket          Binance REST
(btcusdt@trade             (/api/v3/klines)
 btcusdt@depth5)                │
      │                         │
      ▼                         ▼
dataops-stream-producer    dataops-batch-api
(Go / IBM Sarama)          (Python / PySpark)
      │                         │
      ▼                         ▼
Kafka Cluster              MinIO Delta Lake
binance.trade.ticks        binance-datalake
binance.orderbook.snapshot (Bronze / Silver)
      │     │                   │
      │     ▼                   │
      │  Redis Cache             │
      │  (orderbook snapshot)    │
      │                         │
      └──────────┬──────────────┘
                 ▼
          InfluxDB (time-series)
                 │
                 ▼
        dataops-backend-api
          (Go / Gin REST)
                 │
                 ▼
          Client / Dashboard

dataops-protobuf (.proto schemas) — contract cho cả 3 services
```

---

## Bước 0 — Fix infra (blocking prerequisite)

### 0.1 Các fix đã áp dụng trong code

| Fix | File | Nội dung |
|-----|------|----------|
| BUG-1 | `dataops_stream_producer/pkg/configuration/configuration.go` | `splitCSV()` tách `KAFKA_URL` CSV → `[]string` brokers |
| BUG-1 | `dataops_stream_producer/app/server.go` | Dùng `cfg.Kafka.Brokers` thay vì `[]string{cfg.Kafka.URL}` |
| BUG-2 | `dataops_batch_api/Dockerfile` | `ENV PYTHONPATH=/app` để `cli.*` absolute imports hoạt động |
| FIX-HC | `docker-compose.yml` | Kafka healthcheck `timeout: 30s` (SLF4J init mất ~15s) |
| FIX-HC | `docker-compose.yml` | Backend API healthcheck URL: `/api/v1/health` (không phải `/health`) |
| FIX-HC | `docker-compose.yml` | MySQL healthcheck dùng `CMD-SHELL` để expand `$MYSQL_ROOT_PASSWORD` |

### 0.2 Sequence restart chuẩn

```bash
cd ~/DataOps_Backend_Financial_Trading/infra_dataops_finance_trading

# 1. Dừng tất cả
docker compose down

# 2. Xóa Kafka volumes (stale KRaft metadata)
docker volume rm \
  infra_dataops_finance_trading_kafka1-data \
  infra_dataops_finance_trading_kafka2-data \
  infra_dataops_finance_trading_kafka3-data

# 3. Rebuild images (áp dụng BUG-1 + BUG-2 đã fix)
docker compose --env-file ../.env build \
  dataops-stream-producer \
  dataops-batch-api \
  dataops-backend-api

# 4. Khởi động Kafka cluster trước — đợi healthy
docker compose --env-file ../.env up -d kafka1 kafka2 kafka3
sleep 90

# 5. Verify cả 3 healthy (QUAN TRỌNG — không tiếp tục nếu còn unhealthy)
docker compose ps kafka1 kafka2 kafka3

# 6. Khởi động toàn bộ còn lại
docker compose --env-file ../.env up -d
```

### 0.3 Verify infra healthy

```bash
# Tất cả services phải healthy/running
docker compose ps

# Kết quả mong đợi:
# dataops-kafka1         healthy
# dataops-kafka2         healthy
# dataops-kafka3         healthy
# dataops-schema-registry healthy
# dataops-mysql          healthy
# dataops-redis          healthy
# dataops-influxdb       healthy
# dataops-minio          healthy
# dataops-spark-master   healthy
# dataops-backend-api    healthy
# dataops-stream-producer running
# dataops-batch-api      running
```

---

## Bước 1 — Test `dataops-stream-producer` (Go WebSocket → Kafka)

**Mục tiêu:** Verify Binance WebSocket data chảy vào Kafka topic `binance.trade.ticks`

### 1.1 Kiểm tra service start

```bash
docker compose logs dataops-stream-producer --tail=30
```

**Log bình thường khi healthy:**
```
[data-stream-producer] Starting
[DEBUG] cfg.Kafka.URL=kafka1:29092,kafka2:29092,kafka3:29092, cfg.Kafka.Brokers=[kafka1:29092 kafka2:29092 kafka3:29092]
[Producer] Initialized with 3 brokers, SchemaRegistry=http://schema-registry:8081
[BinanceProducer] Starting for symbols: [BTCUSDT ETHUSDT SOLUSDT]
[BinanceProducer] [BTCUSDT] connected to Binance WebSocket
[BinanceProducer] [ETHUSDT] connected to Binance WebSocket
[BinanceProducer] [SOLUSDT] connected to Binance WebSocket
[data-stream-producer] Running — topics: [binance.trade.ticks ...]
```

**Nếu vẫn thấy `too many colons`:** Image chưa rebuild — chạy lại `docker compose build dataops-stream-producer`.

### 1.2 Verify data vào Kafka — Kafka UI

Truy cập **http://localhost:8082**:
- `Topics` → `binance.trade.ticks` → tab **Messages**
- Nếu có messages: ✅ Stream producer hoạt động
- Key: `BTCUSDT` | Value: JSON hoặc Avro bytes

### 1.3 Verify data vào Kafka — CLI

```bash
docker exec dataops-kafka1 kafka-console-consumer \
  --bootstrap-server kafka1:29092 \
  --topic binance.trade.ticks \
  --from-beginning \
  --max-messages 10
```

**Kết quả mong đợi:** JSON trade events hiển thị trong terminal.

### 1.4 Verify Redis cache (orderbook snapshot)

```bash
# Kiểm tra keys đang có
docker exec dataops-redis redis-cli KEYS "binance_*"
# Kết quả mong đợi: binance_btcusdt_btcusdt@depth5, binance_btcusdt_btcusdt@trade, ...

# Xem giá trị cụ thể
docker exec dataops-redis redis-cli GET "binance_btcusdt_btcusdt@trade"
# Kết quả mong đợi: {"ts": <unix_ms>}
```

### 1.5 Verify orderbook topic

```bash
docker exec dataops-kafka1 kafka-console-consumer \
  --bootstrap-server kafka1:29092 \
  --topic binance.orderbook.snapshot \
  --from-beginning \
  --max-messages 5
```

---

## Bước 2 — Test `dataops-batch-api` (Python REST → Delta Lake)

**Mục tiêu:** Verify Binance REST `/api/v3/klines` được ingest vào MinIO Delta Lake Bronze

### 2.1 Kiểm tra service start

```bash
docker compose logs dataops-batch-api --tail=30
```

**Log bình thường (sau fix PYTHONPATH):**
```
=== daily_pipeline started at 2026-10-04T... ===
[daily_pipeline] Step 1/5 — Bronze Binance SPOT
[bronze_flow:binance] Starting SPOT ingestion — 2 instruments, bar=1m
[bronze_flow:binance] BTCUSDT: wrote 300 records
[bronze_flow:binance] ETHUSDT: wrote 300 records
[bronze_flow:binance] Done — total 600 records written
```

**Nếu thấy `ModuleNotFoundError: No module named 'cli'`:** Image chưa rebuild với `ENV PYTHONPATH=/app`.

### 2.2 Test Bronze Binance thủ công

```bash
# Chạy trực tiếp trong container — không chờ cron
docker exec -it dataops-batch-api python -c "
from cli.flows.serving.bronze_flow import run_binance_bronze
n = run_binance_bronze(inst_type='SPOT', inst_ids=['BTCUSDT'], limit=10)
print(f'Wrote {n} records')
"
```

**Kết quả mong đợi:** `Wrote 10 records`

### 2.3 Test toàn bộ Bronze flow

```bash
docker exec dataops-batch-api python cli/flows/serving/bronze_flow.py
```

### 2.4 Verify data trong MinIO

Truy cập **http://localhost:9011** (MinIO Console):
- Login: `minioadmin` / `minioadmin123` (hoặc giá trị từ `.env`)
- Bucket: `binance-datalake`
- Path: `binance_candles_spot/` → kiểm tra có Parquet files không

```bash
# Hoặc kiểm tra qua CLI
docker exec dataops-minio mc ls local/binance-datalake/binance_candles_spot/
```

---

## Bước 3 — Test `dataops-backend-api` (Go REST API)

**Mục tiêu:** Verify API endpoints trả data từ InfluxDB + Redis

### 3.1 Kiểm tra service start

```bash
docker compose logs dataops-backend-api --tail=20
```

**Log bình thường:**
```
[API] Starting REST API
[API] Routes registered
HTTP server running port=8080
```

### 3.2 Health check

```bash
# Bash (WSL)
curl -sf http://localhost:8080/api/v1/health
# Kết quả mong đợi: {"status":"ok"}

# PowerShell
Invoke-RestMethod http://localhost:8080/api/v1/health
```

### 3.3 Lấy JWT token

```bash
# Bash
curl -s -X POST http://localhost:8080/api/v1/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}' | python3 -m json.tool
```

```powershell
# PowerShell
$body = @{ username = "admin"; password = "admin123" } | ConvertTo-Json
$resp = Invoke-RestMethod -Method POST `
  -Uri http://localhost:8080/api/v1/login `
  -Body $body -ContentType "application/json"
$token = $resp.access_token
```

### 3.4 Test Crypto endpoints (Binance data)

```bash
TOKEN="<jwt_token_từ_bước_3.3>"

# OrderBook realtime từ Redis (cần stream-producer đang chạy)
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8080/api/v1/crypto/orderbook?symbol=BTCUSDT"

# OHLCV historical từ InfluxDB
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8080/api/v1/crypto/ohlcv?symbol=BTCUSDT&interval=1h"

# VWAP
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8080/api/v1/crypto/vwap?symbol=BTCUSDT&interval=5m"

# Liquidations
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8080/api/v1/crypto/liquidations?symbol=BTCUSDT"
```

**Kết quả mong đợi:**
- `/orderbook` → data nếu stream-producer đang push vào Redis
- `/ohlcv`, `/vwap` → data nếu batch-api đã ghi vào InfluxDB
- 401 → JWT expired hoặc sai token
- 400 `unsupported symbol` → symbol không có trong `constants/symbols.go`

---

## Bước 4 — Test `dataops-protobuf` (Schema Contracts)

**Lưu ý:** Protobuf không có runtime service — chỉ là schema definition. Test ở đây là validate syntax và consistency.

### 4.1 Validate proto file syntax

```bash
# Cài protoc nếu chưa có
# Ubuntu/WSL: sudo apt install protobuf-compiler

cd ~/DataOps_Backend_Financial_Trading

protoc --proto_path=dataops_protobuf \
  dataops_protobuf/market-data/market_data.proto \
  --descriptor_set_out=/dev/null

protoc --proto_path=dataops_protobuf \
  dataops_protobuf/ingestion/ingestion.proto \
  --descriptor_set_out=/dev/null

protoc --proto_path=dataops_protobuf \
  dataops_protobuf/analytics/analytics.proto \
  --descriptor_set_out=/dev/null
```

### 4.2 Kiểm tra consistency với Avro schemas

So sánh fields giữa:
- `dataops_protobuf/market-data/market_data.proto`
- `dataops_stream_producer/pkg/producer/types.go` (TradeEvent, OrderBookEvent)
- `dataops_stream_producer/pkg/schemas/binance_trade_event.avsc`

Các fields phải nhất quán: `inst_id`, `trade_id`, `price`, `size`, `side`, `ts_ms`, `produced_ms`, `inst_type`

---

## Tóm tắt thứ tự ưu tiên test

| Thứ tự | Service | Dependency | Test nhanh nhất | Kết quả mong đợi |
|--------|---------|------------|-----------------|------------------|
| **0** | Infra (Kafka, MySQL, Redis...) | — | `docker compose ps` | Tất cả `healthy` |
| **1** | `dataops-stream-producer` | Kafka healthy | `docker logs` + Kafka UI :8082 | Messages trong `binance.trade.ticks` |
| **2** | `dataops-batch-api` | MinIO healthy | `docker exec python bronze_flow.py` | `Wrote N records` > 0 |
| **3** | `dataops-backend-api` | Redis + InfluxDB | `curl /api/v1/health` | `{"status":"ok"}` |
| **4** | `dataops-protobuf` | Không có runtime | `protoc --validate` | Exit 0, no errors |

---

## Web UI Access

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8082 | — |
| MinIO Console | http://localhost:9011 | minioadmin / minioadmin123 |
| Spark Master UI | http://localhost:8095 | — |
| Grafana | http://localhost:3001 | admin / admin123 |
| InfluxDB | http://localhost:8086 | admin / admin1234 |
| Schema Registry | http://localhost:8081/subjects | — |
| Backend API | http://localhost:8080/api/v1/health | — |

---

## Lệnh debug hữu ích

```bash
# Xem log realtime của bất kỳ service
docker compose logs -f --tail=50 <service_name>

# Rebuild 1 service sau khi sửa code
docker compose --env-file ../.env up -d --build <service_name>

# Exec vào container để debug
docker exec -it <container_name> sh   # Alpine/Go containers
docker exec -it <container_name> bash # Python/Ubuntu containers

# Kiểm tra healthcheck log chi tiết
docker inspect <container_name> --format '{{json .State.Health.Log}}' | python3 -m json.tool

# Reset toàn bộ (xóa cả volumes)
docker compose down -v
```
