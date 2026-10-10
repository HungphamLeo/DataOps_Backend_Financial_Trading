# DataOps Backend Financial Trading

> **Binance Crypto + TCBS VN Securities** → Kafka → Delta Lake → Spark Streaming → Real-time Analytics API

---

## 🏗️ Architecture Overview

```
Binance WebSocket         → dataops-stream-producer (Go)
                                → Kafka (binance.trade.ticks)
                                → Redis (binance_{sym}_ticker, TTL 10s)

TCBS iFlash REST poll     → dataops-stream-producer (Go)
                                → Kafka (tcbs.market.snapshot)
                                → Redis (tcbs_{sym}_ticker, TTL 120s)

Kafka (binance.trade.ticks) → dataops_processing (Spark Streaming)
                                → InfluxDB (vwap measurement)
                                → Delta Silver (vwap_1m)

Binance REST /klines      → dataops_batch_api (Python/PySpark)
TCBS REST EOD/BSA         → dataops_batch_api (Python/PySpark)
                                → Delta Bronze (MinIO s3a://binance-datalake/)

dataops_backend_api (Go) ← gRPC → dataops_batch_api (port 50051)
                          ← Redis (ticker cache)
                          ← InfluxDB (Flux query: VWAP, RSI+MA signals)
                          ← MySQL (user/auth)
```

---

## 📦 Modules

| Module | Language | Role |
|---|---|---|
| `dataops-stream-producer/` | Go 1.27 | WebSocket Binance + REST poll TCBS → Kafka + Redis |
| `dataops_backend_api/` | Go 1.27 | REST API: JWT auth, Redis, InfluxDB, gRPC client |
| `dataops_batch_api/` | Python 3.11 + PySpark 3.5 | Batch ETL: Binance/TCBS → Delta Bronze. gRPC server :50051 |
| `dataops_processing/` | Python 3.11 + PySpark 3.5 | Spark Structured Streaming: VWAP + Volume Bars |
| `dataops-protobuf/` | Protobuf 3 | Schema contracts (CDM source of truth) |
| `infra_dataops_finance_trading/` | Docker Compose | Kafka KRaft, Redis, MySQL, InfluxDB, MinIO, Grafana+Loki |

---

## 🚀 Getting Started in 5 minutes

### Prerequisites

- Docker 24+ with Docker Compose v2
- `curl` and `jq` installed on host

### 1. Clone & configure

```bash
git clone https://github.com/your-org/DataOps_Backend_Financial_Trading.git
cd DataOps_Backend_Financial_Trading

# Copy env template and fill required secrets
cp .env.example .env
```

Minimum required values in `.env`:

```env
INFLUXDB_TOKEN=your-influxdb-token
TCBS_JWT_TOKEN=your-tcbs-jwt-token        # for TCBS market data
BINANCE_API_KEY=your-binance-api-key      # for batch historical data
BINANCE_SECRET_KEY=your-binance-secret
```

### 2. Start the full stack

```bash
docker-compose -f infra_dataops_finance_trading/docker-compose.yml up -d
```

Services started (first run may take ~3 minutes for images):

| Service | URL |
|---|---|
| Backend API | http://localhost:8080 |
| Grafana | http://localhost:3001 (admin / see .env) |
| Kafka UI | http://localhost:8082 |
| MinIO Console | http://localhost:9011 |
| InfluxDB | http://localhost:8086 |
| Loki (log aggregation) | http://localhost:3100 |

### 3. Run smoke test

```bash
bash scripts/smoke_test.sh
```

Expected output:
```
======================================================
  SMOKE TEST RESULTS
  PASSED: 12    FAILED: 0
======================================================
  ✓ SMOKE TEST PASSED — all 12 checks OK
```

---

## 📡 API Endpoints

All protected endpoints require `Authorization: Bearer <token>` header.

```
# Auth (public)
POST /api/v1/login
POST /api/v1/refresh-token
POST /api/v1/logout

# Binance Crypto
GET  /api/v1/crypto/ticker
GET  /api/v1/crypto/:symbol/ticker
GET  /api/v1/crypto/:symbol/ohlcv?interval=1d&limit=100
GET  /api/v1/crypto/:symbol/vwap?interval=5m
GET  /api/v1/crypto/:symbol/signal?groupId=1&timeframe=1h   # RSI(14)+MA(20)

# TCBS VN Stocks
GET  /api/v1/tcbs/ticker
GET  /api/v1/tcbs/:symbol/ticker
GET  /api/v1/tcbs/:symbol/ohlcv
GET  /api/v1/tcbs/:symbol/bsa

# TCBS Account Proxy
GET  /api/v1/tcbs/account/balance
GET  /api/v1/tcbs/account/assets
GET  /api/v1/tcbs/account/orders
GET  /api/v1/tcbs/derivative/positions
POST /api/v1/tcbs/order/place
```

---

## 🔍 Observability

### Distributed Tracing

Every HTTP request receives a `trace_id` (UUID) via `X-Trace-ID` header:
- **Client sends**: `curl -H "X-Trace-ID: my-request-id" ...`
- **Server auto-generates** if not provided
- **All log entries** include `{"trace_id": "..."}` in JSON output
- **Batch/Streaming jobs** generate `correlation_id` per job run

### Grafana Dashboards

Access at http://localhost:3001 (default: `admin` / password from `.env`)

| Dashboard | Description |
|---|---|
| `DataOps Pipeline Trace` | Cross-service log search by `trace_id` or `correlation_id` |
| `binance-vps-main` | VWAP, candles, market metrics |
| `Kafka` | Broker throughput, lag, partition health |

### LogQL queries for troubleshooting

```logql
# All errors in last 15m
{job=~"dataops-.*"} | json | level="ERROR"

# Trace a specific HTTP request across services
{job=~"dataops-.*"} | json | trace_id="abc123"

# Track a batch job run
{job="dataops-batch-api"} | json | correlation_id="uuid-here"

# Stream producer publish errors
{job="dataops-stream-producer"} | json | level="ERROR" | action="kafka.publish"
```

---

## 🧪 Running Tests

```bash
# Python unit tests (batch_api)
cd dataops_batch_api
python -m pytest tests/ -v

# Python unit tests (processing — no Spark required)
python -m pytest dataops_processing/tests/ -v

# Go build check
cd dataops_backend_api
go build ./...
```

---

## 📖 Port Allocation

| Service | Port |
|---|---|
| Backend API | 8080 |
| Batch API gRPC | 50051 |
| MySQL | 3306 |
| Redis | 6379 |
| InfluxDB | 8086 |
| Kafka (external) | 9092 |
| MinIO API | 9010 |
| MinIO Console | 9011 |
| Grafana | 3001 |
| Loki | 3100 |
| Spark Master | 7087 |
| Spark UI | 8095 |

---

## 📁 Key Files

| File | Purpose |
|---|---|
| `dataops-protobuf/market-data/market_data.proto` | **CDM source of truth** — all field names derive from here |
| `dataops_batch_api/cli/ingestion/schema/canonical.py` | Python canonical mappers (mirrors .proto) |
| `dataops_processing/jobs/vwap_job.py` | Spark Streaming VWAP pipeline |
| `dataops_processing/jobs/volume_bars_job.py` | De Prado Volume Bars batch job |
| `infra_dataops_finance_trading/docker-compose.yml` | Full infra definition |
| `scripts/smoke_test.sh` | E2E health + API validation |
| `export_task/` | Session notes, lesson learned, design decisions |
