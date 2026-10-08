# Binance + TCBS DataOps Platform — Master Design Document

> **Project:** Binance Crypto + TCBS iFlash Securities (Chứng khoán VN) → Kafka → Delta Lake → Analytics
> **Tech stack:** Go 1.27 · Apache Kafka 3.7 KRaft · PySpark 3.5 · Delta Lake · InfluxDB 2.x · Grafana · Docker Compose
> **Data sources:** Binance WebSocket/REST (Crypto) · TCBS iFlash Open API (VN Stocks + Derivatives)
> **Last updated:** 2026-10-07 — Replaced OKX+VPS references → Binance+TCBS

---

## Mục lục

1. [Kiến trúc tổng thể — 5 Modules](#1-kiến-trúc-tổng-thể--5-modules)
2. [Data Sources — Binance & TCBS](#2-data-sources--binance--tcbs)
3. [Tech Stack đầy đủ](#3-tech-stack-đầy-đủ)
4. [Module 1 — dataops_stream_producer](#4-module-1--dataops_stream_producer)
5. [Module 2 — dataops_batch_api](#5-module-2--dataops_batch_api)
6. [Module 3 — dataops_processing](#6-module-3--dataops_processing)
7. [Module 4 — dataops_backend_api](#7-module-4--dataops_backend_api)
8. [Module 5 — dataops_protobuf](#8-module-5--dataops_protobuf)
9. [Kafka Topics & Schema Design](#9-kafka-topics--schema-design)
10. [Request Flow — Use Cases thực tế](#10-request-flow--use-cases-thực-tế)
11. [Redis Key Patterns](#11-redis-key-patterns)
12. [Delta Lake Layers](#12-delta-lake-layers)
13. [Trạng thái hiện tại — Có vs Chưa có](#13-trạng-thái-hiện-tại--có-vs-chưa-có)
14. [Lộ trình phát triển tiếp theo](#14-lộ-trình-phát-triển-tiếp-theo)

---

## 1. Kiến trúc tổng thể — 5 Modules

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                     │
│                                                                           │
│   Binance WebSocket API        Binance REST API       TCBS iFlash API     │
│   Trade Ticks (real-time)      OHLCV klines           VN Stocks/Deriv     │
│   OrderBook depth5             Futures/Spot           REST + WebSocket     │
│   Liquidations (fstream)       (Batch · hourly)       openapi.tcbs.com.vn │
│   (stream · wss:9443)          api.binance.com        JWT Auth required    │
│                                                                           │
└────────┬────────────────────────────┬──────────────────────┬─────────────┘
         │ WebSocket                  │ REST (hourly)        │ REST + WSS
         ▼                            ▼                      ▼
┌─────────────────────┐  ┌────────────────────────────────────────────────┐
│ MODULE 1            │  │ MODULE 2                                        │
│ dataops_stream_     │  │ dataops_batch_api (Python)                      │
│ producer (Go 1.27)  │  │                                                 │
│                     │  │ Binance REST → Delta Bronze (candles)           │
│ · goroutine/symbol  │  │ TCBS REST → Delta Bronze (market snapshot,      │
│ · IBM/sarama Kafka  │  │   BSA supply/demand, intraday ticks)            │
│ · BinancePartitioner│  │ Pydantic validation before write                │
│ · Avro Schema Reg.  │  │ (Driven by External Orchestration / CLI)        │
│ · DLQ handler       │  └──────────────┬─────────────────────────────────┘
│ · TCBS WSS consumer │                 │
│ · Redis ticker cache│                 │
└────────┬────────────┘                 │
         │ Kafka Publish                │ Delta Bronze (MinIO)
         ▼                             ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                   APACHE KAFKA 3.7 (KRaft, 3 brokers, RF=3)              │
│                                                                          │
│  binance.trade.ticks          binance.funding.rates                      │
│  binance.orderbook.snapshot   binance.ohlcv.batch                        │
│  binance.liquidation.events   binance.alerts.funding-anomaly             │
│  tcbs.derivative.contracts    tcbs.alerts.unusual-volume                 │
│  vps.market.snapshot          platform.dlq                               │
└──────────────────────────┬───────────────────────────────────────────────┘
                           │
         ┌─────────────────┼──────────────────┐
         │ spark-streaming │  alert-group      │ influxdb-group
         ▼                 ▼                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ MODULE 3 — dataops_processing (PySpark 3.5)                              │
│                                                                          │
│  VWAP sliding window (5min/1min slide, Watermark 30s)                   │
│  Volume Bars — De Prado aggregate                                        │
│  Funding Rate Z-score rolling → alert Kafka topic                        │
│  TCBS derivative price aggregation                                       │
│  Foreign flow detection (buy_foreign_qty vs sell_foreign_qty)            │
│                                                                          │
│  → Delta Lake Silver (MinIO/S3A)    → InfluxDB 2.x (real-time)          │
└──────────────────────────┬───────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ MODULE 4 — dataops_backend_api (Go 1.27 · Gin)                           │
│                                                                          │
│  JWT Auth · Redis cache · MySQL (users, sessions)                       │
│  Query InfluxDB (real-time) + Delta Lake (historical) + Redis (cache)   │
│  Forward TCBS account endpoints (proxy với JWT token management)         │
│  REST: /api/v1/crypto/* · /api/v1/vnstock/* · /api/v1/tcbs/account/*   │
│  WebSocket: /ws/alerts · /ws/market-feed                                │
└──────────────────────────────────────────────────────────────────────────┘
                           │
         ┌─────────────────┼──────────────────┐
         ▼                 ▼                   ▼
      Trading UI       Grafana            Trading Bot
   (Web Dashboard)   Dashboards         (Algo Strategy)
```

---

## 2. Data Sources — Binance & TCBS

### Binance

| Data | Endpoint | Mode | Auth |
|------|----------|------|------|
| Trade Ticks (real-time) | `wss://stream.binance.com:9443/ws/stream?streams={sym}@trade` | WebSocket | ✅ Free public |
| OrderBook Depth 5 | `wss://...?streams={sym}@depth5` | WebSocket | ✅ Free public |
| Liquidations | `wss://fstream.binance.com/stream?streams=btcusdt@forceOrder` | WebSocket (fstream) | ✅ Free public |
| OHLCV Klines | `https://api.binance.com/api/v3/klines` | REST batch | ✅ Free public (1200 req/min) |
| Futures Klines | `https://fapi.binance.com/fapi/v1/klines` | REST batch | ✅ Free public |
| Account info | `https://api.binance.com/api/v3/account` | REST private | ❌ API Key + HMAC-SHA256 |

**Symbol format:** `BTCUSDT` (không gạch ngang). Spot và Futures dùng cùng symbol nhưng khác domain URL.

### TCBS iFlash

| Data | Endpoint | Mode | Auth |
|------|----------|------|------|
| Market snapshot (cổ phiếu) | `/tartarus/v1/tickerCommons?index=1` | REST poll (1 phút) | JWT Bearer |
| Foreign room | `/tartarus/v1/tickerSnaps?index=1` | REST poll | JWT Bearer |
| Derivatives info | `/tartarus/v1/derivatives` | REST / WSS | JWT Bearer |
| Derivative contracts WSS | `wss://openapi.tcbs.com.vn/api/v1/derivatives/contracts?token=<jwt>` | WebSocket | JWT query param |
| BSA supply/demand | `/nyx/v1/intraday/{ticker}/bsa` | REST (EOD) | JWT Bearer |
| Intraday tick history | `/nyx/v1/intraday/{ticker}/his/paging` | REST (EOD) | JWT Bearer |
| Account portfolio | `/aion/v1/accounts/{accountNo}/se` | REST private | JWT Bearer |
| Open positions | `/khronos/v1/account/portfolio/status` | REST private | JWT Bearer |
| Place order | `/akhlys/v1/accounts/{accountNo}/orders` | REST POST | JWT Bearer |

**Auth flow TCBS:** `POST /gaia/v1/oauth2/openapi/token` với `apiKey + OTP (iOTP)` → JWT (TTL ~8h)

---

## 3. Tech Stack đầy đủ

| Layer | Tech | Version | Module |
|-------|------|---------|--------|
| **Streaming Producer** | Go + IBM/sarama | 1.27 | dataops_stream_producer |
| **Streaming Consumer** | Go + segmentio/kafka-go | 1.27 | dataops_stream_producer |
| **WebSocket** | gorilla/websocket | — | dataops_stream_producer |
| **Batch Ingestion** | Python + requests | 3.11 | dataops_batch_api |
| **Orchestration** | External Orchestration Engine | — | External Repo / Control Plane |
| **Validation** | Pydantic | 2.6.4 | dataops_batch_api |
| **Stream Processing** | PySpark Structured Streaming | 3.5.1 | dataops_processing |
| **Data Lake** | Delta Lake + delta-spark | 3.1.0 | dataops_batch_api, dataops_processing |
| **Object Storage** | MinIO (S3-compatible) | latest | infra |
| **Messaging** | Apache Kafka KRaft | 3.7 | infra |
| **Schema Registry** | Confluent Schema Registry | 7.6 | infra |
| **Serialization** | Apache Avro | — | dataops_stream_producer |
| **Time-series DB** | InfluxDB 2.x | 2.7 | infra |
| **Cache** | Redis 7 | — | infra |
| **Database** | MySQL 8.0 | — | infra |
| **REST API** | Go + Gin | 1.27 | dataops_backend_api |
| **ORM** | GORM | 1.25 | dataops_backend_api |
| **Schema Contract** | Protobuf 3 | — | dataops_protobuf |
| **Dashboard** | Grafana | 10.0 | infra |
| **Container** | Docker Compose | — | infra |

---

## 4. Module 1 — dataops_stream_producer

**Role:** Real-time data gate. Là điểm duy nhất kết nối WebSocket tới Binance và TCBS, publish tất cả
events vào Kafka với đảm bảo at-least-once / exactly-once delivery.

```
dataops_stream_producer/
├── app/server.go                     # Wiring: config→redis→producer→sources
├── constants/
│   └── topics.go                     # Tất cả Kafka topic constants
├── pkg/
│   ├── configuration/configuration.go  # Config từ env vars
│   ├── producer/
│   │   ├── producer.go               # kafkaProducer (IBM/sarama) — PRODUCTION PATH
│   │   ├── transactional_producer.go # TransactionalProducer + BinancePartitioner
│   │   ├── dlq.go                    # DLQProducer standalone
│   │   ├── schema_registry.go        # SchemaRegistryClient
│   │   └── types.go                  # TradeEvent, OrderBookEvent, TCBSDerivativeEvent, ...
│   └── sources/
│       ├── binance/
│       │   ├── auth.go               # HMAC-SHA256 hex signing
│       │   └── websocket/producer.go # BinanceProducer — goroutine/symbol
│       └── tcbs/
│           ├── market_data/poller.go # TCBSPoller — REST poll + Redis cache
│           └── websocket/consumer.go # TCBSWSSConsumer — VN30F derivatives
```

**Kafka topics published:**

| Topic | Source | Key |
|-------|--------|-----|
| `binance.trade.ticks` | Binance WSS @trade | symbol |
| `binance.orderbook.snapshot` | Binance WSS @depth5 | symbol |
| `binance.liquidation.events` | Binance fstream @forceOrder | symbol |
| `tcbs.derivative.contracts` | TCBS WSS /api/v1/derivatives/contracts | symbol |

**Redis cache patterns (TTL theo update frequency):**

| Key | Nguồn | Content | TTL |
|-----|-------|---------|-----|
| `binance_{sym}_{stream}` | Binance WSS | `{"ts": ms}` latency monitor | 60s |
| `binance_{sym}_ticker` | Binance WSS @trade | price, size, side, buy/sell vol, trade_id, ts | 10s |
| `tcbs_{sym}_ticker` (cổ phiếu) | TCBSPoller REST | match_price, qty, vol, change, bid/ask, high, low | 120s |
| `tcbs_{sym}_ticker` (phái sinh) | TCBSWSSConsumer | match_price, qty, vol, change, bid/ask | 30s |

---

## 5. Module 2 — dataops_batch_api

**Role:** Historical/batch data ingestion. Pull dữ liệu REST API định kỳ từ Binance và TCBS,
validate với Pydantic, ghi vào Delta Lake Bronze (MinIO). Hoàn toàn tách biệt với real-time pipeline.

```
dataops_batch_api/
├── config.py                         # Paths, StorageOptions, YAMLLoader
├── cli/
│   ├── flows/
│   │   ├── config/
│   │   │   └── exchange_config.yaml  # Binance + TCBS config, instruments, topics
│   │   └── serving/
│   │       ├── bronze_flow.py        # Ingest → Delta Bronze (Binance + TCBS)
│   │       ├── silver_flow.py        # VWAP, funding anomaly Spark transforms
│   │       ├── backfill_flow.py      # Historical backfill (pagination)
│   │       └── daily_pipeline.py     # Orchestrated daily run
│   └── ingestion/
│       ├── sources/
│       │   ├── binance_client.py     # BinanceRestClient (HMAC-SHA256 hex auth)
│       │   ├── tcbs_client.py        # TCBSRestClient (JWT auth, all endpoints)
│       │   ├── tcbs_constants.yaml   # TCBS endpoints, enums, codes (source of truth)
│       │   └── vps_client.py         # VPS client (legacy, superseded by TCBS)
│       └── schema/
│           └── models.py             # Pydantic data contracts (Bronze validation)
└── platforms/
    └── processing/
        ├── spark_stack/spark_session.py  # SparkSession builder (MinIO + Delta)
        └── config/delta_utils.py         # DeltaBronzeWriter
```

### Batch Data Contracts (Pydantic models — `cli/ingestion/schema/models.py`)

**Binance:**
- `BinanceCandleRecord` — OHLCV từ `/api/v3/klines`: inst_id, inst_type, ts_ms, open/high/low/close, vol, vol_ccy, confirm

**TCBS Market Data (đã có):**
- `TCBSMarketRecord` — Full ticker snapshot từ `/tartarus/v1/tickerCommons`
- `TCBSTickerSnapRecord` — Foreign room từ `/tartarus/v1/tickerSnaps`
- `TCBSBSARecord` — Buy/Sell Analysis từ `/nyx/v1/intraday/{ticker}/bsa`
- `TCBSIntradayTickRecord` — Tick-by-tick từ `/nyx/v1/intraday/{ticker}/his/paging`

**TCBS Trading/Account (cần thêm):**
- `TCBSDerivativeRecord` — Contract info từ `/tartarus/v1/derivatives`
- `TCBSAccountCashRecord` — Cash balance từ `/aion/v1/accounts/{acct}/cashInvestments`
- `TCBSStockAssetRecord` — Stock holding từ `/aion/v1/accounts/{acct}/se`
- `TCBSOrderRecord` — Order entry từ `/aion/v1/accounts/{acct}/orders`
- `TCBSOpenPositionRecord` — Open derivative position từ `/khronos/v1/account/portfolio/status`

### Delta Lake Bronze Tables

| Table path | Data | Partition |
|-----------|------|-----------|
| `s3a://binance-datalake/delta/bronze/binance_candles_spot` | Binance Spot OHLCV | symbol, year, month |
| `s3a://binance-datalake/delta/bronze/binance_candles_futures` | Binance Futures OHLCV | symbol, year, month |
| `s3a://binance-datalake/delta/bronze/tcbs_market` | TCBS ticker snapshot | exchange, symbol, date |
| `s3a://binance-datalake/delta/bronze/tcbs_bsa` | TCBS BSA analytics | ticker, investor_type, date |
| `s3a://binance-datalake/delta/bronze/tcbs_intraday` | TCBS intraday ticks | ticker, date |

---

## 6. Module 3 — dataops_processing

**Role:** Stream processing và aggregation. Subscribe Kafka, tính toán real-time metrics,
ghi kết quả vào Delta Silver và InfluxDB.

**Không pull dữ liệu từ source** — chỉ tiêu thụ từ Kafka và transform.

```
dataops_processing/                   # (hiện tại là platforms/processing/)
├── jobs/
│   ├── vwap_job.py                   # Kafka → VWAP 5-min/1-min window → InfluxDB + Delta Silver
│   ├── funding_anomaly_job.py        # Funding rate Z-score → alert Kafka topic
│   ├── liquidation_cascade_job.py    # Liquidation event clustering (60s window)
│   └── tcbs_foreign_flow_job.py      # TCBS foreign buy/sell flow detection
└── config/
    └── spark_config.py               # SparkSession với Kafka + Delta + InfluxDB connectors
```

**Spark Consumer Groups:**

| Group ID | Topics consumed | Output |
|----------|----------------|--------|
| `spark-streaming-group` | `binance.trade.ticks` | VWAP → InfluxDB + Delta Silver |
| `alert-group` | `binance.funding.rates` | Z-score alerts → `binance.alerts.funding-anomaly` |
| `influxdb-group` | `binance.orderbook.snapshot` | OrderBook metrics → InfluxDB |
| `spark-tcbs-group` | `tcbs.derivative.contracts` | VN30F aggregation → InfluxDB |

---

## 7. Module 4 — dataops_backend_api

**Role:** Serving layer — không thu dữ liệu, chỉ phục vụ query từ clients (UI, bot, dashboard).

**Quan trọng:** TCBS account/trading APIs được **proxy qua đây** — backend giữ JWT token management,
client không cần trực tiếp gọi TCBS. Backend cache kết quả account (Redis TTL 30s).

```
dataops_backend_api/                  # Go 1.27, Gin
├── controller/
│   ├── crypto/                       # Binance endpoints
│   ├── vnstock/                      # TCBS market data
│   ├── tcbs_account/                 # TCBS account proxy (cash, orders, positions)
│   └── admin/
├── service/
│   ├── crypto_service.go             # InfluxDB query + Redis cache
│   ├── tcbs_market_service.go        # Redis cache tcbs_*_ticker
│   └── tcbs_account_service.go       # TCBS REST proxy + Redis cache
├── repository/
│   ├── influxdb/                     # InfluxDB 2.x Flux queries
│   ├── delta/                        # Delta Lake queries (DuckDB)
│   └── redis/                        # Redis cache layer
└── middleware/
    └── auth.go                       # JWT validation
```

### API Endpoints

**Market Data (nguồn: Redis cache + InfluxDB):**

| Endpoint | Latency | Nguồn |
|----------|---------|-------|
| `GET /api/v1/crypto/{sym}/ticker` | <5ms | Redis `binance_{sym}_ticker` |
| `GET /api/v1/crypto/{sym}/vwap` | 10–50ms | InfluxDB |
| `GET /api/v1/crypto/{sym}/candles?bar=1h` | 100–500ms | Delta Lake |
| `GET /api/v1/vnstock/{sym}/ticker` | <5ms | Redis `tcbs_{sym}_ticker` |
| `GET /api/v1/vnstock/{sym}/orderbook` | <5ms | Redis (from TCBSPoller) |
| `GET /api/v1/vnstock/market?index=VN30` | <5ms | Redis |

**TCBS Account (proxy — cần JWT user token):**

| Endpoint | TCBS upstream | Ghi chú |
|----------|--------------|---------|
| `GET /api/v1/tcbs/account/cash` | `/aion/v1/accounts/{acct}/cashInvestments` | Cache 30s |
| `GET /api/v1/tcbs/account/stock-asset` | `/aion/v1/accounts/{acct}/se` | Cache 30s |
| `GET /api/v1/tcbs/account/orders` | `/aion/v1/accounts/{acct}/orders` | No cache |
| `POST /api/v1/tcbs/order/place` | `/akhlys/v1/accounts/{acct}/orders` | No cache |
| `GET /api/v1/tcbs/derivative/positions` | `/khronos/v1/account/portfolio/status` | Cache 10s |
| `POST /api/v1/tcbs/derivative/order/place` | `/khronos/v1/order/place` | No cache |

---

## 8. Module 5 — dataops_protobuf

**Role:** Schema contracts giữa producers (Go) và consumers (Spark/Python).

```
dataops_protobuf/
├── market-data/
│   ├── trade_tick.proto             # BinanceTradeEvent
│   ├── order_book.proto             # BinanceOrderBookEvent
│   └── tcbs_derivative.proto        # TCBSDerivativeEvent
├── analytics/
│   ├── vwap.proto                   # VWAPBar
│   └── funding_rate.proto           # FundingRateEvent
├── vnstock/
│   └── market_snapshot.proto        # VPSMarketSnapshot (legacy)
└── batch-data/
    └── ohlcv_batch.proto            # OHLCVBatch
```

**Scope sử dụng:**
- `dataops_stream_producer` → serialize trước khi publish Kafka
- `dataops_processing` (Spark) → deserialize khi consume Kafka
- `dataops_backend_api` → deserialize nếu consume Kafka real-time (WebSocket push)

**Không dùng Protobuf cho:**
- TCBS account/order data — dùng JSON REST contract
- Delta Lake storage — dùng Parquet/Delta schema
- Redis cache — dùng JSON string

---

## 9. Kafka Topics & Schema Design

### Topic Architecture

| Topic | Partitions | Retention | Key | Producer |
|-------|-----------|-----------|-----|---------|
| `binance.trade.ticks` | 12 | 7d | symbol | dataops_stream_producer |
| `binance.orderbook.snapshot` | 6 | 1d | symbol | dataops_stream_producer |
| `binance.liquidation.events` | 3 | 30d | symbol | dataops_stream_producer |
| `binance.funding.rates` | 3 | 90d | symbol | dataops_stream_producer |
| `binance.ohlcv.batch` | 3 | 7d | symbol | dataops_batch_api |
| `binance.alerts.funding-anomaly` | 1 | 7d | — | dataops_processing |
| `binance.alerts.liquidation` | 1 | 7d | — | dataops_processing |
| `tcbs.derivative.contracts` | 3 | 3d | symbol | dataops_stream_producer |
| `platform.dlq` | 3 | 30d | — | all producers |

### BinancePartitioner Strategy

```
BTC* symbols → Partition 0   (~40% volume)
ETH* symbols → Partition 1   (~20% volume)
Others (altcoins, VPS) → Partition 2..N-1 (FNV32a hashing)
RequiresConsistency() = true → absolute ordering per instrument
```

---

## 10. Request Flow — Use Cases thực tế

### Use Case 1: Lấy TCBS account hiện tại (cash + holdings)

```
User / Trading UI
    │  GET /api/v1/tcbs/account/cash
    │  Header: Authorization: Bearer <user_jwt>
    ▼
dataops_backend_api
    ├─ Validate user JWT (local)
    ├─ Check Redis cache "tcbs_account_{userID}_cash" (TTL 30s)
    │      CACHE HIT  → return immediately (<5ms)
    │      CACHE MISS ↓
    ├─ Forward to TCBS: GET /aion/v1/accounts/{acctNo}/cashInvestments
    │     Header: Authorization: Bearer <tcbs_service_jwt>
    │     (Service JWT — stored in env, auto-refresh khi expire)
    ├─ Validate response, map → TCBSAccountCashRecord
    ├─ Write Redis cache (TTL 30s)
    └─ Return JSON response

Total latency: <5ms (cache) or 50–200ms (TCBS call)
```

### Use Case 2: Lấy real-time price Binance

```
User / Trading UI
    │  GET /api/v1/crypto/BTCUSDT/ticker
    ▼
dataops_backend_api
    ├─ Redis GET "binance_btcusdt_ticker"   ← set bởi dataops_stream_producer
    │     {"price":"67500.12","size":"0.05","side":"buy",
    │      "buy_vol":1250000,"sell_vol":1180000,"ts":1728289200000}
    └─ Return immediately (<5ms)
```

### Use Case 3: Lấy buy/sell volume, VWAP chart

```
User / Trading UI
    │  GET /api/v1/crypto/BTCUSDT/vwap?window=5m
    ▼
dataops_backend_api
    ├─ Check Redis cache (TTL 60s)
    │      CACHE HIT → return
    │      CACHE MISS ↓
    ├─ Query InfluxDB: flux query
    │     from(bucket:"binance-vwap") |> range(start:-1h)
    │     |> filter(fn:(r) => r.symbol == "BTCUSDT")
    │     (Written bởi dataops_processing Spark VWAP job)
    ├─ Write Redis cache (TTL 60s)
    └─ Return OHLCV + VWAP bars

Total latency: 10–50ms
```

### Use Case 4: Lấy historical candles (3 tháng)

```
User / Trading UI
    │  GET /api/v1/crypto/BTCUSDT/candles?interval=1h&from=2026-07-01
    ▼
dataops_backend_api
    ├─ Check Redis (TTL không phù hợp cho historical)
    ├─ Query Delta Lake via DuckDB:
    │     SELECT * FROM delta.`s3a://binance-datalake/delta/bronze/binance_candles_spot`
    │     WHERE symbol='BTCUSDT' AND ts_ms >= epoch_ms('2026-07-01')
    │     ORDER BY ts_ms
    │     (Filled by dataops_batch_api bronze_flow hourly)
    └─ Return array of OHLCV records

Total latency: 100ms–2s (depending on data size)
```

### Use Case 5: Đặt lệnh phái sinh VN30F

```
User / Trading Bot
    │  POST /api/v1/tcbs/derivative/order/place
    │  Body: {accountId, subAccountId, side:"B", symbol:"VN30F2411",
    │          price:1250.5, volume:2, orderType:"0", refId:"uuid"}
    │  Header: Authorization: Bearer <user_jwt>
    ▼
dataops_backend_api
    ├─ Validate user JWT
    ├─ Validate request body (price within ceil/floor range)
    ├─ Get TCBS service JWT (cached, auto-refresh)
    ├─ POST /khronos/v1/order/place with service JWT
    ├─ Return {rc:"1", data:[{orderNo, pk_orderNo, showPrice}]}
    └─ Log order event (audit log → MySQL)

No caching — order placement must be real-time
```

---

## 11. Redis Key Patterns

| Key Pattern | Producer | Consumer | Content | TTL |
|-------------|---------|----------|---------|-----|
| `binance_{sym}_{stream}` | stream_producer (Binance WSS) | dataops_backend_api (monitoring) | `{"ts":ms}` latency | 60s |
| `binance_{sym}_ticker` | stream_producer (Binance @trade) | dataops_backend_api (GET /ticker) | price, side, vol, trade_id, ts | 10s |
| `tcbs_{sym}_ticker` (stock) | stream_producer (TCBSPoller) | dataops_backend_api (GET /vnstock) | match_price, qty, vol, bid/ask, high, low | 120s |
| `tcbs_{sym}_ticker` (deriv) | stream_producer (TCBSWSSConsumer) | dataops_backend_api (GET /derivative) | match_price, qty, vol, bid/ask | 30s |
| `tcbs_account_{userID}_cash` | dataops_backend_api | dataops_backend_api | TCBS cash balance response | 30s |
| `tcbs_account_{userID}_asset` | dataops_backend_api | dataops_backend_api | TCBS stock asset response | 30s |
| `vwap_{sym}_5m` | dataops_backend_api (cache) | dataops_backend_api | VWAP data aggregated | 60s |

---

## 12. Delta Lake Layers

### Bronze (Raw — ingested by dataops_batch_api)

| Table | Source | Schedule |
|-------|--------|---------|
| `bronze/binance_candles_spot` | Binance REST /klines | Hourly |
| `bronze/binance_candles_futures` | Binance fapi/klines | Hourly |
| `bronze/tcbs_market` | TCBS /tartarus/v1/tickerCommons | Every 1min (trading hours) |
| `bronze/tcbs_bsa` | TCBS /nyx BSA endpoints | Daily (15:05 ICT) |
| `bronze/tcbs_intraday` | TCBS /nyx his/paging | Daily (15:10 ICT) |

### Silver (Cleaned/Aggregated — written by dataops_processing)

| Table | Computed from | Schedule |
|-------|--------------|---------|
| `silver/vwap_1m` | Kafka `binance.trade.ticks` | Continuous streaming |
| `silver/volume_bars` | Kafka `binance.trade.ticks` | Continuous streaming |
| `silver/funding_stats` | Kafka `binance.funding.rates` | Continuous streaming |
| `silver/tcbs_foreign_flow` | `bronze/tcbs_market` | Daily |

### Gold (Business metrics — for direct query by dataops_backend_api)

| Table | Built from | Purpose |
|-------|-----------|---------|
| `gold/ohlcv_daily` | Silver VWAP | Daily summary chart |
| `gold/market_indicators` | Silver all | Technical indicators (RSI, MACD) |
| `gold/tcbs_sector_flow` | Silver foreign_flow | Sector rotation analysis |

---

## 13. Trạng thái hiện tại — Có vs Chưa có

### ✅ Đã có (production ready)

**dataops_stream_producer:**
- Binance WSS producer (BTC, ETH, SOL) với BinancePartitioner
- IBM/sarama producer với acks=all, idempotent, Snappy compression
- TCBS market data REST poller (TCBSPoller) + Redis cache đầy đủ
- TCBS WSS consumer cho VN30F derivatives (start khi TCBS_WSS_SYMBOLS != empty)
- DLQ handler (inline sarama + standalone kafka-go)
- Schema Registry client (với JSON fallback khi registry offline)

**dataops_batch_api:**
- `BinanceRestClient` — OHLCV klines với HMAC-SHA256 auth, retry, rate limiting
- `TCBSRestClient` — full JWT auth, all market data + account endpoints
- `TCBSConstants` (tcbs_constants.yaml) — tất cả endpoints, enums, codes
- Pydantic models: `BinanceCandleRecord`, `TCBSMarketRecord`, `TCBSTickerSnapRecord`, `TCBSBSARecord`, `TCBSIntradayTickRecord`
- `bronze_flow.py` — Binance + TCBS ingest → Delta Bronze
- `silver_flow.py` — VWAP + funding anomaly Spark transforms
- `exchange_config.yaml` — single config source (Binance + TCBS + Delta paths)
- `config.py` — `Paths`, `StorageOptions`, `_YAMLLoader`

### ⚠️ Cần bổ sung / hoàn thiện

**dataops_batch_api:**
- [ ] Pydantic models: `TCBSDerivativeRecord`, `TCBSAccountCashRecord`, `TCBSStockAssetRecord`, `TCBSOrderRecord`, `TCBSOpenPositionRecord`
- [ ] Bronze flow cho TCBS derivatives (tartarus/derivatives)
- [ ] docker-compose: thêm `tcbs.derivative.contracts` topic vào Kafka init

**dataops_processing:**
- [ ] VWAP Spark job (platforms/processing chỉ có skeleton)
- [ ] Funding anomaly Z-score job
- [ ] TCBS foreign flow detection job

**dataops_backend_api:**
- [ ] TCBS account proxy endpoints (`/api/v1/tcbs/account/*`)
- [ ] TCBS order placement proxy (`/api/v1/tcbs/order/*`, `/api/v1/tcbs/derivative/order/*`)
- [ ] WebSocket real-time price feed (`/ws/market-feed`)

**dataops_protobuf:**
- [ ] `tcbs_derivative.proto` — TCBSDerivativeEvent schema
- [ ] `tcbs_derivative_event.avsc` — Avro schema cho Schema Registry

### ❌ Chưa có (cần viết mới)

- Binance Futures WebSocket producer (fstream domain — riêng với spot stream)
- Binance REST client cho account/order management (private endpoints)
- dataops_processing Spark jobs (chưa implement bất kỳ job nào)
- dataops_backend_api TCBS account proxy layer

---

## 14. Lộ trình phát triển tiếp theo

### Phase 1 — Complete Batch Layer (Tuần 1-2)

- [ ] Thêm Pydantic models cho TCBS account/trading data vào `schema/models.py`
- [ ] Thêm `TCBSDerivativeRecord` + Bronze flow cho `/tartarus/v1/derivatives`
- [ ] Update `docker-compose.yml`: thêm `tcbs.derivative.contracts` topic
- [ ] Thêm constants `TCBS_BASE_URL`, `BINANCE_BASE_URL`, `BINANCE_FUTURES_BASE_URL` cho docker-compose

### Phase 2 — Spark Processing (Tuần 3-5)

- [ ] Implement VWAP Spark job: `binance.trade.ticks` → InfluxDB + Delta Silver
- [ ] Implement funding anomaly Z-score: `binance.funding.rates` → alert topic
- [ ] TCBS foreign flow: `bronze/tcbs_market` → Delta Silver

### Phase 3 — Backend API Serving (Tuần 6-8)

- [ ] TCBS account proxy controller + service + Redis cache
- [ ] TCBS order placement proxy (với audit log → MySQL)
- [ ] WebSocket real-time feed (`/ws/market-feed`)
- [ ] Historical candles query từ Delta Lake via DuckDB

### Phase 4 — Production Hardening (Tuần 9-10)

- [ ] Kafka HA KRaft (3 brokers, RF=3, `min.insync.replicas=2`)
- [ ] TCBS JWT token auto-refresh pipeline (monitor expiry, refresh trước 1h)
- [ ] Binance WebSocket ping/pong 300s keepalive
- [ ] Consumer lag monitoring, Grafana dashboard

---

## Ghi chú kỹ thuật quan trọng

### TCBS JWT Token Management

```
TCBS JWT có TTL ~8h (không document chính thức).
dataops_stream_producer: token stored in config/env, manual refresh hiện tại.
dataops_backend_api:     cần implement auto-refresh — monitor token age,
                          refresh khi còn < 1h (token_refresh_threshold_sec = 3600).
TCBS WSS: token qua query param — wss://...?token=<jwt>
           reconnect loop tự động retry khi token expire (dial error).
```

### Binance vs TCBS Data Separation

```
Market data realtime    → Redis (cả 2 sources)
Market data historical  → Delta Lake Bronze (cả 2 sources)
Account/Order data      → Direct TCBS REST proxy (không qua Kafka/Delta)
Crypto account/order    → Direct Binance REST (nếu cần — private endpoints)
```

### Separation of Concerns — Quick Reference

| Câu hỏi | Module trả lời | Cách |
|---------|--------------|------|
| "Giá BTCUSDT hiện tại bao nhiêu?" | dataops_backend_api → Redis | <5ms |
| "3 tháng trước BTCUSDT thế nào?" | dataops_backend_api → Delta Lake | 100ms–2s |
| "VWAP 5min của BTCUSDT?" | dataops_backend_api → InfluxDB | 10–50ms |
| "Tôi đang có bao nhiêu tiền trong TCBS?" | dataops_backend_api → TCBS REST | 50–200ms |
| "Tôi đang có vị thế VN30F nào?" | dataops_backend_api → TCBS REST | 50–200ms |
| "Tôi muốn đặt lệnh mua VN30F" | dataops_backend_api → TCBS /khronos | 100–500ms |
| "Có sự kiện bất thường nào không?" | dataops_backend_api → InfluxDB alerts | 10–50ms |
