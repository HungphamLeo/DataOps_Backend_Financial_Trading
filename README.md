# DataOps Backend Financial Trading

> **A production-grade multi-source financial data platform** — real-time streaming from **Binance Crypto** (WebSocket) and **TCBS VN Securities** (REST polling + WebSocket), batch ingestion into a **Delta Lake Medallion Lakehouse** on MinIO, Spark Structured Streaming analytics (VWAP, Volume Bars, Order Flow, Indicator Matrix, Market Breadth), and a REST/gRPC serving API.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Technology Stack](#2-technology-stack)
3. [Project Structure & Module Roles](#3-project-structure--module-roles)
4. [Module Deep Dive](#4-module-deep-dive)
   - [dataops-stream-producer](#41-dataops-stream-producer-go-127)
   - [dataops_backend_api](#42-dataops_backend_api-go-127)
   - [dataops_batch_api](#43-dataops_batch_api-python-311--pyspark-35)
   - [dataops_processing](#44-dataops_processing-python-311--pyspark-35)
   - [dataops-protobuf](#45-dataops-protobuf-protobuf-3)
5. [Data Boundary & Canonical Data Model](#5-data-boundary--canonical-data-model)
6. [Kafka Topics & Partitioning Strategy](#6-kafka-topics--partitioning-strategy)
7. [Analytics Computations](#7-analytics-computations)
8. [API Reference](#8-api-reference)
9. [Infrastructure & Port Allocation](#9-infrastructure--port-allocation)
10. [Getting Started — Docker Setup](#10-getting-started--docker-setup)
11. [Running Individual Services Locally](#11-running-individual-services-locally)
12. [Testing](#12-testing)
13. [Smoke Test (E2E Validation)](#13-smoke-test-e2e-validation)
14. [Observability & Grafana](#14-observability--grafana)
15. [Key Configuration Files](#15-key-configuration-files)

---

## 1. Architecture Overview

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║  DATA SOURCES                                                                  ║
╠══════════════════════════════════════════════════════════════════════════════════╣
║                                                                                ║
║  Binance WebSocket ──────────────────────────────────────────────────┐         ║
║    @trade, @depth5, fstream (liquidations)                          │         ║
║                                                                      ▼         ║
║                                              ┌────────────────────────────┐   ║
║  TCBS iFlash REST poll ──────────────────────►  dataops-stream-producer   │   ║
║    HOSE/HNX/UPCOM snapshots (cron)          │     (Go 1.27 / IBM/sarama) │   ║
║                                              └──────┬─────────────────────┘   ║
║  TCBS VN30F Derivatives WSS ────────────────────────┘                         ║
╚══════════════════════════════════════════════════════════════════════════════════╝
                    │                │
                    ▼                ▼
         ┌──────────────┐   ┌────────────────────────────────────────────────┐
         │  Redis 7     │   │  Apache Kafka 3.7 KRaft (3 brokers)           │
         │  TTL 10–120s │   │  binance.trade.ticks                          │
         │  ticker cache│   │  tcbs.market.snapshot                         │
         └──────────────┘   │  binance.orderbook.snapshot                   │
                            │  binance.liquidation.events                   │
                            │  tcbs.derivatives.ticks          + DLQ topics │
                            └──────────────┬─────────────────────────────────┘
                                           │
                         ┌─────────────────▼──────────────────────────┐
                         │      dataops_processing  (Spark Streaming) │
                         │  VWAP · Order Flow CVD · 5 Streaming Jobs  │
                         └──────────┬────────────────────┬────────────┘
                                    ▼                    ▼
                           InfluxDB v2              Delta Silver
                           (measurement:            (s3a://binance-datalake/
                            vwap, order_flow)        delta/silver/vwap_1m)

╔═══════════════════════════════════════════════════════════════╗
║  BATCH INGESTION LAYER                                       ║
╠═══════════════════════════════════════════════════════════════╣
║  Binance REST /api/v3/klines ──────────────────────┐         ║
║  TCBS iFlash EOD / BSA / Ticks / Margin ──────────►│         ║
║                                          dataops_batch_api   ║
║                                          (Python/PySpark)    ║
║                                                    │         ║
║                                                    ▼         ║
║                             Delta Bronze (MinIO s3a://)      ║
║                             binance_candles_spot/futures     ║
║                             tcbs_market_snapshot/eod/bsa     ║
╚═══════════════════════════════════════════════════════════════╝
                         │
           ┌─────────────▼────────────────────────────────────────┐
           │         dataops_batch_api gRPC Server (:50051)       │
           └─────────────────────────┬────────────────────────────┘
                                     │ gRPC
╔════════════════════════════════════▼════════════════════════════════╗
║         dataops_backend_api  — REST API (Go 1.27 / Gin)           ║
║                                                                    ║
║  JWT Auth ← MySQL 8.0                                             ║
║  Real-time tickers ← Redis (< 5ms)                                ║
║  VWAP / Signals ← InfluxDB Flux queries (~20ms)                   ║
║  Historical OHLCV ← gRPC → Delta Lake (~50ms)                     ║
║  TCBS Account Proxy → TCBS OpenAPI (transactional, no Kafka)      ║
╚════════════════════════════════════════════════════════════════════╝
                         │
                    REST API
                    :8080
                         │
                 ┌───────▼────────┐
                 │   Clients /    │
                 │   Dashboards   │
                 └────────────────┘
```

---

## 2. Technology Stack

| Layer | Technology | Version |
|---|---|---|
| **Streaming Producer** | Go, IBM/sarama, Gorilla WebSocket, go-redis | Go 1.27 |
| **Backend API** | Go, Gin, GORM, gRPC, JWT, Zap | Go 1.27 |
| **Batch ETL** | Python, PySpark, Delta Lake, Pydantic, APScheduler | Python 3.11 / PySpark 3.5.1 |
| **Stream Processing** | Python, PySpark Structured Streaming, influxdb-client | Python 3.11 / PySpark 3.5.1 |
| **Schema Contract** | Protobuf 3, gRPC | proto3 |
| **Message Bus** | Apache Kafka KRaft (no ZooKeeper) | 3.7 / 3 brokers |
| **Object Storage** | MinIO (S3-compatible), Delta Lake ACID | delta-spark 3.1.0 |
| **Time-Series DB** | InfluxDB v2, Flux query language | v2 |
| **Cache** | Redis | 7 |
| **Relational DB** | MySQL | 8.0 |
| **Observability** | Grafana, Loki, Promtail, Prometheus | latest |
| **Container Orchestration** | Docker Compose v2 | — |

---

## 3. Project Structure & Module Roles

```
DataOps_Backend_Financial_Trading/
├── dataops-stream-producer/     # ① Real-time data ingestion (Go)
├── dataops_backend_api/         # ② REST API serving layer (Go)
├── dataops_batch_api/           # ③ Batch ETL + gRPC server (Python)
├── dataops_processing/          # ④ Spark Streaming analytics (Python)
├── dataops-protobuf/            # ⑤ Protobuf CDM schema contracts
├── infra_dataops_finance_trading/  # Docker Compose + infra configs
├── shared/platform/             # Shared Go packages (DI, observability, lineage)
├── scripts/smoke_test.sh        # E2E validation script
├── grafana/                     # Grafana dashboards
├── kafka-connect-s3-sink.json   # Kafka Connect S3 sink (reference config)
├── docs/                        # Technical docs and API spec
└── export_task/                 # Session notes, lesson learned, design decisions
```

| Module | Language | Responsibility | Does NOT |
|---|---|---|---|
| `dataops-stream-producer` | Go 1.27 | WebSocket/REST ingestion → Kafka + Redis | Compute, aggregate, write Delta Lake |
| `dataops_backend_api` | Go 1.27 | REST API, JWT auth, TCBS account proxy | Write to Kafka or Delta Lake |
| `dataops_batch_api` | Python 3.11 | REST pull → Pydantic validate → Delta Bronze + gRPC server | Consume Kafka |
| `dataops_processing` | Python 3.11 | Kafka → Spark Streaming analytics → InfluxDB + Delta Silver/Gold | Pull REST APIs |
| `dataops-protobuf` | Protobuf 3 | Schema source of truth (CDM) | Runtime logic |

---

## 4. Module Deep Dive

### 4.1 `dataops-stream-producer` (Go 1.27)

The real-time data ingestion layer. Subscribes to live market feeds and publishes normalized events to Kafka, with Redis for sub-second ticker snapshots.

#### File Structure

```
dataops-stream-producer/
├── main.go                                  # Entry point
├── app/server.go                            # Service wiring (DI)
├── constants/topics.go                      # All Kafka topic name constants
├── pkg/
│   ├── configuration/configuration.go       # Config from env vars
│   ├── messaging/
│   │   ├── producer.go                      # Generic Kafka producer interface
│   │   ├── consumer.go / consumer_group.go  # Consumer helpers
│   │   ├── codec.go / codec_protobuf.go     # Avro + Protobuf codecs
│   │   ├── dlq.go                           # Dead Letter Queue handler
│   │   ├── headers.go                       # Kafka message headers
│   │   ├── retry_policy.go                  # Retry with backoff
│   │   └── topics.go                        # Topic registry
│   ├── producer/
│   │   ├── producer.go                      # kafkaProducer — IBM/sarama SyncProducer (PRODUCTION PATH)
│   │   ├── transactional_producer.go        # TransactionalProducer + BinancePartitioner
│   │   ├── dlq.go                           # DLQProducer (kafka-go)
│   │   ├── schema_registry.go               # Confluent Schema Registry client
│   │   └── types.go                         # TradeEvent, OrderBookEvent, TCBSSnapshotEvent
│   ├── schemas/                             # Avro schema files (.avsc)
│   │   ├── binance_trade_event.avsc
│   │   ├── binance_orderbook_snapshot.avsc
│   │   ├── binance_liquidation_event.avsc
│   │   ├── tcbs_market_snapshot.avsc
│   │   ├── tcbs_derivative_event.avsc
│   │   ├── tcbs_intraday_tick_event.avsc
│   │   └── tcbs_bsa_event.avsc
│   └── sources/
│       ├── binance/
│       │   ├── auth.go                      # HMAC-SHA256 hex signing
│       │   └── websocket/producer.go        # BinanceProducer — goroutine per symbol
│       └── tcbs/
│           ├── client/client.go             # TCBS HTTP client
│           ├── constants/constants.go       # TCBS endpoints, market hours
│           ├── market_data/poller.go        # TCBSPoller — REST poll cron
│           └── websocket/consumer.go        # VN30F Derivatives WSS consumer
├── redis/                                   # Redis service wrapper
└── monitoring/                              # Prometheus metrics
```

#### Binance WebSocket Producer

- **Combined stream URL**: `wss://stream.binance.com:9443/ws/stream?streams={sym}@trade/{sym}@depth5`
- **Routing**: `@trade` events → `PublishTrade()` → topic `binance.trade.ticks`; `@depth` events → `PublishOrderBook()` → topic `binance.orderbook.snapshot`
- **Redis write**: `binance_{symbol_lower}_ticker` (TTL 10s) — pure snapshot of `{price, size, side, trade_id, ts}`. **No accumulation** — aggregation is `dataops_processing`'s responsibility.
- **Auto-reconnect** with exponential backoff on disconnect.
- **Kafka producer config**: `acks=all`, `Idempotent=true`, `Snappy` compression, 10ms batch timeout.

#### TCBS iFlash REST Poller

- **Cron schedule**: `*/1 9-11 * * 1-5` (morning session) + `*/1 13-14 * * 1-5` (afternoon session) — Vietnam HOSE market hours only.
- **Redis write**: `tcbs_{symbol}_ticker` (TTL 120s).
- **Supported markets**: HOSE, HNX, UPCOM.
- **Env**: `TCBS_SYMBOLS=VCB,VHM,TCB,VIC,HPG`

#### TCBS VN30F Derivatives WSS Consumer

- Activated only when `TCBS_WSS_SYMBOLS` env var is set.
- Publishes to topic `tcbs.derivatives.ticks`.

#### BinancePartitioner (Custom Kafka Partitioner)

```
Partition 0  → BTC*   (~40% volume)
Partition 1  → ETH*   (~20% volume)
Partition 2..N-1 → altcoins + TCBS stocks → FNV32a hash(symbol)
```

#### Producer Environment Variables

```env
KAFKA_URL=localhost:9092
REDIS_ADDRESS=localhost:6379
BINANCE_WS_URL=wss://stream.binance.com:9443/ws
BINANCE_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
TCBS_URL_LOGIN=https://iboard-query.ssi.com.vn/...
TCBS_SYMBOLS=VCB,VHM,TCB,VIC,HPG
TCBS_JWT_TOKEN=<your-tcbs-jwt-token>
TCBS_WSS_SYMBOLS=VN30F2410          # optional — enables derivatives WSS
SCHEMA_REGISTRY_URL=http://localhost:8081
MODE=dev
```

---

### 4.2 `dataops_backend_api` (Go 1.27)

The serving layer — a Gin REST API that aggregates data from Redis, InfluxDB, gRPC (batch), MySQL, and the TCBS OpenAPI proxy.

#### File Structure

```
dataops_backend_api/
├── main.go
├── app/                             # App bootstrap, router setup
├── api/                             # HTTP handlers (controllers)
├── services/
│   ├── analytics/crypto.go          # CryptoService — VWAP, OHLCV, ticker, signal
│   ├── authentication/              # JWT auth (login, refresh, logout)
│   ├── tcbs/                        # TCBS market data service
│   ├── tcbs_account/                # TCBS Account Proxy (cash, assets, orders, derivatives)
│   ├── accounting/                  # Account P&L calculations
│   ├── indicator/                   # Indicator computations
│   ├── portfolio/                   # Portfolio overview, NAV, allocation
│   ├── risk/                        # Pre-trade risk checks
│   ├── vnstock/                     # VN stock data helpers
│   └── crypto_account/              # Crypto account helpers
├── repository/
│   └── batch/
│       ├── client.go                # BatchClient gRPC interface (GetBinanceOHLCV, GetTCBSMarketEOD, ...)
│       └── batchdata/batch_data.go  # Go structs mirror batch_data.proto
├── models/                          # Domain models (BinanceTradeTick, OHLCV, VWAPPoint, IndicatorSignal, ...)
├── middlewares/                     # JWT auth, CORS, trace-ID injection
├── configuration/                   # Config from env vars
├── infrastructure/                  # DB, Redis, InfluxDB clients
├── kafka/                           # Kafka consumer (for backend subscription)
├── redis/                           # Redis service wrapper
├── logging/                         # Structured logging (zap)
├── observability/                   # Prometheus metrics
└── Dockerfile
```

#### Data Source Routing

| Request type | Source | Latency target |
|---|---|---|
| Real-time ticker (Binance) | Redis `binance_{sym}_ticker` TTL 10s | < 5ms |
| Real-time ticker (TCBS) | Redis `tcbs_{sym}_ticker` TTL 120s | < 5ms |
| Real-time order book depth 5 | Redis `binance_{sym}_depth5` TTL 10s | < 5ms |
| Historical OHLCV (4h, 1d) | gRPC → `dataops_batch_api` :50051 → Delta Lake | ~50ms |
| VWAP + intraday indicators | InfluxDB v2 Flux query (`measurement=vwap`) | ~20ms |
| RSI(14) + MA(20) signal | InfluxDB Flux → in-process computation | ~30ms |
| TCBS Account (cash, portfolio, orders) | TCBS OpenAPI proxy | ~200ms |
| JWT Auth / User session | MySQL 8.0 (GORM) | ~10ms |

#### `CryptoService` — Key Methods

- **`GetTicker(symbol)`** — reads `binance_{sym}_ticker` from Redis
- **`GetVWAP(symbol, interval, from, to)`** — Flux query: `measurement="vwap"`, `tag: symbol+interval`
- **`GetOHLCV(symbol, interval, from, to)`** — gRPC (4h/1d) or InfluxDB fallback (intraday)
- **`GetOrderBook(symbol)`** — reads `binance_{sym}_depth5` from Redis
- **`GetFundingRate(symbol, from, to)`** — Flux query: `measurement="funding_rate"`
- **`GetLiquidations(symbol, from, to)`** — Flux query: `measurement="liquidations"`
- **`ComputeSignal(symbol, timeframe)`** — fetches last 21 VWAP points from InfluxDB, runs RSI(14) + MA(20), classifies: `BUY / SELL / HOLD / NEUTRAL` with confidence score

#### Signal Classification Logic (in-process, `analytics/crypto.go`)

```
RSI > 70 AND price > MA20  →  SELL  (confidence 0.75)
RSI < 30 AND price < MA20  →  BUY   (confidence 0.75)
RSI > 60 OR  price > MA20  →  HOLD  (confidence 0.55)
RSI < 40 OR  price < MA20  →  HOLD  (confidence 0.55)
default                    →  NEUTRAL (confidence 0.50)
```

#### TCBS Account Proxy

- Proxies directly to TCBS OpenAPI with Bearer token.
- **No Kafka, no Delta Lake** — transactional data is real-time only.
- Redis cache: account balance/assets TTL 30s, derivative positions TTL 10s.

#### Backend API Environment Variables

```env
DATABASE_DSN=root:password@tcp(localhost:3306)/dataops?parseTime=true
REDIS_ADDRESS=localhost:6379
INFLUXDB_URL=http://localhost:8086
INFLUXDB_TOKEN=<your-token>
INFLUXDB_ORG=binance-vps-platform
INFLUXDB_BUCKET=market-data
BATCH_GRPC_URL=localhost:50051
TCBS_JWT_TOKEN=<your-tcbs-jwt-token>
JWT_SECRET_KEY=<your-jwt-secret>
```

---

### 4.3 `dataops_batch_api` (Python 3.11 + PySpark 3.5)

Batch ETL pipelines pulling historical data from Binance REST and TCBS iFlash REST, validating via Pydantic, and writing ACID Delta Bronze tables to MinIO. Also serves as a gRPC server for historical queries from `dataops_backend_api`.

#### File Structure

```
dataops_batch_api/
├── grpc_server.py                           # gRPC server — HistoricalDataService (:50051)
├── batch_data_pb2.py                        # Handwritten Protobuf stubs (11 message classes)
├── batch_data_pb2_grpc.py                   # Handwritten gRPC stubs
├── __main__.py                              # CLI entrypoint
├── requirements.txt
├── Dockerfile
├── cli/
│   ├── flows/
│   │   ├── loader/
│   │   │   ├── binance_constants_loader.py  # CANONICAL: loads binance_constants.yaml
│   │   │   ├── tcbs_constants_loader.py     # CANONICAL: loads tcbs_constants.yaml
│   │   │   └── load_config.py               # Generic YAML config loader
│   │   ├── config/exchange_config.yaml      # Exchange-level scheduling config
│   │   └── scheduler.py                     # APScheduler flow scheduler
│   ├── ingestion/
│   │   ├── schema/
│   │   │   ├── canonical.py                 # Python mirror of market_data.proto enums
│   │   │   └── models.py                    # Pydantic 2 validation models
│   │   └── sources/
│   │       ├── config/
│   │       │   ├── binance_constants.yaml   # Binance REST endpoints, intervals
│   │       │   └── tcbs_constants.yaml      # TCBS iFlash endpoints, market config
│   │       └── client/
│   │           ├── binance_client/          # Binance REST client (HMAC-SHA256 hex auth)
│   │           │   ├── binance_client.py
│   │           │   ├── http.py
│   │           │   ├── spot/market_data.py  # /api/v3/klines — vol_ccy = row[7]
│   │           │   └── _retry.py
│   │           └── tcbs_client/             # TCBS iFlash REST client
│   │               ├── tcbs_client.py
│   │               ├── auth.py              # JWT token / OTP auth
│   │               ├── market_data.py       # EOD snapshot, intraday ticks
│   │               ├── securities.py        # Securities reference data
│   │               ├── intraday_analytics.py# BSA (Buy-Sell-Aggregate)
│   │               ├── margin.py            # Margin data
│   │               └── _retry.py
│   └── jobs/
│       ├── binance_1d_job.py                # Binance daily candles → Bronze
│       ├── binance_4h_job.py                # Binance 4h candles → Bronze
│       ├── tcbs_eod_job.py                  # TCBS EOD snapshot → Bronze
│       ├── tcbs_bsa_job.py                  # TCBS BSA data → Bronze
│       ├── tcbs_margin_job.py               # TCBS margin data → Bronze
│       ├── benchmark_index_job.py           # VN-Index benchmark
│       ├── _delta_writer.py                 # Lazy Delta Lake writer (try/except import)
│       └── metrics.py                       # Job Prometheus metrics
└── tests/
```

#### Delta Bronze Tables (MinIO `s3a://binance-datalake/`)

| Table | Source | Key Fields |
|---|---|---|
| `binance_candles_spot` | Binance `/api/v3/klines` | symbol, interval, ts_open_ms, open, high, low, close, volume, quote_volume |
| `binance_candles_futures` | Binance fapi klines | same schema, inst_type=FUTURES |
| `tcbs_market_snapshot` | TCBS iFlash market data | symbol, match_price, bid1/ask1, total_vol, ref_price, trading_date |
| `tcbs_market_eod` | TCBS EOD REST | symbol, trading_date, open, high, low, close, volume, foreign_buy/sell |

#### gRPC Server — `HistoricalDataService` (:50051)

```protobuf
rpc GetBinanceOHLCV(BinanceOHLCVRequest)   returns (OHLCVResponse)
rpc GetTCBSMarketEOD(TCBSMarketEODRequest) returns (OHLCVResponse)
rpc GetTCBSBSAHistory(TCBSBSARequest)      returns (BSAResponse)
rpc GetDataRange(DataRangeRequest)         returns (DataRangeResponse)
```

#### Binance REST Client — Key Design

- **Auth**: HMAC-SHA256 **hex** encoded signature in `?signature=` query param (not Base64).
- **Volume field**: `row[7]` = `quoteAssetVolume` (USDT denomination).
- **`confirm`**: hardcoded `1` (Binance klines don't have a confirm flag unlike OKX).
- **Retry**: `tenacity` with exponential backoff for 429 and 5xx responses.

#### Batch API Environment Variables

```env
MINIO_ENDPOINT=http://localhost:9010
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=binance-datalake
BINANCE_API_KEY=<your-key>
BINANCE_SECRET_KEY=<your-secret>
TCBS_JWT_TOKEN=<your-tcbs-jwt-token>
GRPC_PORT=50051
```

---

### 4.4 `dataops_processing` (Python 3.11 + PySpark 3.5)

The Spark Structured Streaming analytics engine. Consumes Kafka events and computes real-time and batch financial analytics, writing results to InfluxDB and Delta Silver/Gold.

#### File Structure

```
dataops_processing/
├── __main__.py
├── config/
│   ├── settings.py          # Pydantic settings — all configs from env + YAML
│   ├── paths.py             # PathResolver — Delta/checkpoint paths via DI
│   ├── spark_config.py      # build_spark_session() + get_kafka_options()
│   └── processing_config.yaml  # Job parameters (window durations, thresholds, symbols)
├── core/
│   ├── base_job.py          # BaseProcessingJob — Strategy pattern base class
│   ├── registry.py          # JobRegistry — @JobRegistry.register("job-name") decorator
│   └── orchestrator.py      # ProcessingOrchestrator — DI wiring + job dispatch
├── jobs/
│   ├── vwap_job.py          # ① VWAP Structured Streaming
│   ├── order_flow_job.py    # ② Order Flow / CVD Streaming
│   ├── volume_bars_job.py   # ③ Volume Bars Batch (De Prado simplified)
│   ├── indicator_matrix_job.py  # ④ Technical Indicator Matrix Batch
│   ├── market_breadth_job.py    # ⑤ Market Breadth & Foreign Flow Batch
│   └── runner.py            # Job runner CLI
├── models/
│   ├── trade.py             # TradeEvent — Kafka bytes deserialization
│   └── vwap.py              # VWAPResult model
├── log/
│   ├── logger_setup.py      # Logger manager
│   └── config/logger_config.yaml
└── tests/
    ├── test_vwap_job.py
    ├── test_volume_bars_job.py
    ├── test_order_flow_job.py
    ├── test_indicator_matrix_job.py
    ├── test_market_breadth_job.py
    └── test_core_orchestrator.py
```

#### Architecture: Strategy Pattern + Job Registry

All jobs extend `BaseProcessingJob` and register via `@JobRegistry.register("name")`. The `ProcessingOrchestrator` handles dependency injection (Settings + PathResolver) and dispatches jobs by name. All config and path parameters are injected — **zero hardcoding** in job files.

---

### 4.5 `dataops-protobuf` (Protobuf 3)

The **Canonical Data Model (CDM) — source of truth** for all field names, enums, and message shapes across Go and Python services.

#### Schema Files

| File | Content |
|---|---|
| `market-data/market_data.proto` | **CDM source of truth**: `Interval`, `Side`, `OrderStatus`, `InstrumentType`, `Exchange` enums; `CanonicalOHLCV`, `CanonicalTrade`, `CanonicalOrder`, `CanonicalPosition` messages; `MarketDataStream` gRPC service |
| `batch-data/batch_data.proto` | `HistoricalDataService` — 4 gRPC methods for historical data queries |
| `analytics/analytics.proto` | Analytics metric messages |
| `data-query/data_query.proto` | Data query request/response messages |
| `ingestion/ingestion.proto` | Batch ingestion event messages |
| `vnstock/vnstock.proto` | VN stock market-specific messages |

#### CDM Canonical Enums (from `market_data.proto`)

```protobuf
enum Interval   { INTERVAL_1M=2; INTERVAL_5M=4; INTERVAL_1H=7; INTERVAL_4H=9; INTERVAL_1D=13; ... }
enum Side       { SIDE_BUY=1; SIDE_SELL=2; SIDE_LONG=3; SIDE_SHORT=4 }
enum OrderStatus{ ORDER_STATUS_PENDING=1; ORDER_STATUS_FILLED=5; ORDER_STATUS_CANCELLED=6; ... }
enum Exchange   { EXCHANGE_BINANCE=1; EXCHANGE_TCBS_HOSE=3; EXCHANGE_TCBS_DERIV=6; ... }
```

#### CDM Mirror Locations

```
Protobuf (source of truth): dataops_protobuf/market-data/market_data.proto
Python mirror:              dataops_batch_api/cli/ingestion/schema/canonical.py
Go mirror:                  dataops-stream-producer/pkg/producer/types.go
```

> **Rule**: Any change to enums or canonical message fields must be applied to all 3 locations.

---

## 5. Data Boundary & Canonical Data Model

| Data type | Owned by | Storage | NOT done at |
|---|---|---|---|
| Real-time Binance ticks | `stream_producer` | Redis TTL 10s | `batch_api`, `processing` |
| Real-time TCBS market snapshot | `stream_producer` | Redis TTL 120s | `batch_api` |
| VWAP (real-time, 5-min windows) | `processing` | InfluxDB `market-data` + Delta Silver | `stream_producer` |
| Historical OHLCV (1d, 4h) | `batch_api` | Delta Bronze (MinIO) | `stream_producer` |
| TCBS historical EOD/BSA | `batch_api` | Delta Bronze (MinIO) | `stream_producer` |
| TCBS Account/Orders | `backend_api` (proxy) | Redis TTL 30s | Delta Lake, Kafka |
| User Auth/Session | `backend_api` | MySQL 8.0 | Kafka, Delta Lake |
| Indicator matrix (EMA, RSI, ATR, BB) | `processing` | Delta Silver | `backend_api` |
| Market breadth & foreign flow | `processing` | Delta Gold | `backend_api` |

---

## 6. Kafka Topics & Partitioning Strategy

### Topics

```
# Binance real-time
binance.trade.ticks              → trade events per symbol (high throughput)
binance.orderbook.snapshot       → depth-5 snapshots
binance.liquidation.events       → futures liquidations (fstream)
binance.funding.rates            → funding rate updates
binance.ohlcv.batch              → batch OHLCV (for compatibility)

# TCBS VN Securities
tcbs.market.snapshot             → HOSE/HNX/UPCOM market snapshots
tcbs.order.matched               → matched order events
tcbs.derivatives.ticks           → VN30F derivatives ticks

# Alert topics (Spark output)
binance.alerts.funding-anomaly
binance.alerts.liquidation
tcbs.alerts.unusual-volume

# Dead Letter Queues
binance.trade.ticks.dlq
binance.orderbook.snapshot.dlq
binance.liquidation.events.dlq
tcbs.market.snapshot.dlq
platform.dlq
```

### Producer Configuration (IBM/sarama)

```
acks          = WaitForAll (acks=all)
min.isr       = 2
replication   = 3 brokers
idempotent    = true
max.open.reqs = 1
compression   = Snappy
batch.timeout = 10ms
```

### Consumer Groups

```go
GroupSparkStreaming = "spark-streaming-group"   // dataops_processing
GroupAlerts        = "alert-group"
GroupInfluxDB      = "influxdb-group"
GroupAnalyticsAPI  = "api-query-group"          // dataops_backend_api
```

---

## 7. Analytics Computations

### 7.1 VWAP Streaming Job (`vwap_job.py`)

**Type**: Spark Structured Streaming  
**Input**: `binance.trade.ticks` (Kafka)  
**Output**: InfluxDB `measurement=vwap` + Delta Silver `vwap_1m`

```
Pipeline:
  Kafka bytes
    → TradeEvent.from_kafka_bytes()          deserialization (Avro/JSON, strips 5-byte Confluent wire header)
    → withWatermark("event_time", "30s")     late data tolerance
    → groupBy(window("5m", "1m"), symbol)    sliding 5-minute window, 1-minute slide
    → VWAP = Σ(price × size) / Σ(size)
    → Dual sink:
        InfluxDB (foreachBatch, append mode) — tag: symbol, interval="5m"
        Delta Silver (append, partitioned by symbol)

InfluxDB schema:
  measurement: "vwap"
  tags:   symbol, interval
  fields: vwap (float), volume (float), num_trades (integer)
  time:   window.end (UTC nanoseconds)
```

### 7.2 Order Flow / CVD Streaming Job (`order_flow_job.py`)

**Type**: Spark Structured Streaming  
**Input**: `binance.trade.ticks`  
**Output**: InfluxDB `measurement=order_flow`

```
Metrics per tumbling window:
  buy_volume  = Σ size where side="buy"
  sell_volume = Σ size where side="sell"
  delta       = buy_volume - sell_volume
  imbalance   = delta / (buy + sell)  ∈ [-1, 1]

CVD (Cumulative Volume Delta):
  Spark emits `delta` per closed window.
  CVD = cumulativeSum(delta) computed via Flux cumulativeSum() at query time.
  → Job restart safe: CVD is stateless in Spark.

InfluxDB schema:
  measurement: "order_flow"
  tags:   symbol, interval
  fields: buy_volume, sell_volume, delta, imbalance, num_trades
  time:   window.end (UTC nanoseconds)
```

### 7.3 Volume Bars Batch Job (`volume_bars_job.py`)

**Type**: PySpark batch  
**Input**: Delta Bronze `binance_candles_spot` (1d candles)  
**Output**: Delta Silver `volume_bars`

```
Algorithm (De Prado simplified, O(n) scan):
  threshold = configurable per symbol (e.g. BTCUSDT=500, ETHUSDT=2000)
  
  For each row (sorted by ts_open_ms):
    cumsum_volume += row.volume
    update bar_high, bar_low, bar_close
    
    if cumsum_volume >= threshold:
      emit bar (OHLCV + bar_index + open_ts + close_ts + num_candles)
      reset accumulator
  
  Emit partial bar at end if remaining volume > 0.

Output schema: symbol, bar_index, open, high, low, close, total_volume, num_candles, bar_open_ts, bar_close_ts
```

### 7.4 Technical Indicator Matrix Batch Job (`indicator_matrix_job.py`)

**Type**: PySpark batch  
**Input**: Delta Bronze (Binance 1d/1h or TCBS EOD)  
**Output**: Delta Silver `indicator_matrix` (partitioned by symbol, interval)

Indicators computed (pure Python, tested independently of Spark):

| Indicator | Formula | Params |
|---|---|---|
| **EMA** | `EMA_t = Close_t × k + EMA_{t-1} × (1-k)`, `k = 2/(n+1)` | 20, 50, 200 |
| **RSI** | `RSI = 100 - 100/(1+RS)`, Wilder's smoothed avg gain/loss | 14 |
| **ATR** | `TR = max(H-L, |H-Prev_C|, |L-Prev_C|)`, Wilder's smoothing | 14 |
| **Bollinger Bands** | `Upper = SMA + 2×StdDev`, `Lower = SMA - 2×StdDev` | period=20, std=2.0 |
| **%B** | `(Close - Lower) / (Upper - Lower)` | — |
| **Bandwidth** | `(Upper - Lower) / Middle` | — |

### 7.5 Market Breadth & Foreign Flow Batch Job (`market_breadth_job.py`)

**Type**: PySpark batch  
**Input**: Delta Bronze `tcbs_market_eod` (1 row / symbol / trading day)  
**Output**: Delta Gold `market_breadth` (idempotent: `replaceWhere trading_date`)

```
Metrics per (trading_date, group):
  advancers / decliners / unchanged
  advance_decline_ratio = advancers / max(decliners, 1)
  pct_advancing
  ceiling_count / floor_count (price hits upper/lower circuit breaker)
  total_volume / total_value
  foreign_buy_qty / foreign_sell_qty / foreign_net_qty
  foreign_net_value_est = Σ(net_foreign_qty × close)

Groups: ALL, per exchange (HOSE/HNX/UPCOM), per basket (VN30/VN30F/... from config)
```

---

## 8. API Reference

All protected endpoints require `Authorization: Bearer <JWT>` header.

### Authentication

```http
POST /api/v1/register        Register new user
POST /api/v1/login           Login — returns { access_token, refresh_token }
POST /api/v1/refresh-token   Refresh JWT
POST /api/v1/logout          Logout
```

### Binance Crypto

```http
GET  /api/v1/crypto/ticker                               All Binance tickers (Redis)
GET  /api/v1/crypto/ticker?symbol=BTCUSDT                Single ticker by query param
GET  /api/v1/crypto/:symbol/ticker                       Single ticker by path param
GET  /api/v1/crypto/:symbol/ohlcv?interval=1d&limit=100  Historical OHLCV (gRPC → Delta)
GET  /api/v1/crypto/:symbol/vwap?interval=5m             VWAP series (InfluxDB Flux)
GET  /api/v1/crypto/:symbol/signal?timeframe=1h          RSI(14)+MA(20) signal
```

### TCBS VN Stocks

```http
GET  /api/v1/tcbs/ticker                                 All TCBS tickers (Redis)
GET  /api/v1/tcbs/:symbol/ticker                         Single ticker
GET  /api/v1/tcbs/:symbol/ohlcv                          Historical OHLCV (gRPC)
GET  /api/v1/tcbs/:symbol/bsa                            Buy-Sell-Aggregate data
```

### TCBS Account Proxy (no Kafka, direct TCBS OpenAPI)

```http
GET  /api/v1/tcbs/account/balance                        Cash balance (Redis TTL 30s)
GET  /api/v1/tcbs/account/assets                         Stock holdings (Redis TTL 30s)
GET  /api/v1/tcbs/account/orders                         Open orders
GET  /api/v1/tcbs/derivative/positions                   VN30F positions (Redis TTL 10s)
POST /api/v1/tcbs/order/place                            Place stock/derivative order
```

### Portfolio & Risk

```http
GET  /api/v1/portfolio/overview         Portfolio overview
GET  /api/v1/portfolio/nav              Net Asset Value
GET  /api/v1/portfolio/allocation       Asset allocation breakdown
GET  /api/v1/portfolio/risk-report      Risk exposure report
POST /api/v1/portfolio/pre-trade-check  Pre-trade risk validation
```

---

## 9. Infrastructure & Port Allocation

| Service | Internal Port | External Port | Notes |
|---|---|---|---|
| Backend API | 8080 | 8080 | Go Gin REST |
| Batch API gRPC | 50051 | 50051 | Python gRPC server |
| MySQL | 3306 | 3306 | User/auth data |
| Redis | 6379 | 6379 | Ticker cache |
| InfluxDB | 8086 | 8086 | Time-series metrics |
| Kafka (internal) | 9092 | 9092 | |
| Kafka (external/host) | 29092 | 29092 | For local producers |
| MinIO API | 9000 | 9010 | S3-compatible |
| MinIO Console | 9001 | 9011 | Web UI |
| Grafana | 3000 | 3001 | Dashboards |
| Loki | 3100 | 3100 | Log aggregation |
| Spark Master | 7087 | 7087 | |
| Spark UI | 8095 | 8095 | |

---

## 10. Getting Started — Docker Setup

### Prerequisites

- Docker 24+ with Docker Compose v2
- `curl` and `jq` installed on host
- At least 8GB RAM (Kafka 3 brokers + Spark + InfluxDB)

### Step 1: Clone & Configure

```bash
git clone https://github.com/your-org/DataOps_Backend_Financial_Trading.git
cd DataOps_Backend_Financial_Trading

# Copy env template and fill required secrets
cp .env.example .env
```

Minimum required values in `.env`:

```env
# InfluxDB
INFLUXDB_TOKEN=PROD_SUPER_SECRET_INFLUXDB_AUTH_TOKEN
INFLUXDB_ORG=binance-vps-platform
INFLUXDB_BUCKET=market-data

# TCBS market data
TCBS_JWT_TOKEN=<your-tcbs-jwt-token>

# Binance REST (batch historical ingestion)
BINANCE_API_KEY=<your-binance-api-key>
BINANCE_SECRET_KEY=<your-binance-secret>

# MinIO
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# Backend JWT
JWT_SECRET_KEY=<random-secret-at-least-32-chars>
```

### Step 2: Start the Full Stack

```bash
# Start all infrastructure + services
docker compose -f infra_dataops_finance_trading/docker-compose.yml up -d

# Check all containers are healthy (wait ~3 minutes on first run for image pulls)
docker compose -f infra_dataops_finance_trading/docker-compose.yml ps
```

Services started:

| Service | URL |
|---|---|
| Backend API | http://localhost:8080 |
| Grafana | http://localhost:3001 (admin / see .env) |
| MinIO Console | http://localhost:9011 (minioadmin / minioadmin) |
| InfluxDB | http://localhost:8086 |
| Loki (logs) | http://localhost:3100 |

### Step 3: Verify Infrastructure Health

```bash
# InfluxDB
curl http://localhost:8086/health

# MinIO
curl http://localhost:9010/minio/health/live

# Backend API
curl http://localhost:8080/health

# Grafana
curl http://localhost:3001/api/health
```

### Step 4: Start Batch Jobs (Populate Bronze Tables)

```bash
# Binance 1d candles (last 30 days for BTCUSDT, ETHUSDT)
docker exec dataops-batch-api python -m cli.jobs.binance_1d_job

# TCBS EOD snapshot
docker exec dataops-batch-api python -m cli.jobs.tcbs_eod_job

# TCBS BSA data
docker exec dataops-batch-api python -m cli.jobs.tcbs_bsa_job
```

### Step 5: Start Processing Jobs (Populate Silver/Gold + InfluxDB)

```bash
# Start VWAP streaming job (runs continuously)
docker exec -d dataops-processing python -m dataops_processing.jobs.runner --job vwap

# Run indicator matrix batch job
docker exec dataops-processing python -m dataops_processing.jobs.runner --job indicator-matrix

# Run volume bars batch job
docker exec dataops-processing python -m dataops_processing.jobs.runner --job volume-bars

# Run market breadth batch job (requires TCBS EOD data first)
docker exec dataops-processing python -m dataops_processing.jobs.runner --job market-breadth
```

### Useful Docker Commands

```bash
# View logs for a specific service
docker logs dataops-stream-producer -f
docker logs dataops-backend-api -f
docker logs dataops-batch-api -f
docker logs dataops-processing -f

# Restart a single service
docker compose -f infra_dataops_finance_trading/docker-compose.yml restart dataops-backend-api

# Stop everything
docker compose -f infra_dataops_finance_trading/docker-compose.yml down

# Stop and remove volumes (FULL RESET — deletes all data)
docker compose -f infra_dataops_finance_trading/docker-compose.yml down -v

# Scale stream producer (multiple instances)
docker compose -f infra_dataops_finance_trading/docker-compose.yml up -d --scale dataops-stream-producer=2

# Execute shell in a container
docker exec -it dataops-batch-api bash
docker exec -it dataops-backend-api sh
```

### Kafka Management Commands

```bash
# List all topics
docker exec dataops-kafka1 kafka-topics --bootstrap-server kafka1:9092 --list

# Create topics manually (normally done automatically)
docker exec dataops-kafka1 kafka-topics --bootstrap-server kafka1:9092 \
  --create --topic binance.trade.ticks --partitions 12 --replication-factor 3

# Check topic partition offsets
docker exec dataops-kafka1 kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server kafka1:9092 --topic binance.trade.ticks --time -1

# Describe consumer group lag
docker exec dataops-kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:9092 --describe --group spark-streaming-group

# Consume messages from a topic (inspect)
docker exec dataops-kafka1 kafka-console-consumer \
  --bootstrap-server kafka1:9092 --topic binance.trade.ticks --from-beginning --max-messages 5
```

### MinIO Management Commands

```bash
# List Delta Lake objects
docker run --rm --network infra_dataops_finance_trading_default \
  --entrypoint sh minio/mc:latest \
  -c "mc alias set minio http://minio:9000 minioadmin minioadmin && mc ls minio/binance-datalake/"

# Check Delta log for a specific table
docker run --rm --network infra_dataops_finance_trading_default \
  --entrypoint sh minio/mc:latest \
  -c "mc alias set minio http://minio:9000 minioadmin minioadmin \
      && mc ls minio/binance-datalake/delta/bronze/binance_candles_1d/_delta_log/"
```

---

## 11. Running Individual Services Locally

### Stream Producer (Go)

```bash
cd dataops-stream-producer

# Build
go build ./...

# Run with env vars
KAFKA_URL=localhost:9092 \
REDIS_ADDRESS=localhost:6379 \
BINANCE_SYMBOLS=BTCUSDT,ETHUSDT \
TCBS_SYMBOLS=VCB,VHM \
TCBS_JWT_TOKEN=<token> \
go run main.go
```

### Backend API (Go)

```bash
cd dataops_backend_api

# Build
go build ./...

# Run
DATABASE_DSN="root:password@tcp(localhost:3306)/dataops?parseTime=true" \
REDIS_ADDRESS=localhost:6379 \
INFLUXDB_URL=http://localhost:8086 \
INFLUXDB_TOKEN=<token> \
BATCH_GRPC_URL=localhost:50051 \
go run main.go
```

### Batch API (Python)

```bash
cd dataops_batch_api

# Install dependencies
pip install -r requirements.txt

# Start gRPC server
python grpc_server.py

# Run a specific batch job
python -m cli.jobs.binance_1d_job
python -m cli.jobs.tcbs_eod_job
```

### Processing Jobs (Python)

```bash
cd dataops_processing

# Install dependencies
pip install -r requirements.txt

# Run VWAP streaming job
python -m dataops_processing.jobs.runner --job vwap

# Run volume bars batch job
python -m dataops_processing.jobs.runner --job volume-bars --symbol BTCUSDT

# Run indicator matrix batch job
python -m dataops_processing.jobs.runner --job indicator-matrix --symbol BTCUSDT --interval 1d
```

### Processing Environment Variables

```env
KAFKA_URL=localhost:9092
MINIO_ENDPOINT=http://localhost:9010
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
DELTA_BRONZE_BASE=s3a://binance-datalake/delta/bronze
DELTA_SILVER_BASE=s3a://binance-datalake/delta/silver
DELTA_GOLD_BASE=s3a://binance-datalake/delta/gold
SPARK_CHECKPOINT_DIR=/tmp/spark-checkpoints
INFLUXDB_URL=http://localhost:8086
INFLUXDB_TOKEN=<token>
INFLUXDB_ORG=binance-vps-platform
INFLUXDB_BUCKET=market-data
```

---

## 12. Testing

### Python Unit Tests (no Spark required)

The core algorithms in all processing jobs are pure-Python and tested without Spark:

```bash
# Run all Python tests (dataops_processing)
python -m pytest dataops_processing/tests/ -v

# Run all Python tests (dataops_batch_api)
cd dataops_batch_api
python -m pytest tests/ -v

# Run a specific test file
python -m pytest dataops_processing/tests/test_vwap_job.py -v
python -m pytest dataops_processing/tests/test_indicator_matrix_job.py -v
python -m pytest dataops_processing/tests/test_volume_bars_job.py -v
python -m pytest dataops_processing/tests/test_order_flow_job.py -v
python -m pytest dataops_processing/tests/test_market_breadth_job.py -v

# Run with coverage
python -m pytest dataops_processing/tests/ --cov=dataops_processing --cov-report=term-missing
```

### Go Build Verification

```bash
# Stream producer
cd dataops-stream-producer
go build ./...
go vet ./...

# Backend API
cd dataops_backend_api
go build ./...
go vet ./...

# Run Go tests
go test ./...
```

### Go Partitioner Unit Test

```bash
cd dataops-stream-producer
go test ./pkg/producer/ -v -run TestBinancePartitioner
```

### Stream Producer Config Test

```bash
cd dataops-stream-producer
go test ./pkg/configuration/ -v
```

---

## 13. Smoke Test (E2E Validation)

The smoke test script validates the full stack after `docker compose up`.

```bash
# Standard run
bash scripts/smoke_test.sh

# Skip user registration (if test user already exists)
bash scripts/smoke_test.sh --skip-auth

# Strict mode — pipeline data checks (Kafka offsets, InfluxDB, Delta) are FAIL instead of WARN
SMOKE_STRICT=1 bash scripts/smoke_test.sh
```

### What the Smoke Test Validates (8 sections)

| Section | Checks |
|---|---|
| **1. Infrastructure health** | InfluxDB `/health`, MinIO `/minio/health/live`, Grafana `/api/health`, Loki `/ready` |
| **2. Backend API health** | `GET /health` returns 200 |
| **3. Authentication** | Register test user (idempotent), login, receive JWT token |
| **4. Crypto endpoints** | `/crypto/ticker?symbol=BTCUSDT`, `/crypto/BTCUSDT/ticker`, `/crypto/ohlcv`, `/crypto/vwap`, `/crypto/BTCUSDT/signal` |
| **5. Trace ID propagation** | `X-Trace-ID` header echoed back in response |
| **6. Portfolio & Risk API** | `/portfolio/{overview,nav,allocation,risk-report}` routes registered, `POST /portfolio/pre-trade-check` |
| **7. Streaming chain** | Docker containers `dataops-kafka1`, `dataops-stream-producer`, `dataops-processing`, `dataops-batch-api` running; Kafka topic offsets > 0 |
| **8. Sinks validation** | InfluxDB `vwap` measurement has points in last 1h; Delta `_delta_log` exists for `silver/vwap_1m`, `bronze/binance_candles_1d`, `bronze/tcbs_market_eod` |

### Expected Output (All Passed)

```
======================================================
  DataOps E2E Smoke Test
  Backend: http://localhost:8080
======================================================

[ 1/8 ] Infrastructure health
  ✓ PASS — InfluxDB health (HTTP 200)
  ✓ PASS — MinIO health (HTTP 200)
  ✓ PASS — Grafana health (HTTP 200)
  ✓ PASS — Loki ready (HTTP 200)

...

======================================================
  SMOKE TEST RESULTS
  PASSED: 22    FAILED: 0
======================================================
  ✓ SMOKE TEST PASSED — all 22 checks OK
```

> **Note**: Sections 7–8 (streaming chain, Delta sinks) will show `! WARN` in non-strict mode if no data has been produced yet (e.g. outside TCBS market hours or before batch jobs are run). These are soft failures that don't block `exit 0`.

---

## 14. Observability & Grafana

### Distributed Tracing

Every HTTP request receives a `trace_id` propagated through the entire call chain:

```bash
# Client sends custom trace ID
curl -H "X-Trace-ID: my-debug-id-123" -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/api/v1/crypto/BTCUSDT/ticker

# Server auto-generates if not provided
# All log entries include: {"trace_id": "my-debug-id-123", "symbol": "BTCUSDT", ...}
```

### Grafana Dashboards (http://localhost:3001)

| Dashboard | Description |
|---|---|
| `DataOps Pipeline Trace` | Cross-service log search by `trace_id` or `correlation_id` |
| `binance-vps-main` | VWAP candles, funding rates, market metrics |
| `Kafka` | Broker throughput, consumer group lag, partition health |

### LogQL Queries (Loki via Grafana)

```logql
# All errors in last 15 minutes
{job=~"dataops-.*"} | json | level="ERROR"

# Trace a specific HTTP request across all services
{job=~"dataops-.*"} | json | trace_id="my-debug-id-123"

# Track a batch job run end-to-end
{job="dataops-batch-api"} | json | correlation_id="<uuid>"

# Stream producer Kafka publish errors
{job="dataops-stream-producer"} | json | level="ERROR" | action="kafka.publish"

# VWAP job InfluxDB writes
{job="dataops-processing"} | json | logger="dataops_processing.jobs.vwap_job"
```

---

## 15. Key Configuration Files

| File | Purpose |
|---|---|
| `dataops-protobuf/market-data/market_data.proto` | **CDM source of truth** — all canonical enums and message shapes |
| `dataops_batch_api/cli/ingestion/schema/canonical.py` | Python canonical mappers mirroring `.proto` |
| `dataops-stream-producer/pkg/producer/types.go` | Go canonical types mirroring `.proto` |
| `dataops_processing/config/processing_config.yaml` | VWAP window sizes, volume bar thresholds, indicator params |
| `dataops_batch_api/cli/ingestion/sources/config/binance_constants.yaml` | Binance REST endpoints, symbol list |
| `dataops_batch_api/cli/ingestion/sources/config/tcbs_constants.yaml` | TCBS iFlash endpoints, market hours |
| `infra_dataops_finance_trading/docker-compose.yml` | Full infrastructure definition |
| `kafka-connect-s3-sink.json` | Kafka Connect S3 sink config reference (Avro → Parquet on MinIO) |
| `scripts/smoke_test.sh` | E2E health + API validation |
| `docs/openapi-v1.0.0.json` | OpenAPI spec for backend REST API |
| `docs/TCBS_AUTH_GUIDE.md` | TCBS JWT token setup guide |
| `docs/TEST_PLAN_ALL_LAYERS.md` | Full test plan for all layers |
| `export_task/` | Session notes, incident reviews, design decisions (read newest first) |

---

## Development Notes

### Go Module Names

- `dataops-stream-producer/go.mod`: `module dataops-stream-producer` ✅
- `dataops_backend_api/go.mod`: `module dataops-backend-api` ✅

### Python Protobuf Stubs

The `dataops_batch_api/batch_data_pb2.py` and `batch_data_pb2_grpc.py` are handwritten stubs. To regenerate from proto when `grpc_tools` and C++ build tools are available:

```bash
cd dataops_protobuf
python -m grpc_tools.protoc -I. \
  --python_out=../dataops_batch_api \
  --grpc_python_out=../dataops_batch_api \
  batch-data/batch_data.proto
```

### PySpark in Docker

Python images for `dataops_batch_api` and `dataops_processing` require `default-jre-headless` to launch the PySpark JVM driver. This is included in the Dockerfiles.

### Dependency Compatibility (do not upgrade arbitrarily)

```
Python 3.11 + PySpark 3.5.1 + delta-spark 3.1.0 + deltalake 0.18.2 + Pydantic 2.6.4
```

This exact combination is tested and pinned. Upgrading any single package may break the delta-spark resolver or Pydantic model serialization.

---

*Built with [IBM Bob](https://www.ibm.com/products/bob) · Session tracking in `export_task/`*
