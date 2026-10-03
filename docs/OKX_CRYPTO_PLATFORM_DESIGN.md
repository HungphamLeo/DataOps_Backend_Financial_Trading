# OKX + VPS Platform — Master Design Document

> **Project:** OKX Crypto + VPS Chứng khoán VN → Kafka → Delta Lake → Real-time Analytics
> **Tech stack:** Go 1.22 · Apache Kafka 3.7 KRaft · PySpark · Delta Lake · InfluxDB 2.x · Grafana · Docker Compose
> **Reuse từ:** 11 repos Go của senior — `trading-bot-finance-service`, `trading-bot-api`, `market-maker-api`, `market-maker-background`, `trading-bot-trading-gateway`, `trading-bot-load-balancer`, `trading-bot-protobuf`

---

## Mục lục

1. [Kiến trúc tổng thể — 4 tầng](#1-kiến-trúc-tổng-thể--4-tầng)
2. [Data Sources](#2-data-sources)
3. [Tech Stack đầy đủ](#3-tech-stack-đầy-đủ)
4. [5 Repos cần xây dựng](#4-5-repos-cần-xây-dựng)
5. [T1 — Batch Ingestion Layer](#5-t1--batch-ingestion-layer)
6. [T2 — Stream Ingestion Layer (Kafka Internals)](#6-t2--stream-ingestion-layer-kafka-internals)
7. [T3 — Spark Structured Streaming](#7-t3--spark-structured-streaming)
8. [T4 — Production Standards](#8-t4--production-standards)
9. [REST API Backend](#9-rest-api-backend)
10. [Kafka Topics & Schema Design](#10-kafka-topics--schema-design)
11. [Reuse Analysis — Code từ repos senior](#11-reuse-analysis--code-từ-repos-senior)
12. [Trạng thái hiện tại — Có vs Chưa có](#12-trạng-thái-hiện-tại--có-vs-chưa-có)
13. [Lộ trình 12 tuần](#13-lộ-trình-12-tuần)
14. [Deliverables](#14-deliverables)

---

## 1. Kiến trúc tổng thể — 4 tầng

```
┌─────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                │
│                                                                      │
│   OKX REST API          OKX WebSocket API       VPS Securities       │
│   OHLCV candles         Trade ticks             Cổ phiếu VN          │
│   Funding rates         Order book L2           HOSE · HNX · UPCOM   │
│   Open interest         Liquidations            REST poll (9h-14h45) │
│   (Batch · hourly)      Mark price              CoinGecko metadata   │
│                         (Stream · real-time)    (Daily enrich)       │
└──────────┬──────────────────────┬───────────────────────────────────┘
           │ REST                 │ WebSocket + REST poll
           ▼                      ▼
┌──────────────────┐   ┌──────────────────────────────────────────────┐
│ T1: BATCH        │   │ T2: STREAM INGESTION                         │
│ INGESTION        │   │ okx-stream-producer (Go 1.22)                │
│ okx-batch-       │   │ · goroutine per trading pair                 │
│ ingestion        │   │ · sarama Kafka client, acks=all              │
│ (Python)         │   │ · Custom Partitioner: BTC/ETH dedicated      │
│ OKX REST →       │   │ · Avro + Schema Registry                     │
│ Parquet → MinIO  │   │ · Transactional producer (exactly-once)      │
│ → Delta Bronze   │   │ · DLQ handler                                │
└──────────┬───────┘   └──────────────────┬───────────────────────────┘
           │                              │
           └──────────────┬───────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   APACHE KAFKA 3.7 (KRaft, 3 brokers, RF=3)         │
│                                                                      │
│  okx.trade.ticks          okx.funding.rates    vps.market.snapshot  │
│  okx.orderbook.snapshot   okx.ohlcv.batch      vps.order.matched    │
│  okx.liquidation.events   okx.alerts.*         platform.dlq         │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
              ┌─────────────────┼──────────────────┐
              │ spark-streaming │  alert-group      │ influxdb-group
              ▼                 ▼                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│ T3: SPARK STRUCTURED STREAMING (PySpark)                            │
│                                                                      │
│  VWAP sliding window (5min/1min slide, Watermark 30s)               │
│  Volume Bars — De Prado aggregate                                    │
│  Funding Rate Z-score rolling → alert Kafka topic                   │
│  Liquidation cascade detection (60s window)                         │
│  VPS stock price aggregation (OHLCV 1D)                             │
│                                                                      │
│  → Delta Lake Silver (MinIO/S3)    → InfluxDB 2.x (real-time)       │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│ T4 (API): okx-analytics-api (Go · Gin)                              │
│                                                                      │
│  JWT Auth · Redis cache · MySQL (users)                             │
│  Query InfluxDB (real-time) + Delta Lake (historical)               │
│  REST: /crypto/* · /vn-stocks/* · /admin/*                          │
│  WebSocket: /ws/alerts                                               │
└─────────────────────────────────────────────────────────────────────┘
                                │
              ┌─────────────────┼──────────────────┐
              ▼                 ▼                   ▼
           Grafana          Kafka UI           Schema Registry
         Dashboards       Partition view        Subject list
```

---

## 2. Data Sources

| Nguồn | Data | Mode | Free? |
|-------|------|------|-------|
| **OKX REST API** | OHLCV candles, funding rates, open interest, order book snapshot | Batch (hourly) | ✅ FREE (public) |
| **OKX WebSocket API** | Trade ticks, order book L2, liquidations, mark price | Stream (real-time) | ✅ FREE (public) |
| **OKX Historical Data** | 1-min OHLCV đến 3 năm cho BTC/ETH/SOL/USDT pairs | Batch (one-time backfill) | ✅ FREE (public) |
| **CoinGecko API** | Market cap, circulating supply, dominance — enrich metadata | Daily | ✅ FREE 30 calls/min |
| **VPS Securities** | Cổ phiếu HOSE/HNX/UPCOM: giá, khối lượng, order book | Poll REST (giờ giao dịch) | Cần tài khoản VPS |

---

## 3. Tech Stack đầy đủ

| Layer | Công nghệ | Phiên bản | Mục đích |
|-------|-----------|-----------|---------|
| **Messaging** | Apache Kafka KRaft | 3.7 | Event streaming, không cần ZooKeeper |
| **Schema** | Confluent Schema Registry | 7.6 | Avro schema evolution |
| **Serialization** | Apache Avro | — | Binary format, ~3-10x nhỏ hơn JSON |
| **Connectivity** | Kafka Connect | — | S3 Sink: stream → Parquet backup |
| **Stream Processing** | PySpark Structured Streaming | 3.5 | VWAP, Volume Bars, Z-Score, anomaly |
| **Alt Stream** | Apache Flink | — | Alternative nếu cần lower latency |
| **Data Lake** | Delta Lake | — | Bronze/Silver/Gold layers, time travel |
| **Object Storage** | MinIO | latest | S3-compatible, local Delta Lake storage |
| **Time-series DB** | InfluxDB 2.x | 2.7 | Real-time metrics, Flux query language |
| **Dashboard** | Grafana | 10.0 | VWAP charts, liquidation heatmap |
| **Stream Producer** | Go 1.22 + gorilla/websocket | 1.22 | WebSocket producer, goroutine per pair |
| **REST API** | Go + Gin | 1.9 | JWT auth, analytics endpoints |
| **Cache** | Redis 7 | — | Session, market data cache (10s TTL) |
| **Database** | MySQL 8 | — | User accounts, metadata |
| **ORM** | GORM | 1.25 | Go database ORM |
| **Container** | Docker Compose | — | Full local dev stack |
| **Cloud Alt** | Azure Event Hubs | — | Kafka-compatible, zero code change |

---

## 4. Five Repos cần xây dựng

```
okx-vps-platform/
├── okx-stream-producer/   ← Go: WebSocket OKX + VPS poll → Kafka         [T2]
├── okx-batch-ingestion/   ← Python: REST → Parquet → Delta Bronze         [T1]
├── okx-spark-streaming/   ← PySpark: Kafka → Delta Silver + InfluxDB      [T3]
├── okx-analytics-api/     ← Go: REST API (JWT, Redis, InfluxDB, MySQL)    [T4]
└── docker-compose.yml     ← Full infra stack
```

**Nguyên tắc thiết kế:**
- 1 repo = 1 bounded context, deploy độc lập
- Go cho latency-sensitive: stream producer + REST API
- Python cho data-heavy: batch ingestion + Spark streaming
- Shared infrastructure qua Docker Compose

---

## 5. T1 — Batch Ingestion Layer

**Repo:** `okx-batch-ingestion/` (Python)

```
okx-batch-ingestion/
├── sources/
│   ├── okx_client.py        ← OKX REST client, retry logic, rate limit 20 req/2s
│   ├── vps_client.py        ← VPS REST lịch sử khớp lệnh
│   └── coingecko_client.py  ← Market cap, dominance metadata
├── pipeline/
│   ├── ohlcv_pipeline.py    ← OKX REST → Parquet landing zone (MinIO) → Delta Bronze
│   ├── vps_pipeline.py      ← VPS historical → Delta Bronze
│   └── backfill.py          ← ThreadPoolExecutor parallel fetch 3 năm + tqdm progress
├── delta/
│   ├── writer.py            ← Idempotent write: partitionBy("symbol", "date")
│   └── schema.py            ← Pydantic models validation TRƯỚC khi write Delta
└── requirements.txt         ← pydantic, delta-rs, minio, requests, tqdm
```

**Key implementations:**
```python
# Rate limiter: 20 req/2s cho OKX REST
# Backfill: ThreadPoolExecutor(max_workers=10) parallel fetch
# Validation: Pydantic model check trước mỗi Delta write
# Idempotent: partitionBy("symbol", "date") — re-run an toàn
```

**Trạng thái:** ❌ Chưa có code — thư mục rỗng, cần viết mới hoàn toàn (Python)

---

## 6. T2 — Stream Ingestion Layer (Kafka Internals)

**Repo:** `okx-stream-producer/` (Go 1.22)

### 6.1 Cấu trúc

```
okx-stream-producer/
├── main.go                                      ← Entry point
├── app/server.go                                ← [REUSE] market-maker-background server
├── pkg/
│   ├── configuration/configuration.go           ← [REUSE 70%] config from env
│   ├── sources/
│   │   ├── okx/websocket/producer.go            ← [MỚI] goroutine per pair
│   │   └── vps/market_data/poller.go            ← [REUSE 85%] VPS Call/Login pattern
│   ├── producer/
│   │   ├── producer.go                          ← [CẦN VIẾT] sarama transactional, acks=all
│   │   ├── partitioner.go                       ← [CẦN VIẾT] BTC/ETH dedicated partitions
│   │   ├── avro.go                              ← [CẦN VIẾT] Avro + Schema Registry
│   │   └── dlq.go                               ← [CẦN VIẾT] Dead Letter Queue handler
│   └── schemas/
│       ├── trade_event.avsc                     ← [CẦN VIẾT] Avro schema
│       ├── orderbook_snapshot.avsc              ← [CẦN VIẾT]
│       ├── liquidation_event.avsc               ← [CẦN VIẾT]
│       └── vps_market_snapshot.avsc             ← [CẦN VIẾT]
├── kafka/writer.go                              ← [REUSE 100%]
├── redis/redis.go                               ← [REUSE 100%]
└── utils/                                       ← [REUSE 100%]
```

### 6.2 Implementations cần hoàn thiện

**Custom Partitioner (BTC/ETH dedicated):**
```go
// pkg/producer/partitioner.go
func (p *OKXPartitioner) Partition(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
    symbol := string(msg.Key.(sarama.StringEncoder))
    switch {
    case strings.HasPrefix(symbol, "BTC"): return 0, nil  // Partition 0: BTC
    case strings.HasPrefix(symbol, "ETH"): return 1, nil  // Partition 1: ETH
    default: return int32(fnv32(symbol) % uint32(numPartitions-2)) + 2, nil
    }
}
```

**Transactional Producer (Exactly-once):**
```go
// pkg/producer/producer.go
config.Producer.RequiredAcks = sarama.WaitForAll   // acks=all
config.Net.MaxOpenRequests = 1                      // Required for idempotent
config.Producer.Idempotent = true
config.Producer.Transaction.ID = "okx-producer-1"  // Transactional ID
```

**DLQ Handler:**
```go
// pkg/producer/dlq.go — REUSE pattern từ trading-bot-finance-service SendStopBotToKafka
func (d *DLQProducer) SendToDLQ(originalTopic string, msg *kafka.Message, reason error) error {
    dlqMsg := kafka.Message{
        Topic: constants.TopicDLQ,
        Headers: []kafka.Header{
            {Key: "original-topic", Value: []byte(originalTopic)},
            {Key: "failure-reason", Value: []byte(reason.Error())},
            {Key: "failed-at",      Value: []byte(time.Now().UTC().String())},
        },
        Value: msg.Value,
    }
    return d.writer.WriteMessages(context.Background(), dlqMsg)
}
```

**Avro Schema — TradeEvent:**
```json
{
  "type": "record",
  "name": "TradeEvent",
  "namespace": "com.okx.platform",
  "fields": [
    {"name": "inst_id",   "type": "string"},
    {"name": "trade_id",  "type": "string"},
    {"name": "price",     "type": "double"},
    {"name": "qty",       "type": "double"},
    {"name": "side",      "type": {"type": "enum", "name": "Side", "symbols": ["buy","sell"]}},
    {"name": "ts",        "type": "long", "logicalType": "timestamp-millis"},
    {"name": "source",    "type": "string", "default": "okx-ws"}
  ]
}
```

**Trạng thái hiện tại:**

| Component | Trạng thái |
|-----------|-----------|
| goroutine per pair WebSocket | ✅ Đã có (`websocket/producer.go`) |
| VPS cron poller + session | ✅ Đã có (`vps/market_data/poller.go`) |
| sarama acks=all + transactional | ❌ Chưa có (`pkg/producer/` rỗng) |
| Custom partitioner BTC/ETH | ❌ Chưa có |
| Avro schema + Schema Registry | ❌ Chưa có (`pkg/schemas/` rỗng) |
| DLQ handler | ❌ Chưa có |
| Kafka Connect S3 Sink config | ❌ Chưa có |

---

## 7. T3 — Spark Structured Streaming

**Repo:** `okx-spark-streaming/` (Python / PySpark 3.5)

```
okx-spark-streaming/
├── jobs/
│   ├── vwap_job.py               ← VWAP sliding window 5min/1min, Watermark 30s
│   ├── volume_bars_job.py        ← Volume Bars (De Prado) — time-based → volume-based
│   ├── funding_anomaly_job.py    ← Z-score rolling → publish alert Kafka topic
│   ├── liquidation_cascade.py   ← Cluster liquidation events trong 60s window
│   └── vps_price_job.py          ← VN stocks price aggregation (daily OHLCV)
├── connectors/
│   ├── kafka_source.py           ← Spark readStream từ Kafka
│   ├── delta_sink.py             ← → Delta Lake Silver (MinIO)
│   └── influxdb_sink.py          ← → InfluxDB 2.x (real-time metrics)
├── schemas/
│   └── avro_schemas.py           ← Match với Avro schemas từ okx-stream-producer
├── utils/
│   └── watermark.py              ← Event-Time + Watermark helpers
└── submit.sh                     ← spark-submit command
```

**Core implementations:**

```python
# jobs/vwap_job.py — VWAP sliding window
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_URL) \
    .option("subscribe", "okx.trade.ticks") \
    .option("startingOffsets", "latest") \
    .load()

# Parse Avro, compute VWAP
vwap_df = df \
    .withWatermark("event_time", "30 seconds") \
    .groupBy(
        window("event_time", "5 minutes", "1 minute"),  # slide every 1min
        col("symbol")
    ) \
    .agg(
        (sum(col("price") * col("qty")) / sum(col("qty"))).alias("vwap"),
        sum("qty").alias("volume")
    )

# funding_anomaly_job.py — Z-score rolling alert
z_score = (col("funding_rate") - mean_30d) / stddev_30d
anomaly_df = funding_df.filter(abs(z_score) > 2.0)
# Publish → okx.alerts.funding-anomaly Kafka topic
```

**Trạng thái:** ❌ Chưa có — toàn bộ cần viết mới (Python/PySpark)

---

## 8. T4 — Production Standards

### 8.1 HA Kafka — KRaft Configuration

```yaml
# docker-compose.yml — đã có
KAFKA_MIN_INSYNC_REPLICAS: 2
KAFKA_DEFAULT_REPLICATION_FACTOR: 3
KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE: "false"
# Producer side:
config.Producer.RequiredAcks = sarama.WaitForAll  # acks=all
```

### 8.2 Chaos Testing Script

```bash
# chaos_test.sh — CẦN VIẾT
# Kill Kafka leader → verify 0 message loss với acks=all
docker stop kafka1
sleep 10
# Verify: consumer đọc được tất cả messages từ kafka2/kafka3
kafka-consumer-groups.sh --bootstrap-server kafka2:29092 \
  --describe --group spark-streaming-group
# Expected: LAG = 0 trên tất cả partitions
```

### 8.3 Security — SASL/SCRAM (CẦN VIẾT)

```go
// utils/kafka_auth.go
config.Net.SASL.Enable = true
config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
config.Net.SASL.User = os.Getenv("KAFKA_USERNAME")
config.Net.SASL.Password = os.Getenv("KAFKA_PASSWORD")
config.Net.TLS.Enable = true  // TLS in-transit
```

### 8.4 Consumer Lag Monitoring

```go
// REUSE pattern từ market-maker-background/CheckProviderSlowResponse
// monitoring/lag.go — CẦN VIẾT
func (m *LagMonitor) CheckConsumerLag() error {
    admin, _ := sarama.NewClusterAdmin(m.brokers, sarama.NewConfig())
    defer admin.Close()
    offsets, _ := admin.ListConsumerGroupOffsets("spark-streaming-group", nil)
    for topic, partitions := range offsets.Blocks {
        for partition, block := range partitions {
            if block.Offset > m.cfg.MaxLagThreshold {
                // Alert → Prometheus/Grafana
                log.Printf("[LAG ALERT] topic: %s partition: %d lag: %d",
                    topic, partition, block.Offset)
            }
        }
    }
    return nil
}
```

### 8.5 Dead Letter Queue Flow

```
Producer → Write message → Kafka topic
                │
                │ Write error (network, serialization)
                ▼
        DLQ Producer → platform.dlq topic
                │
                ▼
        Alert Group consumer → notify engineer
                │
                ▼
        Manual review → replay hoặc discard
```

### 8.6 Azure Event Hubs (Kafka-compatible alternative)

```bash
# Zero code change — chỉ đổi bootstrap server
KAFKA_URL=your-namespace.servicebus.windows.net:9093
# Thêm SASL config với Azure connection string
# Tất cả producer/consumer code hoạt động như cũ
```

---

## 9. REST API Backend

**Repo:** `okx-analytics-api/` (Go + Gin)

### 9.1 Cấu trúc — Layered Architecture

```
okx-analytics-api/
├── main.go                                  ← [REUSE 100%] trading-bot-api/main.go
├── app/server.go                            ← [REUSE 75%] trading-bot-api/app/server.go
├── api/rest/
│   ├── rest.go                              ← [REUSE 80%] market-maker-api/api/rest.go
│   ├── auth.go                              ← [REUSE 95%] authentication handler
│   ├── crypto.go                            ← [MỚI] VWAP, OHLCV, OrderBook, Funding
│   └── vnstock.go                           ← [MỚI] VN stocks endpoints
├── services/
│   ├── services.go                          ← [REUSE 65%] DI container
│   ├── authentication/authentication.go    ← [REUSE 90%] JWT login/refresh/logout
│   ├── analytics/crypto.go                  ← [MỚI] query InfluxDB + Delta
│   └── vnstock/vnstock.go                   ← [MỚI] VPS → InfluxDB queries
├── repository/repository.go                ← [REUSE 85%] GORM init + AutoMigrate
├── models/auth.go                           ← [REUSE 100%] User, UserSession
├── models/analytics.go                     ← [MỚI] OKXTradeTick, VPSSnapshot, OHLCV
├── middlewares/auth.go                      ← [REUSE 100%] JWT middleware + Admin
├── middlewares/security.go                  ← [REUSE 100%] CORS headers
├── configuration/configuration.go          ← [REUSE 70%] config from env
├── constants/constants.go                   ← AUTH_ROLE, EXCHANGE, timeframes
├── redis/redis.go                           ← [REUSE 100%] trading-bot-api/redis/redis.go
├── kafka/writer.go                          ← [REUSE 100%]
└── utils/                                   ← [REUSE 100%] time, env, logger
```

### 9.2 API Endpoints

```
# ─── Public (không cần JWT) ──────────────────────────────────────────
POST /api/v1/login                    Body: {username, password}
POST /api/v1/refresh-token            Body: {refresh_token}
POST /api/v1/logout                   Body: {refresh_token}
GET  /api/v1/health                   → {"status":"ok"}

# ─── Crypto / OKX (JWT required: Authorization: Bearer <token>) ──────
GET  /api/v1/crypto/vwap
     ?symbol=BTC-USDT&interval=5m&from=<unix>&to=<unix>
     → {"symbol":"BTC-USDT","interval":"5m","data":[{"ts","vwap","volume"}]}

GET  /api/v1/crypto/ohlcv
     ?symbol=BTC-USDT&interval=1h&from=<unix>&to=<unix>
     → {"data":[{"ts","open","high","low","close","volume"}]}

GET  /api/v1/crypto/orderbook
     ?symbol=BTC-USDT
     → {"bids":[[price,qty]],"asks":[[price,qty]],"ts"}

GET  /api/v1/crypto/funding
     ?symbol=BTC-USDT-SWAP&from=<unix>&to=<unix>
     → {"data":[{"ts","funding_rate","next_funding_ts"}]}

GET  /api/v1/crypto/liquidations
     ?symbol=BTC-USDT-SWAP&from=<unix>&to=<unix>
     → {"data":[{"ts","side","price","qty","value_usd"}]}

# ─── VN Stocks / VPS (JWT required) ──────────────────────────────────
GET  /api/v1/vn-stocks/market
     ?symbol=VCB
     → {"symbol","exchange","price","volume","change_pct","ts"}

GET  /api/v1/vn-stocks/ohlcv
     ?symbol=VCB&interval=1d&from=<unix>&to=<unix>
     → {"data":[{"ts","open","high","low","close","volume"}]}

GET  /api/v1/vn-stocks/orderbook
     ?symbol=VCB
     → {"bids":[[price,qty]],"asks":[[price,qty]]}

GET  /api/v1/vn-stocks/top-movers
     ?exchange=HOSE&limit=10
     → {"exchange":"HOSE","data":[{symbol,price,change_pct}]}

GET  /api/v1/vn-stocks/market-summary
     → {"VN30":{...},"VNINDEX":{...},"HNXINDEX":{...}}

# ─── Admin (SUPER_ADMIN role required) ────────────────────────────────
GET  /api/v1/admin/kafka/lag          → consumer group lag metrics
POST /api/v1/admin/ingestion/trigger  → trigger manual batch job
GET  /api/v1/admin/schema/list        → Schema Registry subjects

# ─── WebSocket ────────────────────────────────────────────────────────
WS   /api/v1/ws/alerts?token=<jwt>
     → {"type":"funding_anomaly","symbol":"BTC-USDT-SWAP","z_score":3.2,"ts"}
```

### 9.3 Request Flow — Login ví dụ

```
POST /api/v1/login {"username":"alice","password":"xxx"}
        │
        ▼
[SecureMiddleware]     ← CORS headers
        │
        ▼
[AuthHandler.Login]    ← ShouldBindJSON → validate
        │
        ▼
[AuthService.Login]    ← bcrypt.CompareHashAndPassword
        │               jwt.NewWithClaims(HS256, {id,session,exp})
        │               redis.Set(sessionID, roleInfo, 7d)
        ▼
200 {"access_token":"eyJ...","refresh_token":"eyJ..."}
```

---

## 10. Kafka Topics & Schema Design

### 10.1 Topic Architecture

| Topic | Partitions | RF | Retention | Producer |
|-------|------------|----|-----------|---------:|
| `okx.trade.ticks` | 12 | 3 | 7 days | okx-stream-producer (WS) |
| `okx.orderbook.snapshot` | 6 | 3 | 1 day | okx-stream-producer (WS) |
| `okx.liquidation.events` | 3 | 3 | 30 days | okx-stream-producer (WS) |
| `okx.funding.rates` | 3 | 3 | 90 days | okx-stream-producer (WS) |
| `okx.ohlcv.batch` | 3 | 3 | 7 days | okx-batch-ingestion |
| `okx.alerts.funding-anomaly` | 1 | 3 | 7 days | okx-spark-streaming |
| `okx.alerts.liquidation` | 1 | 3 | 7 days | okx-spark-streaming |
| `vps.market.snapshot` | 6 | 3 | 3 days | okx-stream-producer (poll) |
| `vps.order.matched` | 3 | 3 | 30 days | okx-stream-producer (poll) |
| `platform.dlq` | 3 | 3 | 30 days | All (Dead Letter Queue) |

**Kafka config (production):**
```
min.insync.replicas=2
unclean.leader.election.enable=false
acks=all              (producer)
enable.auto.commit=false (consumer)
```

### 10.2 Consumer Groups

| Group ID | Consume từ | Xử lý |
|----------|-----------|-------|
| `spark-streaming-group` | trade.ticks, orderbook, funding | VWAP, Volume Bars → Delta Silver |
| `alert-group` | funding.rates, liquidation.events | Z-score, cascade → alert topics |
| `influxdb-group` | trade.ticks, vps.market.snapshot | Real-time metrics → InfluxDB |
| `api-query-group` | alerts.* | WebSocket broadcast tới clients |

### 10.3 Delta Lake Layers

```
MinIO (S3-compatible)
└── okx-delta/
    ├── bronze/                      ← Raw data từ REST API (T1)
    │   ├── ohlcv/partitioned by symbol/date
    │   └── vps_history/partitioned by symbol/date
    └── silver/                      ← Processed data từ Spark (T3)
        ├── vwap/partitioned by symbol/date
        ├── volume_bars/
        └── funding_stats/
```

---

## 11. Reuse Analysis — Code từ repos senior

### 11.1 Bảng reuse đầy đủ

| File mới cần tạo | Reuse từ repo nào | % Reuse |
|------------------|------------------|---------|
| `okx-stream-producer/redis/redis.go` | `trading-bot-api/redis/redis.go` | **100%** |
| `okx-stream-producer/kafka/writer.go` | `trading-bot-finance-service/kafka/writer.go` | **100%** |
| `okx-stream-producer/utils/time.go` | `market-maker-background/utils/time.go` | **100%** |
| `okx-stream-producer/utils/env.go` | `market-maker-background/utils/env.go` | **100%** |
| `okx-stream-producer/utils/logger.go` | `market-maker-background/utils/logger.go` | **100%** |
| `okx-stream-producer/app/signal.go` | `trading-bot-api/app/signal.go` | **100%** |
| `okx-stream-producer/pkg/sources/vps/vps.go` | `trading-bot-finance-service/pkg/sources/vps/vps.go` | **85%** |
| `okx-stream-producer/pkg/sources/vps/types.go` | `trading-bot-finance-service/pkg/sources/vps/types.go` | **100%** |
| `okx-stream-producer/pkg/sources/vps/constants.go` | `trading-bot-finance-service/pkg/sources/vps/constants.go` | **100%** |
| `okx-stream-producer/pkg/configuration/` | `market-maker-api/configuration/` | **70%** |
| `okx-analytics-api/main.go` | `trading-bot-api/main.go` | **100%** |
| `okx-analytics-api/app/server.go` | `trading-bot-api/app/server.go` | **75%** |
| `okx-analytics-api/middlewares/auth.go` | `market-maker-api/middlewares/auth.go` | **100%** |
| `okx-analytics-api/middlewares/security.go` | `market-maker-api/middlewares/security.go` | **100%** |
| `okx-analytics-api/repository/repository.go` | `market-maker-api/repository/repository.go` | **85%** |
| `okx-analytics-api/models/auth.go` | `market-maker-api/models/auth.go` | **100%** |
| `okx-analytics-api/redis/redis.go` | `trading-bot-api/redis/redis.go` | **100%** |
| `okx-analytics-api/kafka/writer.go` | `trading-bot-finance-service/kafka/writer.go` | **100%** |
| `okx-analytics-api/services/authentication/` | `trading-bot-api/services/authentication/` | **90%** |
| `okx-analytics-api/api/rest/rest.go` | `market-maker-api/api/rest.go` | **80%** |
| `okx-analytics-api/api/rest/auth.go` | `market-maker-api/api/authentication.go` | **95%** |
| `okx-analytics-api/configuration/` | `market-maker-api/configuration/` | **70%** |
| `okx-analytics-api/constants/` | `trading-bot-api/constants/` | **80%** |
| `okx-stream-producer/app/server.go` (cron pattern) | `market-maker-background/app/server.go` | **75%** |
| `okx-stream-producer/pkg/sources/vps/market_data/poller.go` (cron) | `market-maker-background/background.go` | **70%** |
| `okx-stream-producer/pkg/producer/dlq.go` | `trading-bot-finance-service/pkg/producers/job_error.go` | **70%** |

### 11.2 Tổng kết effort

```
Tổng dòng Go cần viết:             ~10,000 dòng
├── Reuse 100% (copy thẳng):        ~2,800 dòng  (28%)
├── Reuse + adapt nhẹ (70-90%):     ~3,200 dòng  (32%)
└── Viết mới hoàn toàn:             ~4,000 dòng  (40%)

Tổng dòng Python cần viết:          ~3,000 dòng
└── Viết mới 100% (không có Python trong repos senior)

→ Tổng tiết kiệm: ~60% effort cho phần Go
```

---

## 12. Trạng thái hiện tại — Có vs Chưa có

### ✅ Đã có (có thể chạy ngay)

| Hạng mục | File/Location |
|----------|--------------|
| Kafka KRaft 3 brokers (RF=3, min.insync=2) | `docker-compose.yml` |
| Schema Registry | `docker-compose.yml` |
| MinIO, Redis, MySQL, InfluxDB, Grafana, Spark | `docker-compose.yml` |
| Kafka UI | `docker-compose.yml` |
| OKX WebSocket producer (goroutine per pair, reconnect) | `okx-stream-producer/pkg/sources/okx/websocket/producer.go` |
| VPS cron poller + session management | `okx-stream-producer/pkg/sources/vps/market_data/poller.go` |
| Kafka topics constants | `okx-stream-producer/constants/topics.go` |
| REST API skeleton: auth, CORS, JWT middleware | `okx-analytics-api/middlewares/` |
| Login / Refresh / Logout (JWT + Redis session) | `okx-analytics-api/services/authentication/` |
| Crypto API handlers (VWAP, OHLCV, OrderBook, Funding) | `okx-analytics-api/api/rest/crypto.go` |
| VN Stocks API handlers (market, ohlcv, orderbook, top-movers) | `okx-analytics-api/api/rest/vnstock.go` |
| InfluxDB Flux queries cho Crypto + VN Stocks | `okx-analytics-api/services/analytics/`, `services/vnstock/` |
| GORM + MySQL repository | `okx-analytics-api/repository/repository.go` |
| DI container (services.go) | `okx-analytics-api/services/services.go` |
| Dockerfile (multi-stage build) cả 2 repos | `*/Dockerfile` |
| .env.example cho cả 2 repos | `*/.env.example` |

### ⚠️ Thư mục tạo rồi nhưng code rỗng

| Hạng mục | Cần làm gì |
|----------|-----------|
| `okx-stream-producer/pkg/producer/` | Viết: sarama transactional producer, custom partitioner, DLQ |
| `okx-stream-producer/pkg/schemas/` | Viết: 4 file `.avsc` + Avro serializer Go |
| `okx-analytics-api/repository/influxdb/` | Extract InfluxDB queries ra repository layer |
| `okx-analytics-api/repository/delta/` | Viết: Delta Lake query qua DuckDB hoặc Spark SQL |
| `okx-analytics-api/queue/` | Copy từ `trading-bot-api/queue/` |
| `okx-analytics-api/utils/` | Copy từ `market-maker-background/utils/` |
| Grafana dashboard JSON | Tạo: VWAP chart, liquidation heatmap |

### ❌ Chưa có gì — cần viết mới hoàn toàn

| Hạng mục | Ngôn ngữ | Độ phức tạp |
|----------|---------|------------|
| `okx-batch-ingestion/` toàn bộ | Python | 🟡 Trung bình |
| `okx-spark-streaming/` toàn bộ | PySpark | 🔴 Khó |
| Kafka SASL/SCRAM + TLS security | Go config | 🟡 Trung bình |
| Kafka Connect S3 Sink configuration | YAML/JSON | 🟢 Dễ |
| Chaos test script | Bash | 🟢 Dễ |
| Consumer lag monitoring | Go | 🟡 Trung bình |
| Azure Event Hubs config | .env | 🟢 Dễ |
| WebSocket `/ws/alerts` endpoint | Go | 🟡 Trung bình |

---

## 13. Lộ trình 12 tuần

### Phase 1 — Foundation (Tuần 1-2)
```
[x] Docker Compose: Kafka KRaft 3 brokers, MinIO, Redis, MySQL, InfluxDB
[x] okx-analytics-api: Auth layer hoàn chỉnh (JWT login, middleware)
[x] okx-stream-producer: OKX WebSocket producer skeleton
[x] okx-stream-producer: VPS cron poller skeleton
[ ] Fill gaps: sarama producer với acks=all
[ ] Fill gaps: copy queue/ và utils/ còn thiếu vào analytics-api
[ ] Test: health endpoint + login + Kafka message từ OKX chạy được
```

### Phase 2 — Stream Production Grade (Tuần 3-4)
```
[ ] Custom Partitioner (BTC/ETH dedicated partitions)
[ ] Avro schemas (4 files .avsc) + Schema Registry client
[ ] Transactional producer (exactly-once semantics)
[ ] DLQ handler
[ ] Kafka Connect S3 Sink config (continuous backup → MinIO)
[ ] Test: Kafka UI thấy messages, Schema Registry thấy subjects
```

### Phase 3 — Batch Ingestion Python (Tuần 5-6)
```
[ ] okx-batch-ingestion/sources/okx_client.py (rate limit 20 req/2s, retry)
[ ] okx-batch-ingestion/sources/vps_client.py
[ ] okx-batch-ingestion/sources/coingecko_client.py
[ ] Pydantic schema validation
[ ] Delta Lake Bronze writer (partitionBy symbol/date, idempotent)
[ ] Historical backfill 3 năm BTC/ETH (ThreadPoolExecutor + tqdm)
[ ] Test: Delta table tồn tại trên MinIO, có thể query
```

### Phase 4 — Spark Streaming (Tuần 7-9)
```
[ ] VWAP sliding window job (5min/1min, Watermark 30s)
[ ] Volume Bars job (De Prado)
[ ] Funding Rate Z-score anomaly → alert Kafka topic
[ ] Liquidation cascade detection (60s window)
[ ] VPS daily OHLCV aggregation
[ ] Write → Delta Silver + InfluxDB
[ ] Test: Grafana VWAP chart hiện real-time data
```

### Phase 5 — Analytics API Complete (Tuần 10)
```
[ ] Delta Lake repository (DuckDB queries cho historical data)
[ ] WebSocket /ws/alerts endpoint
[ ] Response cache middleware (Redis, 10s TTL)
[ ] Consumer lag monitoring endpoint /admin/kafka/lag
[ ] Test: Full end-to-end: OKX live → API response
```

### Phase 6 — Production Hardening (Tuần 11-12)
```
[ ] Kafka SASL/SCRAM + TLS in-transit
[ ] Schema Registry authentication
[ ] Chaos Test: kill kafka1 → verify 0 message loss (acks=all)
[ ] Azure Event Hubs config (.env swap, zero code change)
[ ] Grafana dashboards: VWAP + liquidation heatmap
[ ] Deliverables hoàn chỉnh (xem Section 14)
```

---

## 14. Deliverables

Theo yêu cầu của project (từ ảnh):

| Deliverable | Mô tả | Trạng thái |
|-------------|-------|-----------|
| **Grafana real-time VWAP dashboard** | Live VWAP chart cho BTC/ETH/SOL + VN stocks | ❌ Chưa có |
| **Chaos test GIF/video** | Kill kafka1 → show 0 message loss | ❌ Chưa có |
| **Kafka UI partition distribution screenshot** | Show BTC→partition 0, ETH→partition 1 | ❌ Chưa có |
| **Schema Registry subject list** | List tất cả Avro schemas đã register | ❌ Chưa có |
| **OKX Historical Delta table time travel demo** | `SELECT * AS OF VERSION AS OF 5` | ❌ Chưa có |

---

## Ghi chú nhanh — Để học và code hiệu quả

```
Đọc code theo thứ tự này để hiểu pattern nhanh nhất:

1. market-maker-background/app/server.go       ← Cấu trúc server cơ bản
2. trading-bot-api/redis/redis.go               ← Interface pattern
3. trading-bot-finance-service/pkg/sources/vps/ ← HTTP client + retry
4. market-maker-api/middlewares/auth.go         ← JWT middleware
5. trading-bot-finance-service/pkg/consumers/   ← Kafka consumer + router
6. trading-bot-trading-gateway/pkg/api/rpc/     ← gRPC server pattern
7. trading-bot-load-balancer/networking/        ← Health check pattern

Công thức senior Go backend:
  Config → Connect (DB/Redis/Kafka) → DI Services → Register Routes → ListenAndServe
  └── Mọi file đều: Interface → Struct implement → Constructor return Interface
```

---

*Cập nhật lần cuối: Sau khi scaffold 36 files thực tế · OKX + VPS only · XTB đã loại bỏ*
