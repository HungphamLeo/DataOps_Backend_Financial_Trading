


---
name: dataops-financial-trading
description: Use when working on the DataOps Backend Financial Trading project — covers Go/Python services, Kafka streaming, Delta Lake batch ingestion, Binance/TCBS data pipelines, analytics API, protobuf schemas, and infra/observability. Activate for any task involving dataops-stream-producer, dataops_backend_api, dataops_batch_api, dataops-protobuf, or related docker-compose infrastructure.
---

# HƯỚNG DẪN KỸ NĂNG DÀNH CHO DATAOPS BACKEND FINANCIAL TRADING (PROJECT SKILL MANIFEST)

**Core Directive:** Bạn là AI Assistant làm việc trong dự án DataOps Backend Financial Trading, một nền tảng xử lý dữ liệu thị trường tài chính theo kiến trúc event-driven & lakehouse. Mọi quyết định về thiết kế, code, test, pipeline, và triển khai phải dựa trên repo hiện tại, tài liệu architecture, và tri thức chuyên sâu từ ``allskill`` & ``export_task``. Không được suy đoán theo kinh nghiệm chung khi trong repo đã có nguồn thực tế rõ ràng.

## 0. Quy tắc hành vi bắt buộc (RULES — đọc trước tất cả)

1. **Làm đúng task được yêu cầu, không tự ý thêm task mới.** Khi xong task nào thì thông báo xong task đó. Không âm thầm làm thêm.
2. **Khi được yêu cầu export session vào `export_task/`:** Viết file `.md` trực tiếp — KHÔNG đọc lại các file trong `export_task/` trước khi viết.
3. **Khi được yêu cầu cập nhật kiến thức (đọc SKILL.md):** Ở bước đọc `export_task/`, chỉ đọc **file session gần nhất** (sort by name desc, lấy 1 file). Không đọc toàn bộ.
4. **Khi được yêu cầu đọc `allskill`:** Chỉ đọc skill liên quan đến dự án — xác định tên skill liên quan bằng cách đọc tên file trong thư mục `allskill/` và đối chiếu với context từ `export_task` gần nhất. Không đọc tất cả.

---

## 1. Phân cấp tri thức và mức ưu tiên

1. **[Ưu tiên cao nhất] `./export_task/` (nếu có trong repo):**
   - Chứa bài học, incident review, fix pattern, quyết định thiết kế, và cập nhật tiến độ mới nhất.
   - **Khi cần đọc:** chỉ đọc file có tên mới nhất (sort by filename desc).
   - Nếu có xung đột với tài liệu khác hoặc `allskill`, ưu tiên `./export_task/`.

2. **[Ưu tiên tiếp theo] Source code thực tế trong repo hiện tại:**
   - Cấu trúc module, contract, schema, producer/consumer config thực tế đang chạy.

3. **[Ưu tiên tiếp theo] Tri thức hệ thống `allskill` & Tài liệu kiến trúc repo:**
   - `C:\Users\Admin\Downloads\Project\Github\ai_workspace_management\.ai_workspace\allskill/`
   - Chỉ đọc skill liên quan đến task hiện tại — không đọc tất cả.
   - `README.md`, `docker-compose.yml`, `kafka-connect-s3-sink.json`, và các file cấu hình.

4. **[Ưu tiên cuối cùng] Kiến thức chung / mặc định:**
   - Chỉ dùng khi repo không có thông tin rõ ràng. Luôn cần xác minh bằng code thực tế trước khi khẳng định.

---

## 2. Scope của dự án và stack thực tế

Dự án này là nền tảng data platform xử lý dữ liệu thị trường tiền mã hóa (Binance Crypto) và chứng khoán Việt Nam (TCBS):

- `dataops-stream-producer/`: Go service. Module name: `dataops-stream-producer` (go.mod). WebSocket Binance (Trade Ticks, OrderBook depth5) + cron polling TCBS (Market Snapshot). Publish dữ liệu lên Kafka qua IBM/sarama SyncProducer với Avro/JSON + DLQ. Hai Kafka client library song song: `IBM/sarama` (pkg/producer — production path) và `segmentio/kafka-go` (pkg/messaging — internal/legacy layer).
- `dataops_backend_api/`: Go REST API. Module name: `dataops-backend-api` (go.mod). JWT auth, Redis cache, InfluxDB v2, MySQL, Kafka consumer, Prometheus metrics, repository pattern phục vụ dữ liệu analytics Crypto & TCBS.
- `dataops_batch_api/`: Python pipeline (Spark, Delta Lake, Polars, Prefect orchestration). Ingest batch REST API Binance & TCBS về Delta Lake (Bronze → Silver → Gold).
- `dataops-protobuf/`: Định nghĩa Protobuf schema cho analytics, batch-data, data-query, ingestion, market-data và vnstock.
- `shared/platform/`: Go shared packages (`di`, `observability`).
- `docker-compose.yml` & `infra/`: Kafka cluster, Redis, MySQL, InfluxDB, MinIO (S3 bucket: `binance-datalake`), Spark, Prometheus, Grafana.

> **Go module names (cập nhật SESSION_2026_10_03):**
> - `dataops-stream-producer/go.mod`: `module dataops-stream-producer` ✅
> - `dataops_backend_api/go.mod`: `module dataops-backend-api` ✅ (đã đổi)
> - Import paths trong `dataops_backend_api/` dùng `dataops-backend-api/...`

---

## 3. Environment Configuration (cập nhật SESSION_2026_10_03)

**Single source of truth:** `.env.example` ở root repo — duy nhất, không còn file rải rác ở service con.

| Service | Env keys chính |
|---|---|
| `dataops_backend_api` | `MYSQL_DNS`, `REDIS_ADDRESS`, `REDIS_PASSWORD`, `REDIS_DB_0`, `INFLUXDB_URL/TOKEN/ORG/BUCKET`, `WEB_URL`, `MODE`, `JWT_SECRET`, `JWT_REFRESH_SECRET`, `JWT_LIFESPAN_HOURS`, `JWT_REFRESH_LIFESPAN_HOURS`, `TCBS_BASE_URL`, `TCBS_API_KEY`, `TCBS_JWT_TOKEN` |
| `dataops_stream_producer` | `KAFKA_URL`, `REDIS_ADDRESS`, `REDIS_PASSWORD`, `REDIS_DB_0`, `BINANCE_WS_URL`, `BINANCE_SYMBOLS`, `TCBS_BASE_URL`, `TCBS_API_KEY`, `TCBS_JWT_TOKEN`, `TCBS_SYMBOLS`, `TCBS_INDEXES`, `TCBS_BSA_INVESTOR_TYPE`, `TCBS_INTRADAY_MAX_PAGES`, `SCHEMA_REGISTRY_URL`, `MODE` |
| `dataops_batch_api` | `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`, `AWS_REGION`, `BINANCE_API_KEY`, `BINANCE_SECRET_KEY`, `TCBS_BASE_URL`, `TCBS_API_KEY`, `TCBS_JWT_TOKEN`, `TCBS_TIMEOUT`, `TCBS_RETRY` |

**Lưu ý quan trọng:**
- `dataops_backend_api` đọc `MYSQL_DNS` (full DSN), KHÔNG đọc `DB_HOST/PORT/USER/PASSWORD` riêng lẻ.
- `INFLUXDB_ORG` default: `binance-vps-platform` (không phải `okx-vps` hay `financial-trading`).
- Các service `.env.example` con chỉ là file reference, trỏ về `../.env.example`.

---

## 4. Kiến trúc chuyên sâu Apache Kafka trong Financial Trading

### 4.1 Yêu cầu phi chức năng (NFR)
1. **Zero Data Loss:** `acks=all` (`WaitForAll`), `min.insync.replicas=2`, `replication.factor=3`.
2. **Strict Ordering:** Key định danh là `symbol`. `RequiresConsistency() = true`.
3. **High Throughput & Low Latency:** Compression Snappy, batching 10ms.
4. **Idempotency & EOS:** `Idempotent = true` + `MaxOpenRequests = 1`.

### 4.2 Kafka Producer Internals

| Layer | Package | Library | Dùng cho |
|---|---|---|---|
| Production path | `pkg/producer` | `IBM/sarama` | WebSocket Binance + TCBS poller → Kafka |
| Internal/legacy | `pkg/messaging` | `segmentio/kafka-go` | DLQProducer standalone |

**`pkg/producer/producer.go` — kafkaProducer (sarama):**
- `RequiredAcks = sarama.WaitForAll`, `Idempotent = true`, `Compression = CompressionSnappy`
- `Retry.Max = 5`, `Retry.Backoff = 100ms`, `Partitioner = NewBinancePartitioner`
- Schema Registry fallback: nếu URL rỗng → gửi JSON chuẩn

### 4.3 BinancePartitioner
- Partition 0: `BTC*` (~40% volume)
- Partition 1: `ETH*` (~20% volume)
- Partition 2..N-1: altcoins + TCBS stocks — FNV32a hash
- `numPartitions <= 2` → fallback partition 0

### 4.4 Kafka Topics (`constants/topics.go`)

```
binance.trade.ticks          binance.orderbook.snapshot
binance.liquidation.events   binance.funding.rates
binance.ohlcv.batch
tcbs.market.snapshot         tcbs.order.matched
binance.alerts.funding-anomaly  binance.alerts.liquidation
tcbs.alerts.unusual-volume
# DLQ
binance.trade.ticks.dlq      binance.orderbook.snapshot.dlq
binance.liquidation.events.dlq  tcbs.market.snapshot.dlq
platform.dlq
```

### 4.5 Consumer Groups

```go
GroupSparkStreaming = "spark-streaming-group"
GroupAlerts        = "alert-group"
GroupInfluxDB      = "influxdb-group"
GroupAnalyticsAPI  = "api-query-group"
```

Consumer files trong `pkg/messaging/` có `//go:build ignore` — reference code, không compile.

---

## 5. Quy tắc routing theo chuyên môn

- **Backend / REST API / Auth / Query:** `dataops_backend_api/`
- **Streaming / Producer / Kafka / WS:** `dataops-stream-producer/`
- **Batch pipeline / Delta Lake / Spark / Prefect:** `dataops_batch_api/`
- **Schema / Protobuf:** `dataops-protobuf/` & `pkg/schemas/`
- **Shared packages / DI / Observability:** `shared/platform/`
- **Infrastructure / Docker:** `docker-compose.yml`, `infra/`, `Dockerfile`
- **Monitoring:** `grafana/`, logging config

> **Data Flow:** `Binance WebSocket / TCBS REST` → `dataops-stream-producer` → `Kafka` → `Kafka Connect / Spark` → `MinIO Delta Lake (Bronze/Silver/Gold)` → `dataops_backend_api` → `Client`

---

## 6. Hướng dẫn chi tiết theo module

### 6.1 dataops_backend_api (Go REST API)
- Go module: `dataops-backend-api`
- REST endpoints: Crypto (`/api/v1/crypto/...`), VNStock/TCBS (`/api/v1/vnstock/...`, `/api/v1/accounting/...`)
- Models: `BinanceTradeTick`, `BinanceOrderBook5`, `BinanceFundingRate` (`models/analytics.go`)
- Database: MySQL (`binance_analytics`), InfluxDB org (`binance-vps-platform`), Redis

### 6.2 dataops-stream-producer (Go Streaming Producer)

Config env vars (`pkg/configuration/configuration.go`):
```
KAFKA_URL               default: localhost:9092
REDIS_ADDRESS           default: localhost:6379
BINANCE_WS_URL          default: wss://stream.binance.com:9443/ws
BINANCE_SYMBOLS         default: BTCUSDT,ETHUSDT,SOLUSDT
TCBS_BASE_URL           default: https://openapi.tcbs.com.vn
TCBS_SYMBOLS            default: VCB,VHM,TCB,VIC,HPG,MSN,VNM,MBB,CTG,BID
TCBS_INDEXES            default: HOSE,VN30,HNX
TCBS_BSA_INVESTOR_TYPE  default: all
TCBS_INTRADAY_MAX_PAGES default: 5
SCHEMA_REGISTRY_URL     default: http://localhost:8081
MODE                    default: dev
```

**Binance WebSocket (`pkg/sources/binance/websocket/producer.go`):**
- Combined stream: `wss://stream.binance.com:9443/ws/stream?streams={sym}@trade/{sym}@depth5`
- Routing: `@trade` → `PublishTrade`, `@depth` → `PublishOrderBook`
- Redis cache key: `binance_{symbol_lower}_{stream}` (TTL 60s)
- Auto-reconnect: 5s delay

**TCBS Poller (thay thế VPS Poller):**
- Poll BSA + Intraday ticks cho `TCBS_SYMBOLS`
- Poll market snapshot (tickerCommons) cho `TCBS_INDEXES`
- Redis cache key: `tcbs_{symbol}_ticker` (TTL 120s)

### 6.3 dataops_batch_api (Python Data Lakehouse Pipeline)
- REST client: `BinanceRestClient` (`cli/ingestion/sources/binance_client.py`)
  - Auth: HMAC-SHA256 **hex** signature trong `&signature=`
  - `vol_ccy = row[7]` (quoteAssetVolume), `confirm = 1` hardcode
- REST client: `TCBSRestClient` (`cli/ingestion/sources/tcbs_client.py`)
  - Auth: JWT Bearer từ `TCBS_JWT_TOKEN`
- Delta Lake bucket: `binance-datalake`
  - Tables: `binance_candles_spot`, `binance_candles_futures`, `tcbs_market`, `tcbs_bsa`, `tcbs_intraday`
- Logging: `LoggerManager` singleton (`log/logger_setup.py`) — chỉ dùng `logging.getLogger(__name__)` trong các module

---

## 7. Sự khác biệt OKX vs Binance (Lesson Learned)

| Khía cạnh | OKX | Binance |
|---|---|---|
| Auth | OK-ACCESS-KEY (4 headers) | X-MBX-APIKEY + `?signature=` |
| Signature | Base64(HMAC-SHA256) | **Hex**(HMAC-SHA256) |
| Candle endpoint | `/api/v5/market/history-candles` | `/api/v3/klines` |
| vol_ccy | `row[6]` | `row[7]` |
| Symbol | `BTC-USDT` | `BTCUSDT` |
| Pagination | `before` param | `endTime` param |

---

## 8. TODO còn lại

**[HIGH]**
- Implement `dataops-stream-producer/pkg/sources/binance/rest/` — REST client (thư mục rỗng)
- Test kết nối Binance WebSocket thực tế, verify `@trade` và `@depth5`
- Handle Binance ping/pong (300s keepalive)

**[LOW]**
- Rename Avro schema files: `pkg/schemas/okx_*.avsc` → `binance_*.avsc`
- Update Grafana dashboard `grafana/dashboards/okx-vps-main.json`

---

## 9. Quy trình làm việc chuẩn khi nhận task

1. **Phân loại domain:** Xác định module liên quan.
2. **Kiểm tra `./export_task/`:** Chỉ đọc file mới nhất.
3. **Đọc code liên quan:** Không đoán field names, topic names, API endpoints.
4. **Thực thi đúng task được yêu cầu.** Không thêm task ngoài yêu cầu.
5. **Verify:** `go vet ./...` (Go) hoặc `python -m py_compile` (Python).
6. **Thông báo xong task.**

---

## 10. Quy tắc ưu tiên giải quyết xung đột

`./export_task/` (file mới nhất) → Source code hiện tại → Repo docs / config → `allskill` (chỉ skill liên quan) → Kiến thức chung.
