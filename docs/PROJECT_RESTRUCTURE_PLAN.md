# DataOps Financial Trading — Project Restructure Plan

> **Mục tiêu kép:** (1) Data Platform pipeline hoạt động end-to-end với dữ liệu thật.
> (2) Backend API serve data đó cho client.
> **Portfolio target:** Senior Data Engineer / Backend Engineer tại Fintech.
> **Core tech được rèn luyện:** Apache Kafka · Apache Spark · Delta Lake · Go · Python.
>
> **Confirmed decisions (2026-10-09):**
> - Observability: Grafana Loki (đã có trong docker-compose) — không thêm stack mới.
> - Volume Bars: time-volume bars đơn giản trước — không implement full De Prado stateful.
> - Indicators/Accounting CRUD: loại hoàn toàn khỏi plan này.

---

## Chẩn Đoán Hiện Trạng

Project hiện tại bị "split identity" — vừa là Data Platform, vừa là Web Backend, không hoàn chỉnh cái nào:

| Vấn đề | Biểu hiện trong code |
|---|---|
| `dataops_processing/` rỗng | Không có Spark job nào — core của Data Platform chưa tồn tại |
| `ComputeSignal` là placeholder | `indicator.go:227` hardcode `"NEUTRAL"` signal — không tính gì |
| Code từ project khác chưa adapt | `indicators_accounting.go` comment `REUSE: market-maker-api` |
| Kafka → Delta Silver bị đứt | Stream Producer publish lên Kafka nhưng không có consumer xử lý |
| `dataops_processing` trùng vai | VWAP accumulation đang làm trong Go Stream Producer |

**Quyết định restructure:** Ưu tiên hoàn thiện **Data Pipeline end-to-end** trước, Backend API sau. Loại bỏ code CRUD thừa không liên quan đến fintech data.

---

## Kiến Trúc Mục Tiêu (Sau Restructure)

```
Binance WSS / TCBS REST
        │
        ▼
[Module 1] dataops_stream_producer (Go)
  · WebSocket → Kafka publish (ĐANG HOẠT ĐỘNG)
  · Redis cache ticker (ĐANG HOẠT ĐỘNG)
  · KHÔNG làm aggregation (loại buyVol/sellVol accumulation ra)
        │ Kafka topics
        ▼
[Module 3] dataops_processing (PySpark) ← PHASE 1 BUILD
  · Kafka consume → VWAP window → InfluxDB + Delta Silver
  · Funding Rate Z-score → alert topic
  · Volume Bars (De Prado) → Delta Silver
        │
        ▼ Delta Silver / InfluxDB / Redis
[Module 4] dataops_backend_api (Go/Gin)
  · Query InfluxDB (real-time) + Delta (historical)
  · TCBS account proxy
  · WebSocket alert push
        │
[Module 2] dataops_batch_api (Python) — chạy song song
  · Binance REST → Delta Bronze (ĐANG HOẠT ĐỘNG)
  · TCBS EOD → Delta Bronze (ĐANG HOẠT ĐỘNG)

[Module 5] dataops_protobuf — schema contract
[Module 6] dataops_observability — centralized logging/tracing (MỚI)
```

---

## Ranh Giới Dữ Liệu Rõ Ràng (Data Ownership)

| Loại dữ liệu | Tầng sở hữu | Storage | Không được làm ở |
|---|---|---|---|
| Real-time ticks (Binance WSS) | stream_producer | Redis TTL 10s | batch_api, backend_api |
| Historical OHLCV | batch_api | Delta Bronze | stream_producer |
| VWAP aggregated | processing (Spark) | InfluxDB + Delta Silver | stream_producer (hiện tại sai) |
| Volume Bars | processing (Spark) | Delta Silver | backend_api |
| TCBS market snapshot | stream_producer | Redis TTL 120s | batch_api (real-time) |
| TCBS historical EOD | batch_api | Delta Bronze | stream_producer |
| TCBS account/order | backend_api (proxy) | Redis TTL 30s | Delta Lake (sai) |
| User auth/session | backend_api | MySQL | Kafka, Delta |
| Technical indicators (RSI/MACD) | processing (Spark) HOẶC backend_api on-demand | Redis cache | MySQL |

---

## Sub-Tasks

---

### Sub-Task 1 — Dọn Dẹp Stream Producer: Tách Aggregation Ra

**Status:** `[ ] pending`

**Intent:**
`BinanceProducer.handleMessage()` hiện tại đang tích lũy `buyVol / sellVol` trong Redis — đây là aggregation logic thuộc về Processing layer. Stream Producer chỉ nên làm 1 việc: forward raw event lên Kafka. Việc dọn sạch boundary này là bước đầu tiên để Processing layer có thể làm đúng việc của nó mà không bị double-count.

**Expected Outcomes:**
- `handleMessage()` trong `pkg/sources/binance/websocket/producer.go` không còn tích lũy `buyVol/sellVol`
- Redis ticker key `binance_{sym}_ticker` vẫn set nhưng chỉ chứa: `price, size, side, trade_id, ts` (snapshot, không accumulate)
- VWAP accumulation được remove — sẽ do Spark job trong `dataops_processing` tính sau

**Todo List:**
1. Sửa `handleMessage()` bỏ logic đọc Redis cũ + tích lũy `buyVol/sellVol`
2. Redis ticker set đơn giản: chỉ price/size/side/trade_id/ts — không cộng dồn
3. Cập nhật `pkg/producer/types.go` nếu có field `BuyVol/SellVol` trong TradeEvent struct không cần thiết
4. Thêm comment rõ ràng: "Aggregation là trách nhiệm của dataops_processing"

**Relevant Context:**
- File: `dataops_stream_producer/pkg/sources/binance/websocket/producer.go` line 160–188
- File: `dataops_stream_producer/pkg/producer/types.go`

---

### Sub-Task 2 — Build `dataops_processing`: SparkSession + VWAP Job

**Status:** `[ ] pending`

**Intent:**
Đây là core của Data Platform — tầng Processing hiện tại hoàn toàn rỗng. VWAP job là job quan trọng nhất vì nó kết nối trực tiếp Kafka → InfluxDB, tạo ra data mà `dataops_backend_api` cần để serve `/api/v1/crypto/vwap`. Không có job này, toàn bộ real-time analytics pipeline bị đứt.

**Expected Outcomes:**
- `dataops_processing/config/spark_config.py` — SparkSession với Kafka + Delta + S3A connector
- `dataops_processing/jobs/vwap_job.py` — consume `binance.trade.ticks`, tính VWAP 5-min/1-min window (Watermark 30s), ghi vào InfluxDB bucket `binance-vwap` và Delta Silver `silver/vwap_1m`
- Job chạy được với `spark-submit` hoặc `python -m dataops_processing.jobs.vwap_job`
- Prometheus metrics cho job (reuse pattern từ `cli/jobs/metrics.py`)

**Todo List:**
1. Tạo `dataops_processing/` directory structure: `config/`, `jobs/`, `tests/`
2. Viết `config/spark_config.py` — SparkSession builder với packages: `spark-sql-kafka`, `delta-spark`, S3A
3. Viết `jobs/vwap_job.py`:
   - Deserialize Avro từ Kafka (`binance.trade.ticks`) — dùng Schema Registry
   - `withWatermark("event_time", "30 seconds")`
   - `groupBy(window("event_time", "5 minutes", "1 minute"), "symbol")`
   - VWAP formula: `sum(price * quantity) / sum(quantity)`
   - Dual sink: InfluxDB (foreachBatch) + Delta Silver (append)
4. Viết `jobs/funding_anomaly_job.py` — Z-score rolling window từ `binance.funding.rates`
5. Tạo `requirements.txt` cho processing module: `pyspark==3.5.1`, `delta-spark==3.1.0`, `influxdb-client`, `confluent-kafka`
6. Thêm `Dockerfile` cho processing service
7. Viết 3 unit tests mock Spark cho VWAP formula validation

**Relevant Context:**
- Kafka topic: `binance.trade.ticks` (Avro schema: `pkg/schemas/binance_trade_event.avsc`)
- InfluxDB config: `dataops_backend_api/repository/influxdb/influxdb.go` — xem bucket name và org
- Delta path pattern: `s3://binance-datalake/delta/silver/` (mirror từ `_delta_writer.py`)
- Avro schema: `dataops_stream_producer/pkg/schemas/binance_trade_event.avsc`

---

### Sub-Task 3 — Build `dataops_processing`: Volume Bars Job (Simplified)

**Status:** `[ ] pending`

**Intent:**
Time-volume bars (OHLCV bars nhưng aggregated theo volume threshold, không theo time) là bước đơn giản hóa của De Prado Volume Bars — vẫn cho thấy hiểu biết về Financial ML trong portfolio mà không cần stateful streaming phức tạp. Cách tiếp cận: đọc từ Delta Bronze (batch) thay vì Kafka streaming, tính toán bằng PySpark batch, ghi ra Delta Silver. Có thể nâng cấp lên true streaming sau khi VWAP job (Sub-Task 2) đã proven.

**Expected Outcomes:**
- `dataops_processing/jobs/volume_bars_job.py` — đọc `bronze/binance_candles_1d`, aggregate theo volume threshold, ghi `silver/volume_bars`
- Threshold: 1 bar = N BTC volume (configurable per symbol trong YAML)
- Output columns: `symbol`, `bar_index`, `open`, `high`, `low`, `close`, `total_volume`, `num_trades`, `bar_open_ts`, `bar_close_ts`
- Chạy được như batch job: `python -m dataops_processing.jobs.volume_bars_job --symbol BTCUSDT`

**Todo List:**
1. Viết `config/processing_config.yaml` — volume thresholds per symbol (BTCUSDT: 500 BTC/bar, ETHUSDT: 5000 ETH/bar — tương đương ~1–2 bars/ngày để dễ verify)
2. Viết `jobs/volume_bars_job.py`:
   - Read Delta Bronze `bronze/binance_candles_1d` (reuse `_delta_writer._storage_options()` pattern)
   - Sort by `ts_open_ms`, cumsum `volume`, assign bar index khi cumsum vượt threshold
   - OHLC aggregation per bar
   - Write Delta Silver `silver/volume_bars` mode=overwrite, partition by symbol
3. Viết 2 unit tests: threshold logic + OHLC aggregation correctness

**Relevant Context:**
- Input: Delta Bronze path pattern từ `dataops_batch_api/cli/jobs/_delta_writer.py`
- Config pattern: reuse `dataops_batch_api/cli/flows/config/exchange_config.yaml` style
- Output path: `s3://binance-datalake/delta/silver/volume_bars`

---

### Sub-Task 4 — Fix `dataops_backend_api`: Kết Nối InfluxDB + Delta Cho Analytics

**Status:** `[ ] pending`

**Intent:**
Backend API hiện tại có handlers cho `/vwap`, `/ohlcv`, `/liquidations` nhưng service implementations query InfluxDB/Delta chưa rõ thực sự hoạt động hay không — cần audit và fix. `ComputeSignal` đã là placeholder — scope lại thành tính RSI/MA từ InfluxDB, **không** liên quan đến Indicators CRUD (đã loại khỏi scope). `indicators_accounting.go` — move handlers không liên quan data platform vào file riêng hoặc comment out.

**Expected Outcomes:**
- `services/analytics/crypto.go` — `GetVWAP()` query InfluxDB bucket `binance-vwap` (được ghi bởi Sub-Task 2) trả về data thật
- `services/analytics/crypto.go` — `GetOHLCV()` query Delta Lake qua gRPC batch server (Sub-Task 6)
- `ComputeSignal` implement RSI(14) + MA(20) từ InfluxDB price data — không còn hardcode `"NEUTRAL"`
- `indicators_accounting.go` tách thành 2 file: `indicators.go` (giữ signal compute) + `accounting.go` (comment out toàn bộ — out of scope)
- Endpoint `GET /api/v1/crypto/vwap?symbol=BTCUSDT` trả về VWAP bars thật khi Spark job đang chạy

**Todo List:**
1. Audit `repository/influxdb/influxdb.go` — xác nhận Flux query syntax và bucket/measurement names khớp với Spark job output schema (Sub-Task 2)
2. Fix `services/analytics/crypto.go` `GetVWAP()` — map đúng field names từ InfluxDB measurement `vwap_5m`
3. Fix `services/analytics/crypto.go` `GetOHLCV()` — gọi gRPC batch server (Sub-Task 6)
4. Implement `ComputeSignal`: fetch 20 candles từ InfluxDB → tính RSI(14) + MA(20) → trả `IndicatorSignal`
5. Tách `indicators_accounting.go` → `indicators.go` + `accounting.go` (accounting comment out)
6. Integration test: chạy với Docker Compose, `GET /api/v1/crypto/vwap` → non-empty JSON

**Relevant Context:**
- `dataops_backend_api/repository/influxdb/influxdb.go`
- `dataops_backend_api/services/analytics/crypto.go`
- `dataops_backend_api/api/rest/indicators_accounting.go`
- InfluxDB measurement names cần xác nhận với Sub-Task 2 (VWAP job output schema)
- Sub-Task 6 (Protobuf/gRPC) phải done trước step 3

---

### Sub-Task 5 — `dataops_observability`: Centralized Logging Layer

**Status:** `[ ] pending`

**Intent:**
Project có 4 modules viết log theo cách riêng (Go `log.Printf` + Python `logger_manager`). Khi chạy production với Docker Compose, không có cách nào trace 1 trade event từ Stream Producer → Kafka → Spark → InfluxDB → Backend API. Centralized observability là mandatory cho Senior-level portfolio và là điều kiện để debug production issues.

**Expected Outcomes:**
- Tất cả modules emit structured JSON logs với trường: `trace_id`, `service`, `level`, `message`, `ts`
- `dataops_stream_producer` (Go): dùng `zap` logger, emit JSON, ship tới Loki hoặc stdout với structured format
- `dataops_batch_api` (Python): update `log/logger_setup.py` để emit JSON với trace_id
- `dataops_processing` (Python/Spark): Spark log4j config → stdout JSON
- `dataops_backend_api` (Go): update `logging/logger.go` để emit JSON với `trace_id` từ request context
- `docker-compose.yml`: thêm Grafana Loki + Promtail để collect logs từ stdout của tất cả containers

**Todo List:**
1. Go modules: thêm `go.uber.org/zap`, wrap trong `logging/` package với `NewLogger(service string)`
2. Python modules: update `logger_manager` để output JSON format với `correlation_id` field
3. Spark: set `spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j2.json` trong spark_config
4. `docker-compose.yml`: thêm Loki + Promtail services
5. Grafana: thêm Loki datasource, tạo 1 dashboard "Pipeline Trace" với log stream per service

**Relevant Context:**
- `dataops_backend_api/logging/logger.go` — Go logger hiện tại
- `dataops_batch_api/log/logger_setup.py` — Python logger hiện tại
- `dataops_backend_api/observability/` — đã có OpenTelemetry skeleton, extend thay vì viết mới

---

### Sub-Task 6 — `dataops_protobuf`: Codegen + Batch Data gRPC

**Status:** `[ ] pending`

**Intent:**
`grpc_server.py` trong `dataops_batch_api` đang gọi `batch_data_pb2` nhưng file này chưa được generate. Backend API có `repository/batch/` nhưng cũng phụ thuộc vào proto stubs. Đây là dependency block ngăn cả 2 modules hoạt động đúng.

**Expected Outcomes:**
- `batch_data_pb2.py` + `batch_data_pb2_grpc.py` được generate vào `dataops_batch_api/`
- `batch_data_pb2.go` + `batch_data_pb2_grpc.go` được generate vào `dataops_backend_api/`
- `dataops_backend_api/repository/batch/batchdata/batch_data.go` query được gRPC server
- `grpc_server.py` khởi động thành công với proto stubs đúng

**Todo List:**
1. Chạy `protoc` codegen từ `dataops_protobuf/` cho Python output → `dataops_batch_api/`
2. Chạy `protoc` codegen cho Go output → `dataops_backend_api/`
3. Fix import paths trong `grpc_server.py` sau khi có stubs
4. Fix `repository/batch/batchdata/batch_data.go` — verify gRPC call pattern đúng
5. Viết integration test: `dataops_backend_api` gọi gRPC batch server → nhận OHLCV data

**Relevant Context:**
- Proto file: `dataops_protobuf/batch-data/batch_data.proto`
- Python server: `dataops_batch_api/grpc_server.py`
- Go client: `dataops_backend_api/repository/batch/client.go`

---

### Sub-Task 7 — Docker Compose: End-to-End Pipeline Smoke Test

**Status:** `[ ] pending`

**Intent:**
Tất cả sub-tasks trước đều isolated. Sub-task này verify toàn bộ pipeline chạy thật với dữ liệu thật: Binance WebSocket → Kafka → Spark VWAP → InfluxDB → Backend API GET /vwap trả về data. Đây là "demo-able artifact" cho portfolio — 1 curl command chứng minh toàn bộ pipeline hoạt động.

**Expected Outcomes:**
- `docker-compose up` khởi động toàn bộ stack (Kafka, MinIO, InfluxDB, Redis, MySQL, Grafana, Loki)
- Stream Producer connect Binance WSS, publish trades vào Kafka
- Spark VWAP job consume từ Kafka, ghi vào InfluxDB
- `GET /api/v1/crypto/vwap?symbol=BTCUSDT` trả về VWAP bars thật
- Grafana dashboard hiển thị: Kafka lag, Spark job health, VWAP chart, Pipeline logs

**Todo List:**
1. Thêm `dataops-processing` service vào `docker-compose.yml`
2. Thêm healthcheck cho tất cả services (Kafka, Spark, InfluxDB, gRPC batch server)
3. Viết `scripts/smoke_test.sh` — curl pipeline endpoints và verify non-empty responses
4. Tạo Grafana dashboard JSON `infra_dataops_finance_trading/grafana/pipeline_dashboard.json`
5. Update `README.md` với "Getting Started in 5 minutes" — `docker-compose up` + curl examples

**Relevant Context:**
- `infra_dataops_finance_trading/docker-compose.yml` — main compose file
- Session: `SESSION_2026_10_05_INFRA_HARDENING_AND_KAFKA_TOPICS.md` — các healthcheck đã làm

---

## Thứ Tự Ưu Tiên Implementation

```
Sub-Task 1 (Stream Producer cleanup)
    ↓ unblocks
Sub-Task 2 (VWAP Spark job) ← HIGHEST VALUE
    ↓ unblocks
Sub-Task 4 (Backend API fix)
    ↓ + Sub-Task 6 (Protobuf codegen) parallel
    ↓
Sub-Task 3 (Volume Bars) ← Financial ML showcase
    ↓
Sub-Task 5 (Observability) ← Production hardening
    ↓
Sub-Task 7 (End-to-end smoke test) ← Portfolio demo
```

---

## Quyết Định Scope: Những Gì KHÔNG Làm Trong Plan Này

| Item | Lý do dừng |
|---|---|
| Indicators CRUD (`indicators_accounting.go`) | Không liên quan đến data pipeline — để sau |
| Accounting Balance/PnL handlers | Feature thuộc trading bot project riêng |
| TCBS account order placement | Blocking on TCBS JWT management — không prioritize |
| Binance Futures WebSocket | Geo-block CDN tại VN nodes — infra constraint |
| `dataops_processing` Liquidation cascade job | After VWAP proven — same pattern |
| `dataops_processing` TCBS foreign flow | Complexity cao, value thấp cho portfolio demo |

---

## Định Nghĩa "Done" Cho Toàn Plan

Project được coi là complete khi:
1. `docker-compose up` → tất cả services healthy
2. `curl /api/v1/crypto/vwap?symbol=BTCUSDT` → trả về VWAP data thật (non-mock)
3. `curl /api/v1/crypto/ohlcv?symbol=BTCUSDT` → trả về candles từ Delta Bronze
4. Grafana dashboard hiển thị pipeline health metrics
5. Tất cả 4 modules có structured JSON logs trong Loki
6. `dataops_processing/` có ít nhất 2 Spark jobs (VWAP + Volume Bars) với unit tests
