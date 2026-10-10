# SESSION EXPORT — Stream Producer Cleanup + dataops_processing VWAP + Protobuf Codegen

**Date:** 2026-10-10 (session 4)
**Branch:** `20261010_data_processing`
**Scope:** `dataops_stream_producer/` · `dataops_processing/` (NEW) · `dataops_batch_api/`
**References:**
- `docs/PROJECT_RESTRUCTURE_PLAN.md` — nguồn plan chính
- `github/copilotinstructions/SKILL.md`
- `github/agent/realtime-streaming-engineer.agent.md`
- `github/agent/batch-lakehouse-engineer.agent.md`

---

## 1. MỤC TIÊU SESSION

Tiếp nối từ SESSION_2026_10_09, session này thực hiện 3 Sub-Tasks theo đúng thứ tự dependency:

| Sub-Task | Scope | Status |
|---|---|---|
| **ST-1** | Stream Producer cleanup — xóa `buyVol/sellVol` accumulation | ✅ DONE |
| **ST-2** | Build `dataops_processing` VWAP Spark Streaming Job | ✅ DONE |
| **ST-6** | Protobuf codegen — Python stubs cho `grpc_server.py` | ✅ DONE |

**Còn lại (session tiếp theo):**

| Sub-Task | Scope | Status |
|---|---|---|
| **ST-4** | Fix `dataops_backend_api` kết nối InfluxDB + Delta thật | ⚠️ TODO |
| **ST-3** | Build Volume Bars Job (De Prado simplified batch) | ⚠️ TODO |
| **ST-5** | Centralized Observability (Grafana Loki) | ⚠️ TODO |
| **ST-7** | Docker Compose end-to-end smoke test | ⚠️ TODO |

---

## 2. SUB-TASK 1 — STREAM PRODUCER CLEANUP

### Vấn đề

`BinanceProducer.handleMessage()` trong
`dataops_stream_producer/pkg/sources/binance/websocket/producer.go` line 160–187
đang tích lũy `buyVol/sellVol` trong Redis — vi phạm Data Boundary rule.

Stream Producer chỉ được làm 1 việc: **forward raw event lên Kafka**.
VWAP/aggregation thuộc về `dataops_processing` (Spark).

### Fix đã thực hiện

**File:** `dataops_stream_producer/pkg/sources/binance/websocket/producer.go`

**Xóa 14 dòng:**
```go
// CŨ — SAI (accumulation trong Stream Producer)
existing := p.redis.Get(tickerKey)
var buyVol, sellVol float64
if existing != "" {
    var prev map[string]interface{}
    if err := json.Unmarshal([]byte(existing), &prev); err == nil {
        if v, ok := prev["buy_vol"].(float64); ok { buyVol = v }
        if v, ok := prev["sell_vol"].(float64); ok { sellVol = v }
    }
}
if side == "buy" { buyVol += size } else { sellVol += size }
// redis.Set với buy_vol, sell_vol fields
```

**Redis ticker sau fix — snapshot thuần:**
```go
// MỚI — ĐÚNG (snapshot only)
p.redis.Set(tickerKey, map[string]interface{}{
    "symbol":   symbol,
    "price":    price,
    "size":     size,
    "side":     side,
    "trade_id": rawTrade.TradeID,
    "ts":       rawTrade.TradeTime,
}, 10*time.Second)
```

**Thêm comment ranh giới layer trên `handleMessage()`:**
```go
// RANH GIỚI LAYER: Producer chỉ làm 1 việc — forward raw event lên Kafka.
// Aggregation (VWAP, buyVol/sellVol accumulation) là trách nhiệm của dataops_processing (Spark).
// Redis ticker chỉ lưu snapshot thuần — không tích lũy state liên message.
```

**Validation:** `go vet` + `go build` package — PASS, no errors.

---

## 3. SUB-TASK 2 — BUILD `dataops_processing` VWAP SPARK JOB

### Module mới tạo từ đầu

```
dataops_processing/
├── __init__.py                  ← module docstring + ranh giới layer
├── requirements.txt             ← pyspark 3.5.1, delta-spark 3.1.0, influxdb-client 1.44.0
├── config/
│   ├── __init__.py
│   └── spark_config.py          ← build_spark_session() + get_kafka_options()
├── jobs/
│   ├── __init__.py
│   └── vwap_job.py              ← Structured Streaming pipeline đầy đủ
└── tests/
    ├── __init__.py
    └── test_vwap_job.py         ← 11 unit tests (3 class), 11/11 PASSED
```

### spark_config.py — Thiết kế

```python
# Spark packages (resolve qua spark.jars.packages tại runtime)
_SPARK_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "io.delta:delta-spark_2.12:3.1.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])
```

Hàm `build_spark_session(app_name)` — cấu hình đầy đủ:
- Delta Lake extension + catalog
- S3A / MinIO (`fs.s3a.endpoint`, `path.style.access=true`)
- Streaming: `checkpointLocation`, `stopGracefullyOnShutdown`
- Shuffle: `spark.sql.shuffle.partitions=8`

Hàm `get_kafka_options(topic)` — return dict cho `.options(**opts)`:
- `kafka.bootstrap.servers` từ env `KAFKA_URL`
- `startingOffsets=latest`, `maxOffsetsPerTrigger=10000`
- `failOnDataLoss=false` — tránh crash khi topic bị compact

### vwap_job.py — Pipeline Design

```
Kafka (binance.trade.ticks)
    → UDF parse_trade_udf(bytes) → struct(symbol, price, size, side, event_time, notional)
    → .withWatermark("event_time", "30 seconds")
    → .groupBy(window("event_time", "5 minutes", "1 minute"), symbol)
    → .agg(
          sum(notional)/sum(size) AS vwap,
          sum(size) AS total_volume,
          count(*) AS num_trades
       )
    → DUAL SINK:
        [A] InfluxDB foreachBatch (influxdb-client, line protocol)
        [B] Delta Silver s3://binance-datalake/delta/silver/vwap_1m (append, partition by symbol)
```

**InfluxDB schema (khớp hoàn toàn với `GetVWAP()` Flux query trong backend):**
```
measurement = "vwap"
tags:   symbol (e.g. "BTCUSDT"), interval ("5m")
fields: vwap (float64), volume (float64), num_trades (int64)
_time:  window.end (nanoseconds UTC)
```

**Backend query đang chờ (crypto.go:86):**
```
filter(fn: (r) => r._measurement == "vwap"
               and r.symbol == "{symbol}"
               and r.interval == "{interval}")
fields: "vwap", "volume"   ← khớp 100%
```

**Confluent Wire Format handling:**
```python
def _strip_confluent_header(raw: bytes) -> bytes:
    # [0x00][4-byte schema_id BE][payload] → strip 5 bytes nếu có magic byte
    if len(raw) > 5 and raw[0] == 0x00:
        return raw[5:]
    return raw  # plain JSON fallback
```

**Quyết định thiết kế:**

| Decision | Lý do |
|---|---|
| `outputMode("append")` | Chỉ emit window đã đóng sau watermark — no duplicate |
| `processingTime("30 seconds")` | Real-time đủ cho dashboard, không overload InfluxDB |
| `foreachBatch` cho InfluxDB | Spark không có native InfluxDB sink |
| `partition by symbol` Delta | Fast-scan `WHERE symbol=BTCUSDT` trên Silver layer |
| Watermark 30s | Binance latency < 200ms, 30s buffer an toàn |

### Test Results

```
11 passed in 1.84s ✅

TestStripConfluentHeader::test_strips_5_byte_header_when_magic_byte_present  PASSED
TestStripConfluentHeader::test_returns_raw_when_no_magic_byte                PASSED
TestStripConfluentHeader::test_returns_raw_when_too_short_for_header         PASSED
TestParseTradePayload::test_parses_valid_trade_event                         PASSED
TestParseTradePayload::test_returns_none_for_malformed_bytes                 PASSED
TestParseTradePayload::test_returns_none_for_missing_required_fields         PASSED
TestParseTradePayload::test_returns_none_for_negative_price                  PASSED
TestParseTradePayload::test_returns_none_for_zero_size                       PASSED
TestParseTradePayload::test_strips_confluent_header_before_parse             PASSED
TestWriteToInfluxDB::test_empty_dataframe_is_skipped                         PASSED
TestWriteToInfluxDB::test_writes_correct_line_protocol                       PASSED
```

---

## 4. SUB-TASK 6 — PROTOBUF CODEGEN

### Tình trạng trước

| Side | Trạng thái |
|---|---|
| Go `batchdata/batch_data.go` | ✅ Manual stub đầy đủ — không cần generate |
| Python `batch_data_pb2.py` | ❌ Chưa tồn tại → `_PROTO_AVAILABLE = False` trong grpc_server.py |
| Python `batch_data_pb2_grpc.py` | ❌ Chưa tồn tại |

### Tại sao không dùng `protoc`

`grpcio-tools==1.64.1` yêu cầu **Microsoft C++ Build Tools** để build wheel trên Windows.
Môi trường build này không có Visual C++ → pip install fail.

**Giải pháp:** Viết Python stubs thủ công với semantics tương đương protoc output.
Khi môi trường có đủ build tools, chạy lại protoc để replace:
```bash
cd dataops_protobuf
python -m grpc_tools.protoc \
    -I. \
    --python_out=../dataops_batch_api \
    --grpc_python_out=../dataops_batch_api \
    batch-data/batch_data.proto
```

### Files đã tạo

**`dataops_batch_api/batch_data_pb2.py`** — 11 message classes mirror đầy đủ proto:
- `BinanceOHLCVRequest` (+ `HasField("from")`, `HasField("to")`)
- `BinanceOHLCVBar`, `BinanceOHLCVResponse`
- `TCBSMarketEODRequest`, `TCBSMarketEODBar`, `TCBSMarketEODResponse`
- `TCBSBSARequest`, `TCBSBSAPoint`, `TCBSBSAResponse`
- `DataRangeRequest`, `DataRangeResponse`

**`dataops_batch_api/batch_data_pb2_grpc.py`** — 3 components:
- `HistoricalDataServiceServicer` — base class, 4 methods raise `NOT_IMPLEMENTED`
- `add_HistoricalDataServiceServicer_to_server()` — registration function
- `HistoricalDataServiceStub` — client stub cho integration tests

### Validation

```python
# grpc_server.py import block simulation
import batch_data_pb2
import batch_data_pb2_grpc
_PROTO_AVAILABLE = True  ✅

# All assertions PASSED:
req.HasField('from') == False  ✅
svc = HistoricalDataServiceServicer() — 4 methods present  ✅
callable(add_HistoricalDataServiceServicer_to_server)  ✅
```

```
go vet ./repository/batch/...       ✅  (no output)
go build dataops-backend-api/repository/batch/...  ✅
```

---

## 5. DATA BOUNDARY SAU SESSION NÀY

| Loại dữ liệu | Layer sở hữu | Storage | KHÔNG làm ở |
|---|---|---|---|
| Real-time ticks (Binance WSS) | stream_producer | Redis TTL 10s (snapshot only) | batch_api, processing |
| Historical OHLCV | batch_api | Delta Bronze | stream_producer |
| VWAP aggregated | **processing (Spark)** ← MỚI | InfluxDB `market-data` + Delta Silver | stream_producer (đã fix) |
| TCBS market snapshot | stream_producer | Redis TTL 120s | batch_api (real-time) |
| TCBS historical EOD | batch_api | Delta Bronze | stream_producer |
| TCBS account/order | backend_api (proxy) | Redis TTL 30s | Delta Lake |
| User auth/session | backend_api | MySQL | Kafka, Delta |

---

## 6. CÔNG VIỆC CÒN LẠI (session tiếp theo)

### Priority 1 — Sub-Task 4: Fix `dataops_backend_api` (UNBLOCKED bởi ST-6)

**File chính:**
- `dataops_backend_api/services/analytics/crypto.go`
- `dataops_backend_api/repository/influxdb/influxdb.go`
- `dataops_backend_api/api/rest/indicators_accounting.go`

**Việc cần làm:**
1. Audit Flux query trong `GetVWAP()` — measurement `"vwap"`, tags `symbol`+`interval`, fields `vwap`+`volume`
   → Đã khớp với VWAP job output schema ✅ (không cần sửa)
2. Fix `GetOHLCV()` — gọi Batch gRPC client (đã unblock bởi ST-6)
3. Implement `ComputeSignal()` — RSI(14) + MA(20) từ InfluxDB price data (thay hardcode `"NEUTRAL"`)
4. Tách `indicators_accounting.go` → `indicators.go` + `accounting.go` (accounting: comment out)

### Priority 2 — Sub-Task 3: Volume Bars Job

**File cần tạo:** `dataops_processing/jobs/volume_bars_job.py`
- Read Delta Bronze `bronze/binance_candles_1d`
- Sort by `ts_open_ms`, cumsum `volume`, assign bar index khi vượt threshold
- OHLC aggregation per bar → Delta Silver `silver/volume_bars`

**Config cần tạo:** `dataops_processing/config/processing_config.yaml`
```yaml
volume_bars:
  BTCUSDT: 500   # BTC/bar (~1-2 bars/day để dễ verify)
  ETHUSDT: 5000  # ETH/bar
```

### Priority 3 — Sub-Task 5: Centralized Observability (Grafana Loki)

**Việc cần làm:**
1. Update `dataops_batch_api/log/logger_setup.py` — emit JSON với `correlation_id` field
2. Update `dataops_backend_api/logging/logger.go` — emit JSON với `trace_id` từ request context
3. `docker-compose.yml` — thêm Loki + Promtail services
4. Grafana — thêm Loki datasource + dashboard "Pipeline Trace"

### Priority 4 — Sub-Task 7: Docker Compose End-to-End Smoke Test

**Việc cần làm:**
1. Thêm `dataops-processing` service vào `infra_dataops_finance_trading/docker-compose.yml`
2. Viết `scripts/smoke_test.sh` — curl endpoints, verify non-empty responses
3. Update `README.md` — "Getting Started in 5 minutes"

---

## 7. LESSON LEARNED SESSION NÀY

### [L21] Stream Producer không được tích lũy state liên message trong Redis

Pattern sai: đọc Redis cũ → cộng dồn → ghi lại.
Pattern đúng: ghi snapshot thuần từ event hiện tại.

Bất kỳ aggregation nào (VWAP, volume bar, RSI) đều phải nằm trong **Processing layer** (Spark) với proper watermark, state management và windowing.
Stream Producer chỉ forward — không compute.

### [L22] Spark foreachBatch là pattern chuẩn cho InfluxDB sink

Spark không có native InfluxDB connector. Pattern chuẩn:
```python
.writeStream
.outputMode("append")  # chỉ emit closed windows
.foreachBatch(_write_to_influxdb)
.option("checkpointLocation", ...)
.start()
```
Hàm `_write_to_influxdb(df_pandas, batch_id)` nhận pandas DataFrame và ghi bằng `influxdb-client`.
Chú ý: `df_pandas.empty` check trước để tránh empty write.

### [L23] InfluxDB line protocol thủ công nhanh hơn `Point` builder trong hot path

```python
# Nhanh hơn influxdb_client.Point().tag().field().time()
point = f"{measurement},symbol={symbol},interval={interval} vwap={vwap},volume={volume} {ts_ns}"
```
Trong `foreachBatch` với hàng trăm symbols, build string trực tiếp tránh object allocation overhead của `Point` builder.

### [L24] outputMode("append") cho time-window aggregation với watermark

Spark Structured Streaming có 3 output modes:
- `complete` — emit toàn bộ aggregated result mỗi batch (không dùng với InfluxDB — duplicate)
- `update` — emit rows changed (window chưa đóng cũng emit — partial VWAP)
- `append` — chỉ emit rows sau khi watermark đã pass window end (đúng cho VWAP)

**Rule:** Window aggregation + InfluxDB sink → luôn dùng `outputMode("append")`.

### [L25] Protobuf Python stubs thủ công — khi không thể chạy protoc

Khi `grpcio-tools` không install được (Windows thiếu C++ Build Tools):
1. Viết message classes với `__init__(self, field=default)` kwargs
2. `HasField(field_name)` cho optional Timestamp fields
3. Servicer base: methods raise `NotImplementedError` + set `StatusCode.UNIMPLEMENTED`
4. `add_*Servicer_to_server()` dùng `grpc.unary_unary_rpc_method_handler` + `grpc.ServiceRpcHandler`
5. Luôn ghi comment rõ "handwritten stub" + codegen command để replace sau

### [L26] `grpc.ServiceRpcHandlers` (plural) không tồn tại — dùng `grpc.ServiceRpcHandler` (singular)

Bug phát hiện khi chạy: `AttributeError: module 'grpc' has no attribute 'ServiceRpcHandlers'`.
Tên đúng: `grpc.ServiceRpcHandler` (không có 's' cuối).

---

## 8. ENV VARS MỚI CẦN THÊM VÀO `.env`

```bash
# dataops_processing
KAFKA_URL=localhost:9092
MINIO_ENDPOINT=http://minio:9010
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
DELTA_SILVER_BASE=s3://binance-datalake/delta/silver
SPARK_CHECKPOINT_DIR=/tmp/spark-checkpoints/vwap_job
INFLUXDB_URL=http://localhost:8086
INFLUXDB_TOKEN=<your-token>
INFLUXDB_ORG=binance-vps-platform
INFLUXDB_BUCKET=market-data
```

---

## 9. FILE CHANGES SUMMARY

| File | Action | Ghi chú |
|---|---|---|
| `dataops_stream_producer/pkg/sources/binance/websocket/producer.go` | Modified | Xóa buyVol/sellVol accumulation |
| `dataops_processing/__init__.py` | Created | Module docstring |
| `dataops_processing/requirements.txt` | Created | pyspark 3.5.1, delta-spark 3.1.0, influxdb-client 1.44.0 |
| `dataops_processing/config/__init__.py` | Created | Empty |
| `dataops_processing/config/spark_config.py` | Created | SparkSession builder + Kafka options |
| `dataops_processing/jobs/__init__.py` | Created | Empty |
| `dataops_processing/jobs/vwap_job.py` | Created | VWAP Structured Streaming job |
| `dataops_processing/tests/__init__.py` | Created | Empty |
| `dataops_processing/tests/test_vwap_job.py` | Created | 11 unit tests, 11/11 PASSED |
| `dataops_batch_api/batch_data_pb2.py` | Created | Python proto stubs (messages) |
| `dataops_batch_api/batch_data_pb2_grpc.py` | Created | Python proto stubs (gRPC servicer) |
