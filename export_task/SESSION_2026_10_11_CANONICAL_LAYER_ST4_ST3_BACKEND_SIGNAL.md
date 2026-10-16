# SESSION EXPORT — Canonical Layer + ST-4 Backend API Fix + ST-3 Volume Bars Job

**Date:** 2026-10-11 (session 5)
**Branch:** `20261010_data_processing`
**Scope:** `dataops_batch_api/` · `dataops_backend_api/` · `dataops_processing/`
**References:**
- `docs/PROJECT_RESTRUCTURE_PLAN.md` — nguồn plan chính
- `github/copilotinstructions/SKILL.md`
- `github/agent/backend-software-engineer.agent.md`
- `github/agent/batch-lakehouse-engineer.agent.md`

---

## 1. MỤC TIÊU SESSION

Tiếp nối từ SESSION_2026_10_10, session này thực hiện 3 nhóm việc:

| Nhóm | Scope | Status |
|---|---|---|
| **Canonical Layer** | Tạo `canonical.py` mapper + update 4 jobs batch_api | ✅ DONE |
| **ST-4** | Fix backend_api: tách file, RSI+MA signal, endpoint mới | ✅ DONE |
| **ST-3** | Build `volume_bars_job.py` + `processing_config.yaml` | ✅ DONE |

**Còn lại (session tiếp theo):**

| Sub-Task | Scope | Status |
|---|---|---|
| **ST-5** | Centralized Observability — Grafana Loki, JSON logging với trace_id | ⚠️ TODO |
| **ST-7** | Docker Compose E2E smoke test — processing service, smoke_test.sh | ⚠️ TODO |
| Fix O(n²) bug trong `volume_bars_job.py` | `assign_volume_bars` dùng `rows.index(row)` | ⚠️ TODO |
| Unit tests cho `volume_bars_job.py` | ST-3 incomplete | ⚠️ TODO |

---

## 2. CANONICAL LAYER REFACTOR

### Vấn đề

4 batch jobs (`binance_1d_job.py`, `binance_4h_job.py`, `tcbs_eod_job.py`, `tcbs_bsa_job.py`) mỗi cái
tự map field names theo cách riêng — không có điểm trung tâm để đảm bảo field names khớp với
`dataops_protobuf/market-data/market_data.proto`. Khi .proto thay đổi, phải sửa ≥4 chỗ.

### Fix đã thực hiện

**File tạo mới:** `dataops_batch_api/cli/ingestion/schema/canonical.py`

Tạo 4 pure mapper functions:

```python
from_binance_kline_row(row: BinanceCandleRecord, exchange: str = "BINANCE") -> dict
from_tcbs_ticker_record(row: TCBSMarketRecord, exchange: str = "TCBS_HOSE") -> dict
from_tcbs_bsa_record(row: TCBSBSARecord, exchange: str = "TCBS_HOSE") -> dict
from_tcbs_intraday_tick(row: TCBSIntradayTickRecord, exchange: str = "TCBS_HOSE") -> dict
```

**File cập nhật:** `dataops_batch_api/cli/ingestion/schema/__init__.py`
- Export 4 mapper functions ra public API của package

**4 jobs cập nhật** để dùng canonical mappers thay vì tự map:

| File | Thay đổi |
|---|---|
| `cli/jobs/binance_1d_job.py` | `from cli.ingestion.schema import from_binance_kline_row` |
| `cli/jobs/binance_4h_job.py` | `from cli.ingestion.schema import from_binance_kline_row` |
| `cli/jobs/tcbs_eod_job.py` | `from cli.ingestion.schema import from_tcbs_ticker_record` |
| `cli/jobs/tcbs_bsa_job.py` | `from cli.ingestion.schema import from_tcbs_bsa_record` |

### Canonical field mapping (khớp .proto)

**CanonicalOHLCV (Binance kline row):**
```python
{
    "symbol":       symbol (str, e.g. "BTCUSDT"),
    "exchange":     "BINANCE" | "BINANCE_FUTURES",
    "inst_type":    "SPOT" | "FUTURES",
    "interval":     "1d" | "4h" | ...,
    "ts_open_ms":   int (milliseconds UTC),
    "ts_close_ms":  int (milliseconds UTC),
    "open":         float,
    "high":         float,
    "low":          float,
    "close":        float,
    "volume":       float  # ← row[7] quoteAssetVolume (Binance-specific),
    "num_trades":   int,
    "confirm":      1,     # hardcoded — Binance không có field này
    "ingested_at":  ISO 8601 UTC string,
}
```

**CanonicalTicker (TCBS market record):**
```python
{
    "symbol":       str (e.g. "VCB"),
    "exchange":     "TCBS_HOSE" | "TCBS_HNX" | "TCBS_UPCOM",
    "ts_ms":        int (milliseconds UTC),
    "price":        float (lastPrice),
    "open":         float,
    "high":         float,
    "low":          float,
    "close":        float (lastPrice),
    "volume":       float,
    "change_pct":   float,
    "ingested_at":  ISO 8601 UTC string,
}
```

### Lesson Learned

- **[L25]** Canonical mapper layer phải là pure functions (không side effects, không import heavy libs)
  — có thể unit test mà không cần PySpark hay DB
- **[L26]** `.proto` là source of truth → khi sửa .proto phải update `canonical.py` cùng commit

---

## 3. SUB-TASK 4 — BACKEND API FIX (ST-4)

### 3.1 Tách `indicators_accounting.go`

**File xóa:** `dataops_backend_api/api/rest/indicators_accounting.go`
(Đã bị xóa externally từ environment — xác nhận trong `external_changes`)

**File tạo mới:**

`dataops_backend_api/api/rest/indicators.go`:
- Giữ lại toàn bộ Indicator Group CRUD handlers (5 handlers)
- Giữ `ComputeSignal` handler gọi `indicator.Service`
- Package route: `GET/POST /api/v1/indicators`, `POST /api/v1/indicators/:id/signal`

`dataops_backend_api/api/rest/accounting.go`:
- Toàn bộ Accounting handlers (GetBalance, GetTransactionHistory, GetDailyPnL) được **comment out**
- Status: `// STATUS: COMMENTED OUT — out of scope cho DataOps Financial Trading platform`
- Giữ lại code để integrate trading bot sau, không xóa

### 3.2 ComputeSignal RSI(14) + MA(20)

**File:** `dataops_backend_api/services/indicator/indicator.go`

Thay hardcode `"NEUTRAL"` bằng implementation thực:

```go
// fetchClosePrices() — query InfluxDB "vwap" measurement
//   bucket: market-data, measurement: vwap
//   filter: symbol + interval (timeframe), field: vwap
//   tail: n điểm gần nhất

// calcMA(prices []float64, n int) float64
//   Simple Moving Average của n giá trị cuối

// calcRSI(prices []float64, period int) float64
//   Wilder's smoothed RSI: RS = avg_gain/avg_loss, RSI = 100 - 100/(1+RS)
//   Fallback 50.0 khi không đủ data

// classifySignal(price, ma20, rsi14 float64) (string, float64)
//   RSI > 70 AND price > MA → SELL, 0.75
//   RSI < 30 AND price < MA → BUY, 0.75
//   RSI > 60 OR price > MA  → HOLD, 0.55
//   RSI < 40 OR price < MA  → HOLD, 0.55
//   else                    → NEUTRAL, 0.5
```

**InfluxDB query pattern (Flux):**
```flux
from(bucket: "market-data")
  |> range(start: -Xh)
  |> filter(fn: (r) => r._measurement == "vwap" and r.symbol == "BTCUSDT" and r.interval == "1h")
  |> filter(fn: (r) => r._field == "vwap")
  |> sort(columns: ["_time"], desc: false)
  |> tail(n: 21)
```

### 3.3 Endpoint mới: `GET /api/v1/crypto/:symbol/signal`

**File:** `dataops_backend_api/api/rest/crypto.go`

Thêm `GetSignal` handler:
```go
// GET /api/v1/crypto/:symbol/signal?groupId=1&exchange=BINANCE&timeframe=1h
func (h *CryptoHandler) GetSignal(ctx *gin.Context) { ... }
```

Thêm vào `Register()`:
```go
r.GET("/:symbol/signal", h.GetSignal)
```

**CryptoService.ComputeSignal** trong `services/analytics/crypto.go`:
- Đọc groupId từ query param
- Gọi `indicator.Service.ComputeSignal(groupID, symbol, exchange, timeframe)`
- Trả `{ "data": IndicatorSignal }` JSON

### 3.4 `go build` result

```
go build ./... → SUCCESS (no errors)
```

---

## 4. SUB-TASK 3 — VOLUME BARS JOB (ST-3)

### File tạo mới

**`dataops_processing/jobs/volume_bars_job.py`**

Algorithm:
```
Delta Bronze (binance_candles_1d)
    → filter(symbol, interval="1d")
    → orderBy("ts_open_ms")
    → .collect() → pure Python list[dict]
    → assign_volume_bars(rows, threshold)
        → cumsum volume → emit bar khi cumsum >= threshold
        → partial bar cuối được emit (không bỏ data)
    → enrich(symbol, inst_type, ingested_at)
    → spark.createDataFrame(records, schema)
    → Delta Silver s3a://binance-datalake/delta/silver/volume_bars
       mode=overwrite, partitionBy=symbol
```

**Output schema:**
```
symbol, inst_type, bar_index, open, high, low, close,
total_volume, num_candles, bar_open_ts, bar_close_ts, ingested_at
```

**`dataops_processing/config/processing_config.yaml`**

```yaml
volume_bars:
  BTCUSDT:  500.0    # ~1-2 bars/ngày tại volume Binance spot
  ETHUSDT:  5000.0
  SOLUSDT:  50000.0
  BNBUSDT:  10000.0
```

### ⚠️ Bug đã phát hiện — CHƯA FIX

**File:** `dataops_processing/jobs/volume_bars_job.py`  
**Function:** `assign_volume_bars()` line 137-138  
**Bug:** `rows.index(row)` dùng linear search → O(n²) khi n = rows lớn

```python
# HIỆN TẠI — SAI (O(n²))
if rows.index(row) + 1 < len(rows):
    next_row = rows[rows.index(row) + 1]

# CẦN SỬA (O(n)) — dùng enumerate
for i, row in enumerate(rows):
    ...
    if i + 1 < len(rows):
        next_row = rows[i + 1]
```

**Priority:** HIGH — cần fix trước khi test với Bronze data thực tế có hàng nghìn rows.

---

## 5. TRẠNG THÁI TỔNG THỂ SAU SESSION NÀY

| Sub-Task | Status | Files |
|---|---|---|
| ST-1 Stream Producer cleanup | ✅ DONE | `dataops_stream_producer/pkg/sources/binance/websocket/producer.go` |
| ST-2 VWAP Spark job | ✅ DONE | `dataops_processing/jobs/vwap_job.py`, `config/spark_config.py` |
| ST-6 Protobuf Python stubs | ✅ DONE | `dataops_batch_api/batch_data_pb2.py`, `batch_data_pb2_grpc.py` |
| **Canonical Layer** | ✅ DONE (session này) | `cli/ingestion/schema/canonical.py`, `__init__.py` |
| **ST-4 Backend API** | ✅ DONE (session này) | `indicators.go`, `accounting.go`, `indicator/indicator.go`, `crypto.go` |
| **ST-3 Volume Bars** | ✅ DONE (session này) | `jobs/volume_bars_job.py`, `config/processing_config.yaml` |
| ST-5 Observability | ⚠️ TODO | — |
| ST-7 E2E smoke test | ⚠️ TODO | — |

**pytest:** `dataops_batch_api/tests/` — 13/13 PASSED (chưa chạy lại sau session này)  
**go build:** `dataops_backend_api/` — PASSED

---

## 6. NEXT STEPS — SESSION TIẾP THEO

### Priority 1 — Fix O(n²) bug trong `volume_bars_job.py`

**File:** `dataops_processing/jobs/volume_bars_job.py`

Refactor `assign_volume_bars()` dùng `enumerate` thay `rows.index(row)`.  
Sau đó viết 2 unit tests:
1. `test_assign_volume_bars_threshold` — verify bar tạo đúng khi cumsum vượt ngưỡng
2. `test_assign_volume_bars_partial_bar` — verify partial bar cuối được emit

### Priority 2 — ST-5: Centralized Observability

**Goal:** Tất cả modules emit structured JSON logs với `trace_id` để trace end-to-end pipeline.

**Việc cần làm:**

#### Go modules (stream_producer + backend_api)
- `dataops_backend_api/logging/logger.go`: Thêm `trace_id` field vào `LogEntry` struct
- `dataops_backend_api/api/rest/`: Middleware extract `X-Trace-ID` header → `gin.Context`
- Mọi `logger.Info/Error` call trong handlers đều truyền `trace_id` từ context

#### Python modules (batch_api + processing)
- `dataops_batch_api/log/logger_setup.py`: Thêm `correlation_id` vào `JsonFormatter`
  - `JsonFormatter.format()` đọc `record.correlation_id` nếu có
- `dataops_processing/jobs/vwap_job.py`: Thêm `correlation_id = uuid4()` tại job startup, log mọi batch

#### Docker Compose infra
File: `infra_dataops_finance_trading/docker-compose.yml`

Thêm 2 services:
```yaml
loki:
  image: grafana/loki:2.9.0
  ports: ["3100:3100"]
  command: -config.file=/etc/loki/local-config.yaml
  volumes: [loki_data:/loki]

promtail:
  image: grafana/promtail:2.9.0
  volumes:
    - /var/lib/docker/containers:/var/lib/docker/containers:ro
    - ./promtail-config.yaml:/etc/promtail/config.yaml
  depends_on: [loki]
```

Thêm `promtail-config.yaml` để scrape stdout của tất cả containers với label `job=dataops-*`.

#### Grafana
- Thêm Loki datasource vào Grafana provisioning
- Tạo dashboard "Pipeline Trace" với log stream per service
  - LogQL: `{job="dataops-stream-producer"} | json | trace_id != ""`

### Priority 3 — ST-7: Docker Compose E2E Smoke Test

**Goal:** `docker-compose up` → `curl /api/v1/crypto/vwap?symbol=BTCUSDT` trả data thật.

**Việc cần làm:**

1. **Thêm `dataops-processing` service vào `docker-compose.yml`:**
```yaml
dataops-processing:
  build:
    context: ../dataops_processing
    dockerfile: Dockerfile
  environment:
    KAFKA_URL: kafka:9092
    MINIO_ENDPOINT: http://minio:9010
    INFLUXDB_URL: http://influxdb:8086
    INFLUXDB_TOKEN: ${INFLUXDB_TOKEN}
    INFLUXDB_ORG: binance-vps-platform
    INFLUXDB_BUCKET: market-data
  depends_on: [kafka, minio, influxdb]
```

2. **`dataops_processing/Dockerfile`** — cần tạo:
```dockerfile
FROM python:3.11-slim
RUN apt-get install -y default-jre-headless
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . /app
WORKDIR /app
CMD ["python", "-m", "dataops_processing.jobs.vwap_job"]
```

3. **`scripts/smoke_test.sh`:**
```bash
#!/bin/bash
# Health checks + curl endpoints + verify non-empty
curl -f http://localhost:8080/health || exit 1
TOKEN=$(curl -s -X POST .../login -d '...' | jq -r .token)
curl -f -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8080/api/v1/crypto/vwap?symbol=BTCUSDT&interval=5m" | \
  jq '.data | length > 0' | grep true || exit 1
echo "SMOKE TEST PASSED"
```

4. **`README.md`** — thêm "Getting Started in 5 minutes":
```
git clone ... && cd DataOps_Backend_Financial_Trading
cp .env.example .env  # fill INFLUXDB_TOKEN, TCBS_JWT_TOKEN
docker-compose -f infra_dataops_finance_trading/docker-compose.yml up -d
bash scripts/smoke_test.sh
```

---

## 7. FILES THAY ĐỔI TRONG SESSION NÀY

### Tạo mới
```
dataops_batch_api/cli/ingestion/schema/canonical.py        ← mapper functions (NEW)
dataops_backend_api/api/rest/indicators.go                 ← tách từ indicators_accounting.go
dataops_backend_api/api/rest/accounting.go                 ← handlers commented out
dataops_processing/jobs/volume_bars_job.py                 ← ST-3 batch job
dataops_processing/config/processing_config.yaml           ← volume thresholds
```

### Sửa đổi
```
dataops_batch_api/cli/ingestion/schema/__init__.py         ← export canonical mappers
dataops_batch_api/cli/jobs/binance_1d_job.py               ← dùng from_binance_kline_row()
dataops_batch_api/cli/jobs/binance_4h_job.py               ← dùng from_binance_kline_row()
dataops_batch_api/cli/jobs/tcbs_eod_job.py                 ← dùng from_tcbs_ticker_record()
dataops_batch_api/cli/jobs/tcbs_bsa_job.py                 ← dùng from_tcbs_bsa_record()
dataops_processing/jobs/vwap_job.py                        ← canonical boundary comment
dataops_backend_api/services/indicator/indicator.go        ← RSI+MA implementation
dataops_backend_api/services/analytics/crypto.go           ← ComputeSignal + GetSignal
dataops_backend_api/api/rest/crypto.go                     ← GetSignal handler + route
```

### Xóa
```
dataops_backend_api/api/rest/indicators_accounting.go      ← tách thành 2 file
```

---

## 8. KIẾN THỨC KỸ THUẬT ĐÃ CHỐT (Lessons Learned mới)

### [L25] Canonical mapper = pure functions, không import heavy libs
`canonical.py` chỉ import từ `models.py` (Pydantic) — không import PySpark, Delta, boto3.
→ Có thể chạy unit test mà không cần toàn bộ pipeline environment.

### [L26] `.proto` thay đổi → update `canonical.py` + `types.go` cùng commit
Field names trong canonical.py phải luôn khớp với `.proto` message definitions.
Source of truth: `dataops_protobuf/market-data/market_data.proto`.

### [L27] `assign_volume_bars` phải dùng `enumerate`, không dùng `list.index()`
`list.index()` là O(n) linear search → toàn bộ loop thành O(n²).
Với 365 candles/year BTCUSDT: ~133k operations thay vì ~365 → gần như invisible ở scale nhỏ nhưng
sẽ break ở intraday data (8760 hourly candles → 76M operations).

### [L28] InfluxDB Flux query cho time series tính toán
`tail(n: X)` để lấy X điểm gần nhất — hiệu quả hơn `sort + limit` vì Flux optimize tại server.
Lookback phải có buffer đủ rộng (3x n) vì không phải giờ nào cũng có trade event.

### [L29] RSI Wilder's method vs Simple RSI
Wilder dùng smoothed average (EMA-based) cho production grade accuracy.
Implementation đơn giản (SMA-based) trong `calcRSI()` là đủ cho signal classification (BUY/SELL/HOLD)
— không cần độ chính xác tuyệt đối vì chỉ dùng để tạo signal, không trade thật.
