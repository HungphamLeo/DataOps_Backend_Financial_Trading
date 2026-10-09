# SESSION EXPORT — Batch API: gRPC Server, Prometheus Metrics, Chunked Backfill & Test

**Date:** 2026-10-09 (session 2)
**Branch:** `20260710_data_batch_api`
**Scope:** `dataops_batch_api/`
**References:**
- `export_task/SESSION_2026_10_09_BACKEND_API_REFACTOR_AND_INFRA_CONFIG.md`
- `github/agent/batch-lakehouse-engineer.agent.md`
- `github/copilotinstructions/SKILL.md`

---

## 1. MỤC TIÊU SESSION

Hoàn thành các TODO còn lại từ session trước (`SESSION_2026_10_09_BACKEND_API_REFACTOR_AND_INFRA_CONFIG.md`):

| Priority | Task |
|---|---|
| MEDIUM | Xác nhận port gRPC `50051:50051` trong `docker-compose.yml` |
| MEDIUM | Bổ sung `ticker_commons_fields()` vào `TCBSConstants` |
| MEDIUM | Thêm chunked pagination vào `binance_1d_job.py` để hỗ trợ backfill 10 năm |
| LOW | Unit tests mock MinIO S3 cho `_delta_writer.py` |
| LOW | gRPC Python server với `grpc.health.v1` healthcheck |
| LOW | Prometheus metrics counter ingest success/failure cho tất cả batch jobs |

---

## 2. CHI TIẾT CÁC FILE ĐÃ TẠO VÀ CHỈNH SỬA

### Files Tạo Mới

| File | Vai trò |
|---|---|
| [`dataops_batch_api/grpc_server.py`](dataops_batch_api/grpc_server.py) | Python gRPC server: `HistoricalDataServicer` (4 RPC methods) + `grpc.health.v1` healthcheck + graceful shutdown 5s |
| [`dataops_batch_api/cli/jobs/metrics.py`](dataops_batch_api/cli/jobs/metrics.py) | Prometheus counter module: `record_ingest()` + `push_metrics()`. Optional import — no-crash nếu thiếu lib |
| [`dataops_batch_api/tests/__init__.py`](dataops_batch_api/tests/__init__.py) | Package init cho test suite |
| [`dataops_batch_api/tests/test_delta_writer.py`](dataops_batch_api/tests/test_delta_writer.py) | 12 unit tests mock MinIO cho `_delta_writer.py` (không cần MinIO thực) |

### Files Cập Nhật

| File | Thay đổi chính |
|---|---|
| [`dataops_batch_api/cli/flows/loader/tcbs_constants_loader.py`](dataops_batch_api/cli/flows/loader/tcbs_constants_loader.py) | Thêm classmethod `ticker_commons_fields()` → trả về `Dict[str, str]` mapping từ section `ticker_commons_fields` trong `tcbs_constants.yaml` |
| [`dataops_batch_api/cli/jobs/binance_1d_job.py`](dataops_batch_api/cli/jobs/binance_1d_job.py) | Thêm `_fetch_chunked()` với auto-pagination (chunk 900 ngày/request); tích hợp `record_ingest` + `push_metrics` |
| [`dataops_batch_api/cli/jobs/binance_4h_job.py`](dataops_batch_api/cli/jobs/binance_4h_job.py) | Tích hợp `record_ingest` + `push_metrics` |
| [`dataops_batch_api/cli/jobs/tcbs_eod_job.py`](dataops_batch_api/cli/jobs/tcbs_eod_job.py) | Thêm import `TCBSConstants` (đã có từ trước) + `record_ingest` + `push_metrics` |
| [`dataops_batch_api/cli/jobs/tcbs_bsa_job.py`](dataops_batch_api/cli/jobs/tcbs_bsa_job.py) | Tích hợp `record_ingest` + `push_metrics` |
| [`dataops_batch_api/requirements.txt`](dataops_batch_api/requirements.txt) | Thêm `grpcio-health-checking==1.64.1` và `prometheus-client==0.20.0` |

---

## 3. QUYẾT ĐỊNH KỸ THUẬT & DESIGN DECISIONS

### [D1] `ticker_commons_fields()` — Đọc từ YAML, không hardcode

`tcbs_constants.yaml` đã có sẵn section `ticker_commons_fields` với mapping đầy đủ `snake_case → camelCase`.
Method chỉ cần gọi `cls._data.get("ticker_commons_fields", {})` — không cần thêm YAML key mới.

```python
# tcbs_constants_loader.py
@classmethod
def ticker_commons_fields(cls) -> Dict[str, str]:
    cls._ensure_loaded()
    return cls._data.get("ticker_commons_fields", {})
```

**Lesson learned:** Luôn kiểm tra YAML đã có section chưa trước khi thêm data mới.

---

### [D2] Chunked Pagination cho Binance Backfill 10 năm

**Vấn đề:** `binance_1d_job.py` ban đầu tính `limit = min(days+1, 1000)` — với range 10 năm (3288 ngày) thì chỉ lấy được 1000 candles đầu tiên.

**Giải pháp:** Hàm `_fetch_chunked()` — chia range thành chunks 900 ngày (buffer dưới 1000 max của Binance):

```python
_CHUNK_DAYS = 900  # < 1000 max per Binance request
_MS_PER_DAY = 86_400_000

def _fetch_chunked(client, symbol, inst_type, start_ms, end_ms, now_iso):
    records = []
    chunk_start = start_ms
    while chunk_start < end_ms:
        chunk_end = min(chunk_start + _CHUNK_DAYS * _MS_PER_DAY, end_ms)
        rows = client.get_klines(..., start_time=chunk_start, end_time=chunk_end, limit=limit)
        # advance: last_close_ms + 1ms (tránh overlap)
        chunk_start = int(rows[-1][6]) + 1
        if len(rows) < limit:
            break  # hết data
    return records
```

**Kết quả:** Range 2016-01-01 → 2026-01-01 = 3288 ngày → **4 requests per symbol** (thay vì bị cắt ngắn ở 1000).

**Lệnh backfill:**
```bash
cd dataops_batch_api
python -m cli.jobs.binance_1d_job 2016-01-01 2026-01-01
```

---

### [D3] gRPC Server Architecture

```
grpc_server.py
├── HistoricalDataServicer
│   ├── GetBinanceOHLCV   → DeltaTable("binance_candles_4h" / "binance_candles_1d")
│   ├── GetTCBSMarketEOD  → DeltaTable("tcbs_market_eod")
│   ├── GetTCBSBSAHistory → DeltaTable("tcbs_bsa_daily")
│   └── GetDataRange      → metadata: oldest/newest date, total count
└── grpc.health.v1.Health
    ├── "" (server)                           → SERVING
    └── "batchdata.v1.HistoricalDataService"  → SERVING
```

**Key decisions:**
- Dùng `deltalake` (delta-rs) thay vì PySpark — đủ nhanh cho batch queries < 10k records, không cần JVM startup
- `_PROTO_AVAILABLE` flag — server vẫn start được kể cả khi proto stubs chưa được generate (graceful degradation)
- Graceful shutdown: set health → `NOT_SERVING` trước, rồi `server.stop(grace=5)`
- Hard cap `_MAX_LIMIT = 10_000` cho tất cả queries

**Codegen command (cần chạy 1 lần):**
```bash
cd dataops_protobuf
python -m grpc_tools.protoc \
    -I. \
    --python_out=../dataops_batch_api \
    --grpc_python_out=../dataops_batch_api \
    batch-data/batch_data.proto
# Output: dataops_batch_api/batch_data_pb2.py + batch_data_pb2_grpc.py
```

**Khởi động server:**
```bash
cd dataops_batch_api
python grpc_server.py
# Hoặc: GRPC_PORT=50052 GRPC_MAX_WORKERS=20 python grpc_server.py
```

---

### [D4] Prometheus Metrics — Optional Dependency Pattern

```python
# cli/jobs/metrics.py
try:
    from prometheus_client import CollectorRegistry, Counter, push_to_gateway
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False  # no-op, không crash job

# Counter: batch_ingest_records_total{job, symbol, inst_type, status}
```

**Integration pattern** (đồng nhất trong tất cả 4 jobs):
```python
record_ingest("binance_1d_job", count=written, status="success")
if errors:
    record_ingest("binance_1d_job", count=errors, status="error")
push_metrics("binance_1d_job")
```

**Push to Pushgateway:** Set env var `PROMETHEUS_PUSHGATEWAY_URL=http://pushgateway:9091`

---

### [D5] Unit Test Strategy cho `_delta_writer.py`

Mock toàn bộ I/O, không cần MinIO/AWS credentials:
```python
@patch("cli.jobs._delta_writer.write_deltalake")
def test_happy_path_returns_count(self, mock_write):
    ...
```

**12 test cases:**
1. `_table_path` — format `s3://bucket/delta/layer/table`
2. `_storage_options` — đọc đúng env vars + defaults + missing key raises `KeyError`
3. `write_records` — happy path count, correct args, empty list skip, exception propagate, `partition_by=None`
4. `apply_rolling_retention` — delete predicate, table-not-found no-raise
5. Schema enforcement — PyArrow cast string → float64

**Chạy tests:**
```bash
cd dataops_batch_api
pip install pyarrow deltalake pydantic PyYAML prometheus-client grpcio grpcio-health-checking
pytest tests/test_delta_writer.py -v
```

---

## 4. TRẠNG THÁI & CÔNG VIỆC TIẾP THEO

| Hạng mục | Trạng thái | Ghi chú |
|---|---|---|
| Port gRPC `50051` docker-compose | ✅ Done | Đã có từ session trước (line 686) |
| `TCBSConstants.ticker_commons_fields()` | ✅ Done | Đọc từ YAML section có sẵn |
| Chunked pagination `binance_1d_job` | ✅ Done | 4 chunks × 900 ngày cho 10 năm |
| Unit tests `_delta_writer.py` | ✅ Done | 12 test cases, mock-only |
| `grpc_server.py` + `grpc.health.v1` | ✅ Done | Cần chạy protoc codegen trước |
| Prometheus metrics 4 jobs | ✅ Done | Optional dep, push via Pushgateway |
| Protoc codegen `batch_data_pb2` | ⚠️ TODO | Chạy lệnh codegen → generate `batch_data_pb2.py` |
| Backfill Binance 1D chạy thực tế | ⚠️ TODO | Cần MinIO đang chạy + env vars AWS_* |
| Observability: `grpc.health.v1` trong docker-compose healthcheck | ⚠️ TODO | Thêm healthcheck cho `dataops-batch-api` container |

---

## 5. ENV VARS MỚI CẦN SET

```bash
# grpc_server.py
GRPC_PORT=50051              # default 50051
GRPC_MAX_WORKERS=10          # default 10

# metrics push (optional)
PROMETHEUS_PUSHGATEWAY_URL=http://pushgateway:9091

# _delta_writer (đã có từ trước, nhắc lại)
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_ENDPOINT_URL=http://minio:9000
AWS_REGION=us-east-1
DELTA_BUCKET=binance-datalake
```

---

## 6. DEPENDENCY MỚI TRONG requirements.txt

```
grpcio-health-checking==1.64.1   # grpc.health.v1 standard healthcheck
prometheus-client==0.20.0        # Prometheus metrics (optional — no-op nếu thiếu)
```
