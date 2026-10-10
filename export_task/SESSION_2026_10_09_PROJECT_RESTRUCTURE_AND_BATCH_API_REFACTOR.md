# SESSION EXPORT — Project Restructure Plan + dataops_batch_api Refactor

**Date:** 2026-10-09 (session 3)
**Branch:** `20260710_data_batch_api`
**Scope:** `docs/` · `dataops_batch_api/`
**References:**
- `docs/PROJECT_RESTRUCTURE_PLAN.md` — plan file đầy đủ
- `github/copilotinstructions/SKILL.md`
- `github/agent/batch-lakehouse-engineer.agent.md`

---

## 1. MỤC TIÊU SESSION

### Phần 1 — Làm rõ mục tiêu project
Project bị "split identity" giữa Data Platform và Web Backend. Session này chốt lại:
- **Primary goal:** Data Platform pipeline end-to-end với dữ liệu thật (Kafka + Spark + Delta Lake)
- **Secondary goal:** Backend API serve data đó
- **Portfolio target:** Senior Data Engineer / Backend Engineer tại Fintech

### Phần 2 — Refactor `dataops_batch_api`
Dọn dẹp cấu trúc file bị nhân đôi, fix import bugs, fix test failures.

---

## 2. CHẨN ĐOÁN VẤN ĐỀ (từ code thực tế)

| Vấn đề phát hiện | File | Mức độ |
|---|---|---|
| `cli/flows/jobs/` là stale duplicate của `cli/jobs/` | cả thư mục | 🔴 Critical — source of truth mơ hồ |
| `cli/jobs/__init__.py` self-import gây cascade fail | `cli/jobs/__init__.py:19` | 🔴 Critical — tests 100% fail |
| `deltalake` hard import làm test fail offline | `cli/jobs/_delta_writer.py:26` | 🔴 Critical |
| `tcbs_eod_job.py` import `TCBSConstants` từ indirect path | `tcbs_eod_job.py:48` | 🟡 Medium |
| `cli/ingestion/sources/binance_constants_loader.py` unused duplicate | đó | 🟡 Medium |
| Test schema enforcement dùng `str` input thay vì `float` | `test_delta_writer.py:226` | 🟡 Medium |
| `dataops_processing/` rỗng — Kafka→Delta Silver bị đứt | thư mục rỗng | 🔴 Critical (phase 2) |
| `ComputeSignal` hardcode `"NEUTRAL"` placeholder | `indicator.go:227` | 🟡 Medium (phase 2) |
| VWAP accumulation làm trong Go Stream Producer (sai layer) | `producer.go:160-188` | 🟡 Medium (phase 2) |

---

## 3. PLAN FILE ĐÃ TẠO

File: `docs/PROJECT_RESTRUCTURE_PLAN.md`

### 7 Sub-Tasks theo thứ tự ưu tiên

| # | Sub-Task | Dependency | Status |
|---|---|---|---|
| 1 | Stream Producer cleanup — bỏ buyVol/sellVol accumulation | — | ⚠️ TODO |
| 2 | **VWAP Spark Job** — Kafka → InfluxDB + Delta Silver | ST-1 | ⚠️ TODO |
| 3 | Volume Bars Job (simplified batch) | ST-2 infra | ⚠️ TODO |
| 4 | Backend API fix — connect InfluxDB/Delta thật | ST-2, ST-6 | ⚠️ TODO |
| 5 | Centralized Observability (Loki) | — | ⚠️ TODO |
| 6 | Protobuf codegen (gRPC unblock) | — | ⚠️ TODO |
| 7 | Docker Compose smoke test end-to-end | All | ⚠️ TODO |

### Confirmed decisions
- Observability: Grafana Loki (đã có trong docker-compose)
- Volume Bars: time-volume bars đơn giản (không phải De Prado stateful streaming)
- Indicators/Accounting CRUD: **loại hoàn toàn** khỏi scope

---

## 4. REFACTOR `dataops_batch_api` — ĐÃ THỰC HIỆN

### 4.1 Files đã xóa

| File/Dir | Lý do |
|---|---|
| `cli/flows/jobs/` (toàn bộ thư mục) | Stale duplicate của `cli/jobs/`. Jobs cũ hơn — thiếu geo-block fix, thiếu `apply_rolling_retention` đúng. Canonical là `cli/jobs/`. |
| `cli/ingestion/sources/binance_constants_loader.py` | Unused duplicate re-export. `binance_constants.py` cùng thư mục đã làm việc đó rồi. |

### 4.2 Files đã sửa

#### `cli/jobs/_delta_writer.py`
**Vấn đề:** `from deltalake import DeltaTable, write_deltalake` là hard import — nếu `deltalake` chưa install (môi trường test offline), toàn bộ module fail khi import.

**Fix:** Lazy import với `try/except`:
```python
try:
    from deltalake import DeltaTable, write_deltalake
except ImportError:  # pragma: no cover
    DeltaTable = None      # type: ignore[assignment,misc]
    write_deltalake = None  # type: ignore[assignment]
```
**Lesson learned:** Các dependency nặng (deltalake, pyspark, grpcio) nên lazy import để test mock hoạt động không cần cài đặt thật.

---

#### `cli/jobs/__init__.py`
**Vấn đề:** Self-import gây cascade fail:
```python
# CŨ — WRONG
from cli.jobs import (
    _delta_writer, binance_1d_job, ...  # import chính mình!
)
```
Khi `_delta_writer` fail (thiếu `deltalake`), toàn bộ `cli.jobs` package bị broken → `@patch("cli.jobs._delta_writer.write_deltalake")` ném `AttributeError`.

**Fix:** Xóa eager imports, chỉ giữ docstring. Python package không cần re-export các submodules trong `__init__.py`.

```python
# MỚI — CORRECT
"""
cli/jobs — Scheduled ETL Jobs cho dataops_batch_api.
...
"""
# Không import gì — caller import trực tiếp từ cli.jobs.binance_1d_job, v.v.
```

---

#### `cli/jobs/tcbs_eod_job.py`
**Vấn đề:** Import `TCBSConstants` từ `cli.flows.loader.tcbs_constants_loader` (indirect re-export path).

**Fix:**
```python
# CŨ
from cli.flows.loader.tcbs_constants_loader import TCBSConstants

# MỚI — import trực tiếp từ canonical source
from cli.ingestion.sources.tcbs_constants_loader import TCBSConstants
```

---

#### `tests/test_delta_writer.py`
**Vấn đề:** `TestSchemaEnforcement` dùng string `"99999.5"` làm input cho `float64` field — test giả định `pa.Table.from_pylist` sẽ auto-cast, nhưng PyArrow không làm vậy.

**Fix:** Dùng native `float` value. Thêm assertion giá trị thực tế:
```python
# CŨ
records = [{"symbol": "BTCUSDT", "close": "99999.5"}]  # string — wrong

# MỚI
records = [{"symbol": "BTCUSDT", "close": 99999.5}]    # float — correct
# Thêm:
self.assertAlmostEqual(written_table.column("close")[0].as_py(), 99999.5)
```

**Lesson learned:** `pa.Table.from_pylist` không auto-cast types. Pydantic validate trước → output luôn là native Python types → không cần cast trong `write_records`.

---

### 4.3 Import dependency graph sau refactor

```
cli/flows/loader/
    ├── tcbs_constants_loader.py   ← CANONICAL (implementation thật, load YAML)
    ├── binance_constants_loader.py ← CANONICAL
    └── load_config.py             ← CANONICAL

cli/ingestion/sources/
    ├── tcbs_constants_loader.py   ← re-export → cli.flows.loader.tcbs_constants_loader
    └── binance_constants.py       ← re-export → cli.flows.loader.binance_constants_loader

cli/jobs/                          ← CANONICAL jobs (không duplicate ở flows/jobs nữa)
    ├── _delta_writer.py           ← lazy deltalake import
    ├── binance_1d_job.py
    ├── binance_4h_job.py
    ├── tcbs_eod_job.py
    ├── tcbs_bsa_job.py
    └── metrics.py
```

---

## 5. KẾT QUẢ VALIDATION

```
cd dataops_batch_api
python -m pytest tests/test_delta_writer.py -v

13 passed in 2.17s ✅
```

**Trước:** 13/13 FAILED (cascade từ `__init__.py` self-import + hard deltalake import)
**Sau:** 13/13 PASSED

---

## 6. DATA BOUNDARY ĐƯỢC CHỐT

| Loại dữ liệu | Layer sở hữu | Storage | KHÔNG làm ở |
|---|---|---|---|
| Real-time ticks (Binance WSS) | stream_producer | Redis TTL 10s | batch_api |
| Historical OHLCV | **batch_api** | Delta Bronze | stream_producer |
| VWAP aggregated | processing (Spark) — TODO | InfluxDB + Delta Silver | stream_producer (hiện sai) |
| TCBS market snapshot | stream_producer | Redis TTL 120s | batch_api (real-time) |
| TCBS historical EOD | **batch_api** | Delta Bronze | stream_producer |
| TCBS account/order | backend_api (proxy) | Redis TTL 30s | Delta Lake |
| User auth/session | backend_api | MySQL | Kafka, Delta |

---

## 7. CÔNG VIỆC CÒN LẠI (TODO tiếp theo)

### Priority 1 — Stream Producer cleanup (Sub-Task 1)
- File: `dataops_stream_producer/pkg/sources/binance/websocket/producer.go`
- Xóa logic tích lũy `buyVol/sellVol` trong `handleMessage()` line 160–188
- Redis ticker set chỉ: `price, size, side, trade_id, ts` (snapshot, không accumulate)
- Thêm comment: "Aggregation là trách nhiệm của dataops_processing"

### Priority 2 — Build `dataops_processing` VWAP Job (Sub-Task 2)
- Tạo thư mục `dataops_processing/` với `config/spark_config.py` và `jobs/vwap_job.py`
- SparkSession: `spark-sql-kafka`, `delta-spark 3.1.0`, S3A MinIO connector
- Kafka consume `binance.trade.ticks` (Avro schema: `dataops_stream_producer/pkg/schemas/binance_trade_event.avsc`)
- Watermark 30s, groupBy 5-min/1-min window, VWAP formula
- Dual sink: InfluxDB bucket `binance-vwap` + Delta Silver `silver/vwap_1m`

### Priority 3 — Protobuf codegen (Sub-Task 6 — unblocks Sub-Task 4)
```bash
cd dataops_protobuf
python -m grpc_tools.protoc \
    -I. \
    --python_out=../dataops_batch_api \
    --grpc_python_out=../dataops_batch_api \
    batch-data/batch_data.proto
# Output: dataops_batch_api/batch_data_pb2.py + batch_data_pb2_grpc.py
```

### Priority 4 — Backend API InfluxDB fix (Sub-Task 4)
- Sau khi VWAP job chạy, audit `repository/influxdb/influxdb.go`
- Fix `services/analytics/crypto.go` `GetVWAP()` — map đúng field names từ InfluxDB `vwap_5m` measurement
- Tách `indicators_accounting.go` → `indicators.go` + `accounting.go` (accounting comment out)

---

## 8. LESSON LEARNED SESSION NÀY

### [L16] Package `__init__.py` không nên eager-import submodules
Nếu submodule có heavy dependency (deltalake, pyspark, grpcio), eager import trong `__init__.py` làm toàn bộ package unimportable khi thiếu dependency. Pattern đúng: caller import trực tiếp từ submodule path.

### [L17] Lazy import cho heavy dependencies trong shared utilities
`_delta_writer.py` là utility được import bởi cả production code lẫn test mocks. Hard import `deltalake` tại module level phá vỡ toàn bộ test suite nếu package không có. Pattern:
```python
try:
    from heavy_lib import SomeClass
except ImportError:
    SomeClass = None  # test mock replace này
```

### [L18] Duplicate directories là dấu hiệu refactor chưa hoàn thành
`cli/flows/jobs/` vs `cli/jobs/` — 2 bản song song với content khác nhau = không có single source of truth. Quy tắc: khi phát hiện duplicate directory, xác định cái nào mới hơn (geo-block fix, `apply_rolling_retention` đúng) → giữ cái đó, xóa cái kia, cập nhật imports.

### [L19] `pa.Table.from_pylist` không auto-cast types
PyArrow KHÔNG tự cast `"99999.5"` (string) → `float64`. Pydantic validate trước → output luôn là native Python types. Tests phải dùng đúng native types, không dựa vào auto-cast behavior không có thật.

### [L20] Phân tích import graph trước khi xóa file
Trước khi xóa file duplicate, luôn check:
1. `Get-ChildItem -Recurse -Filter "*.py" | Select-String "module_name"` để tìm tất cả importers
2. Nếu có importer → cần update import path, không chỉ xóa
3. Nếu không có importer → xóa an toàn
