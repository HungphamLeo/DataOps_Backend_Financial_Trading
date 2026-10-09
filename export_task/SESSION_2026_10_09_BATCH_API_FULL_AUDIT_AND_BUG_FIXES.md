# SESSION EXPORT — Batch API: Toàn Diện Audit, Sửa Lỗi Imports, Constants Loaders & Pipeline Jobs

**Date:** 2026-10-09 (session 3)  
**Branch:** `20260710_data_batch_api`  
**Scope:** `dataops_batch_api/`  
**References:**
- `export_task/SESSION_2026_10_09_BATCH_API_GRPC_METRICS_COMPLETION.md`
- `export_task/SESSION_2026_10_08_GRPC_ETL_DESIGN_AND_CLEANUP.md`
- `export_task/SESSION_2026_10_08_BATCH_CLI_REFACTOR_LOGGER_SEPARATION.md`
- `github/agent/batch-lakehouse-engineer.agent.md`
- `github/copilotinstructions/SKILL.md`

---

## 1. MỤC TIÊU SESSION

Thực hiện rà soát và kiểm thử toàn diện toàn bộ module `dataops_batch_api/`, phát hiện tất cả các lỗi import, lỗi pathing YAML, lỗi phân tầng kiến trúc, logic parse API response và cấu hình Dockerfile, sau đó sửa chữa dứt điểm và chạy kiểm thử tự động.

---

## 2. CHI TIẾT CÁC LỖI ĐÃ PHÁT HIỆN VÀ KHẮC PHỤC

### [Bug 1] `FileNotFoundError` khi load YAML constants
- **Hiện tượng:** `TCBSConstants._ensure_loaded()` và `BinanceConstants._ensure_loaded()` ném lỗi `FileNotFoundError` do hardcode đường dẫn `Path(__file__).parent / "tcbs_constants.yaml"` trong khi file cấu hình nằm ở `cli/ingestion/sources/config/`.
- **Khắc phục:** Thêm hàm `_find_yaml_path()` hỗ trợ fallback đa tầng (`cli/ingestion/sources/config/`, `cli/flows/config/`, sibling path).
- **Files:** [`cli/flows/loader/tcbs_constants_loader.py`](dataops_batch_api/cli/flows/loader/tcbs_constants_loader.py), [`cli/flows/loader/binance_constants_loader.py`](dataops_batch_api/cli/flows/loader/binance_constants_loader.py).

### [Bug 2] `ModuleNotFoundError` trong `tcbs_client` mixins
- **Hiện tượng:** 6 module mixin trong `cli/ingestion/sources/client/tcbs_client/` (`tcbs_client.py`, `batch_helpers.py`, `securities.py`, `intraday_analytics.py`, `market_data.py`, `auth.py`) cố định import `from cli.ingestion.sources.tcbs_constants_loader import TCBSConstants`, nhưng loader thực tế nằm ở `cli.flows.loader`.
- **Khắc phục:** Tạo re-export wrappers tương thích ngược tại `cli/ingestion/sources/` cho `tcbs_constants_loader.py`, `binance_constants_loader.py` và `binance_constants.py`.
- **Files:** [`cli/ingestion/sources/tcbs_constants_loader.py`](dataops_batch_api/cli/ingestion/sources/tcbs_constants_loader.py), [`cli/ingestion/sources/binance_constants_loader.py`](dataops_batch_api/cli/ingestion/sources/binance_constants_loader.py), [`cli/ingestion/sources/binance_constants.py`](dataops_batch_api/cli/ingestion/sources/binance_constants.py).

### [Bug 3] Đứt gãy cấu trúc `cli.jobs` và `cli.scheduler`
- **Hiện tượng:** Thư mục `cli/jobs/` và `scheduler.py` bị di dời sang `cli/flows/jobs/` khiến toàn bộ câu lệnh CLI (`python -m cli.scheduler`, `python -m cli.jobs.binance_1d_job`), `grpc_server.py`, `test_delta_writer.py` và các imports nội bộ bị lỗi.
- **Khắc phục:** Khôi phục cấu trúc chuẩn `dataops_batch_api/cli/jobs/` (`__init__.py`, `_delta_writer.py`, `binance_1d_job.py`, `binance_4h_job.py`, `tcbs_bsa_job.py`, `tcbs_eod_job.py`, `metrics.py`) và `dataops_batch_api/cli/scheduler.py`, đồng thời tạo alias re-export ở `cli/flows/jobs` và `cli/flows/scheduler.py`.
- **Files:** [`cli/jobs/__init__.py`](dataops_batch_api/cli/jobs/__init__.py), [`cli/scheduler.py`](dataops_batch_api/cli/scheduler.py), [`cli/flows/jobs/__init__.py`](dataops_batch_api/cli/flows/jobs/__init__.py), [`cli/flows/scheduler.py`](dataops_batch_api/cli/flows/scheduler.py).

### [Bug 4] `TypeError` khi gọi `client.authenticate()`
- **Hiện tượng:** `TCBSAuthMixin.authenticate(self, otp: str)` thiếu default parameter, khiến việc gọi `client.authenticate()` không tham số trong `tcbs_eod_job.py` và `tcbs_bsa_job.py` văng lỗi `missing 1 required positional argument: 'otp'`.
- **Khắc phục:** Đổi signature thành `authenticate(self, otp: str = "")`, ưu tiên sử dụng token đã được nạp từ biến môi trường `TCBS_JWT_TOKEN`, hoặc fallback lấy OTP từ `TCBS_OTP`.
- **Files:** [`cli/ingestion/sources/client/tcbs_client/auth.py`](dataops_batch_api/cli/ingestion/sources/client/tcbs_client/auth.py).

### [Bug 5] Lỗi parse response TCBS & xử lý `index_code`
- **Hiện tượng:** 
  1. `client.get_ticker_commons()` và `client.get_intraday_bsa()` trả về `dict` (`{"data": [...]}`). Code cũ lặp trực tiếp `for row in raw_list:` duyệt qua keys dạng chuỗi (`"tradingDate"`, `"data"`) dẫn đến lỗi `AttributeError` khi gọi `.get()`.
  2. `tcbs_eod_job.py` truyền `index_code` dạng integer (`1`) vào `get_ticker_commons(index=index_code)`, khiến `TCBSConstants.index_code` lỗi vì chỉ nhận chuỗi hoa `"HOSE"`.
- **Khắc phục:** Bóc tách an toàn `.get("data", [])` cho cả trường hợp API trả về `dict` lẫn `list`; nâng cấp `TCBSConstants.index_code` để hỗ trợ cả `int`, `str` số và mã chuỗi.
- **Files:** [`cli/jobs/tcbs_eod_job.py`](dataops_batch_api/cli/jobs/tcbs_eod_job.py), [`cli/jobs/tcbs_bsa_job.py`](dataops_batch_api/cli/jobs/tcbs_bsa_job.py), [`cli/flows/loader/tcbs_constants_loader.py`](dataops_batch_api/cli/flows/loader/tcbs_constants_loader.py).

### [Bug 6] Sai lệch trường so sánh trong rolling retention `binance_4h_job`
- **Hiện tượng:** `apply_rolling_retention(path, date_col="year", keep_days=365)` so sánh chuỗi năm `"2026"` với mốc ngày `"YYYY-MM-DD"`.
- **Khắc phục:** Chuyển `date_col="ingested_at"` (chuỗi ISO8601 UTC) để so sánh thời gian chính xác theo khung 365 ngày.
- **Files:** [`cli/jobs/binance_4h_job.py`](dataops_batch_api/cli/jobs/binance_4h_job.py).

### [Bug 7] Dockerfile CMD cũ
- **Hiện tượng:** `Dockerfile` dòng 49 chỉ định chạy `cli/flows/serving/daily_pipeline.py` (file Prefect cũ đã bị xóa).
- **Khắc phục:** Cập nhật `CMD ["python", "grpc_server.py"]` để chạy gRPC service phục vụ backend API.
- **Files:** [`dataops_batch_api/Dockerfile`](dataops_batch_api/Dockerfile).

---

## 3. DANH SÁCH FILES THAY ĐỔI VÀ TẠO MỚI

| Trạng thái | File | Nội dung thay đổi |
|---|---|---|
| **Modified** | `dataops_batch_api/cli/flows/loader/tcbs_constants_loader.py` | Thêm `_find_yaml_path()`, nâng cấp `index_code` hỗ trợ `int/str` |
| **Modified** | `dataops_batch_api/cli/flows/loader/binance_constants_loader.py` | Thêm `_find_yaml_path()` tìm đúng file YAML |
| **Created** | `dataops_batch_api/cli/ingestion/sources/tcbs_constants_loader.py` | Re-export `TCBSConstants` backward-compat |
| **Created** | `dataops_batch_api/cli/ingestion/sources/binance_constants_loader.py` | Re-export `BinanceConstants` backward-compat |
| **Created** | `dataops_batch_api/cli/ingestion/sources/binance_constants.py` | Re-export `BinanceConstants` backward-compat |
| **Modified** | `dataops_batch_api/cli/ingestion/sources/client/tcbs_client/auth.py` | Fix `authenticate(self, otp="")`, tái sử dụng token |
| **Modified** | `dataops_batch_api/cli/jobs/__init__.py` | Export đầy đủ các submodules `_delta_writer`, `binance_*`, `tcbs_*` |
| **Modified** | `dataops_batch_api/cli/jobs/_delta_writer.py` | Dọn dẹp unused import `pyarrow.parquet` |
| **Modified** | `dataops_batch_api/cli/jobs/binance_4h_job.py` | Sửa retention `date_col="ingested_at"` |
| **Modified** | `dataops_batch_api/cli/jobs/tcbs_eod_job.py` | Parse an toàn `.get("data", [])` từ response `get_ticker_commons` |
| **Modified** | `dataops_batch_api/cli/jobs/tcbs_bsa_job.py` | Parse an toàn `.get("data", [])` từ response `get_intraday_bsa` |
| **Modified** | `dataops_batch_api/cli/flows/jobs/__init__.py` | Re-export từ `cli.jobs` |
| **Modified** | `dataops_batch_api/cli/flows/scheduler.py` | Re-export từ `cli.scheduler` |
| **Modified** | `dataops_batch_api/Dockerfile` | Update `CMD ["python", "grpc_server.py"]` |

---

## 4. KẾT QUẢ KIỂM THỬ & VALIDATION

1. **Python Compilation (100% Passed):**
   ```powershell
   python -m py_compile $(Get-ChildItem -Path dataops_batch_api -Recurse -Filter *.py | ForEach-Object { $_.FullName })
   ```
   -> Toàn bộ codebase biên dịch không có lỗi cú pháp.

2. **Integration & Initialization Verification:**
   - Đã kiểm tra khởi tạo và liên kết thành công 18 module chính trong `dataops_batch_api`:
     * Config: `load_exchange_config()`
     * Constants: `TCBSConstants`, `BinanceConstants`
     * Clients: `TCBSRestClient`, `BinanceSpotClient`, `BinanceFuturesClient`
     * Pydantic Contracts: `BinanceCandleRecord`, `TCBSMarketRecord`, `TCBSBSARecord`, `TCBSAccountCashRecord`, `TCBSOrderRecord`, `TCBSOpenPositionRecord`
     * Jobs: `binance_1d_job`, `binance_4h_job`, `tcbs_bsa_job`, `tcbs_eod_job`, `metrics`
     * Scheduler: `build_scheduler()` với 4 cron triggers
     * gRPC: `grpc_server.py` (`HistoricalDataServicer`, `grpc.health.v1`)

3. **Unit Tests Suite ([`tests/test_delta_writer.py`](dataops_batch_api/tests/test_delta_writer.py)):**
   - **13/13 tests PASSED (100% OK, 0 failures, 0 errors)**
   - Các nhóm test: `TestTablePath`, `TestStorageOptions`, `TestWriteRecords`, `TestApplyRollingRetention`, `TestSchemaEnforcement`.

---

## 5. TRẠNG THÁI CUỐI PHIÊN

- Module `dataops_batch_api` đã sạch lỗi import, pathing và parse dữ liệu.
- Sẵn sàng tích hợp với container runtime và kết nối gRPC từ `dataops_backend_api`.
