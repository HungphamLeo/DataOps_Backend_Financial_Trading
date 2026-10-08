# SESSION EXPORT — Batch CLI Refactor: Constants Separation, Logger Manager & Log Storage
**Date:** 2026-10-08
**Branch:** 20260710_data_batch_api
**Scope:** `dataops_batch_api/cli/ingestion/sources/` · `dataops_batch_api/log/`
**References:**
- `github/copilotinstructions/SKILL.md`
- `github/agent/batch-lakehouse-engineer.agent.md`
- `docs/DATAOPS_PLATFORM_DESIGN.md`
- `export_task/SESSION_2026_10_08_BATCH_SOURCES_TEST_AND_PREFECT_DECOUPLING.md`

---

## 1. MỤC TIÊU SESSION

Đơn giản hóa và phân tách trách nhiệm trong `dataops_batch_api/cli/` theo đúng vai trò của module theo kiến trúc platform design:

1. **Tách phần gọi HTTP ra riêng** — client chỉ chứa HTTP logic + retry + JWT auth.
2. **Tách struct/constants ra riêng** — `BinanceConstants`, `TCBSConstants` mỗi class một file, chỉ load YAML.
3. **Chuẩn hóa Logger** — toàn bộ module dùng `logger_manager.get_logger(__name__)` từ `log/logger_setup.py`, tuyệt đối không dùng `import logging` trực tiếp.
4. **Chuẩn hóa Log Storage** — phân tách file log rõ ràng theo component, chuẩn production.

---

## 2. QUYẾT ĐỊNH KIẾN TRÚC

### [D1] Phân tách Constants ra file riêng
- `binance_constants.py` (mới): class `BinanceConstants` — load `binance_constants.yaml`, expose accessor methods (`base_url`, `public_endpoint`, `kline_field_index`, `rate_limit`, validators).
- `tcbs_constants_loader.py` (mới): class `TCBSConstants` — tách ra từ `tcbs_client.py`, load `tcbs_constants.yaml`, expose `endpoint`, `index_code`, validators, `raw`.
- `tcbs_client.py` re-export `TCBSConstants` qua `__all__` để backward-compatible với code cũ.

### [D2] Bắt buộc dùng LoggerManager — không import logging trực tiếp
- Tất cả module trong `dataops_batch_api` đều phải dùng pattern:
  ```python
  from log.logger_setup import logger_manager
  logger = logger_manager.get_logger(__name__)
  ```
- Không được dùng `logger = logging.getLogger(__name__)` ở bất kỳ module nào.
- `LoggerManager` singleton đảm bảo load `logger_config.yaml` một lần duy nhất, fallback console-only nếu YAML lỗi.

### [D3] Log Storage phân tách chuẩn production
- File log phân tách theo component trong `log/log_storage/` (RotatingFileHandler, JSON format):

| File | Nội dung | Level |
|---|---|---|
| `application.log` | Tất cả events toàn pipeline | DEBUG+ |
| `cli.log` | Tất cả flows + ingestion sources + indicators | DEBUG |
| `tcbs.log` | TCBS client + constants loader (isolate debug) | DEBUG |
| `error.log` | ERROR+ toàn hệ thống | ERROR |
| `platforms.log` | Platform services | INFO |
| `processing.log` | Delta utils, Spark config | INFO |
| `spark.log` | SparkSession, streaming queries | INFO |
| `orchestration.log` | Prefect flows, daily pipeline | INFO |

---

## 3. CHI TIẾT FILES TẠO MỚI / CHỈNH SỬA

### 3.1 Files tạo mới

#### `dataops_batch_api/cli/ingestion/sources/binance_constants.py`
```
BinanceConstants class:
  - Singleton — load binance_constants.yaml một lần
  - base_url(market)               → str, e.g. "https://api.binance.com/api/v3"
  - public_endpoint(name)          → str, e.g. "/klines"
  - private_endpoint(name)         → str
  - kline_field_index(field)       → int, e.g. quote_asset_volume → 7
  - rate_limit(key)                → Any
  - validate_interval(interval)    → str, raises ValueError nếu invalid
  - validate_instrument_type(t)    → str, raises ValueError nếu invalid
Logger: logger_manager.get_logger(__name__)
```

#### `dataops_batch_api/cli/ingestion/sources/tcbs_constants_loader.py`
```
TCBSConstants class (tách từ tcbs_client.py):
  - Singleton — load tcbs_constants.yaml một lần
  - endpoint(name)                      → str path
  - index_code(index_name)              → int
  - validate_price_type(price_type)     → str
  - validate_exec_type(exec_type)       → str
  - validate_bsa_investor_type(t)       → str
  - valid_session_states()              → List[str]
  - intraday_history_config()           → Dict
  - raw()                               → Dict (toàn bộ YAML)
Logger: logger_manager.get_logger(__name__)
```

#### `dataops_batch_api/log/log_storage/.gitkeep`
- Placeholder để `log/log_storage/` được commit vào git
- Runtime log files được tạo tự động bởi RotatingFileHandler

### 3.2 Files chỉnh sửa

#### `dataops_batch_api/cli/ingestion/sources/binance_client.py`
- Xóa: `import logging`, `import base64`, `from datetime import datetime, timezone`
- Thêm:
  ```python
  from log.logger_setup import logger_manager
  logger = logger_manager.get_logger(__name__)
  ```
- Logic nghiệp vụ (HTTP, retry, HMAC-SHA256 auth) giữ nguyên.

#### `dataops_batch_api/cli/ingestion/sources/tcbs_client.py`
- Xóa: `import logging`, `import yaml`, `from pathlib import Path`, toàn bộ class `TCBSConstants` inline
- Thêm:
  ```python
  from log.logger_setup import logger_manager
  from cli.ingestion.sources.tcbs_constants_loader import TCBSConstants  # noqa: F401
  logger = logger_manager.get_logger(__name__)
  __all__ = ["TCBSRestClient", "TCBSConstants"]  # backward-compatible
  ```
- Logic nghiệp vụ (HTTP, JWT auth, retry) giữ nguyên.

#### `dataops_batch_api/log/config/logger_config.yaml`
- Thêm 4 logger entries mới:
  ```yaml
  cli.ingestion.sources.tcbs_constants_loader:
    level: DEBUG
    handlers: [console, tcbs_file, cli_file, error_file]
    propagate: false

  cli.ingestion.sources.binance_client:
    level: DEBUG
    handlers: [console, cli_file, error_file]
    propagate: false

  cli.ingestion.sources.binance_constants:
    level: DEBUG
    handlers: [console, cli_file, error_file]
    propagate: false
  ```
- Giữ nguyên: `cli.ingestion.sources.tcbs_client` (đã có từ trước)

---

## 4. KIẾN TRÚC SAU KHI REFACTOR

```
dataops_batch_api/
├── cli/
│   └── ingestion/
│       └── sources/
│           ├── binance_constants.py     ← NEW: Constants loader (YAML → accessors)
│           ├── binance_constants.yaml   ← source of truth (không thay đổi)
│           ├── binance_client.py        ← REFACTORED: HTTP client, dùng logger_manager
│           ├── tcbs_constants_loader.py ← NEW: Constants loader (YAML → accessors)
│           ├── tcbs_constants.yaml      ← source of truth (không thay đổi)
│           └── tcbs_client.py          ← REFACTORED: HTTP client, import TCBSConstants từ loader
└── log/
    ├── logger_setup.py                  ← Không thay đổi
    ├── config/
    │   └── logger_config.yaml           ← UPDATED: thêm 3 logger entries mới
    └── log_storage/                     ← NEW: runtime log directory
        └── .gitkeep
```

**Luồng Logger chuẩn (áp dụng cho MỌI module mới/cũ):**
```
Module import
    │
    ▼
from log.logger_setup import logger_manager
logger = logger_manager.get_logger(__name__)
    │
    ▼
logger_config.yaml routing theo __name__ prefix hierarchy
    │
    ├── cli.ingestion.sources.tcbs_client   → tcbs.log + cli.log + error.log
    ├── cli.ingestion.sources.tcbs_constants_loader → tcbs.log + cli.log + error.log
    ├── cli.ingestion.sources.binance_client → cli.log + error.log
    ├── cli.ingestion.sources.binance_constants → cli.log + error.log
    └── cli.*                                → cli.log + error.log
```

---

## 5. QUY TẮC BẮT BUỘC (áp dụng cho toàn `dataops_batch_api`)

1. **KHÔNG ĐƯỢC** dùng `import logging` + `logging.getLogger(__name__)` ở bất kỳ module nào.
2. **BẮT BUỘC** dùng `from log.logger_setup import logger_manager` + `logger = logger_manager.get_logger(__name__)`.
3. **KHÔNG ĐƯỢC** hardcode endpoint path, enum value, field index trong source code — luôn đọc qua `BinanceConstants` hoặc `TCBSConstants`.
4. **BẮT BUỘC** mọi hàm đều có khối `try/except` với `logger.error(...)` trước khi `raise`.
5. **KHÔNG ĐƯỢC** gọi `logging.basicConfig()` ở bất kỳ nơi nào trừ `logger_setup.py` (fallback mode).

---

## 6. VALIDATION

```
python -m py_compile cli/ingestion/sources/binance_constants.py    → OK
python -m py_compile cli/ingestion/sources/tcbs_constants_loader.py → OK
python -m py_compile cli/ingestion/sources/binance_client.py       → OK
python -m py_compile cli/ingestion/sources/tcbs_client.py          → OK

Get-ChildItem -Recurse -Filter "*.py" | python -m py_compile (all) → 100% Passed
```

---

## 7. DANH SÁCH FILES TẠO MỚI / CHỈNH SỬA / XÓA

| Trạng thái | Đường dẫn file | Ghi chú |
|---|---|---|
| **Created** | `dataops_batch_api/cli/ingestion/sources/binance_constants.py` | BinanceConstants class load YAML |
| **Created** | `dataops_batch_api/cli/ingestion/sources/tcbs_constants_loader.py` | TCBSConstants tách ra từ tcbs_client.py |
| **Created** | `dataops_batch_api/log/log_storage/.gitkeep` | Placeholder cho runtime log directory |
| **Modified** | `dataops_batch_api/cli/ingestion/sources/binance_client.py` | Dùng logger_manager thay import logging |
| **Modified** | `dataops_batch_api/cli/ingestion/sources/tcbs_client.py` | Tách TCBSConstants ra, dùng logger_manager |
| **Modified** | `dataops_batch_api/log/config/logger_config.yaml` | Thêm 3 logger entries mới cho constants + binance_client |

---

## 8. BÀI HỌC KINH NGHIỆM (LESSONS LEARNED)

1. **Phân tách Constants ra file riêng là đúng hướng:** Khi `TCBSConstants` nằm trong `tcbs_client.py`, bất kỳ module nào cần dùng constants đều phải import cả `TCBSRestClient` — coupling không cần thiết. Tách ra `tcbs_constants_loader.py` và `binance_constants.py` giúp module nào cần constant thì import đúng file đó.

2. **Logger Manager Singleton > `logging.getLogger`:** Dùng trực tiếp `logging.getLogger` tạo ra risk là module được load trước khi `dictConfig` được apply — dẫn đến log không được route đúng handler. `logger_manager.get_logger()` đảm bảo `configure()` được gọi trước khi trả về logger.

3. **`__all__` + re-export để backward-compatible:** Khi tách `TCBSConstants` ra file mới, các file cũ đang `from cli.ingestion.sources.tcbs_client import TCBSConstants` vẫn hoạt động nhờ `from tcbs_constants_loader import TCBSConstants` + khai báo trong `__all__`.

4. **apply_diff với SEARCH block quá lớn có thể dẫn đến duplicate code** nếu pattern không match chính xác → nên dùng `write_file` cho các refactor lớn thay thế toàn bộ file.
