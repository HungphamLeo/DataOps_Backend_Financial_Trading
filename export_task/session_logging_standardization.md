# Session Notes — Logging Standardization & Logger Manager Setup

> **Dự án:** DataOps Backend Financial Trading
> **Branch:** `25092026_set_up_base_code`
> **Scope:** `dataops_batch_api/` — toàn bộ logging layer
> **Mục tiêu:** Tạo Head Logger Manager (`logger_setup.py`), chuẩn hóa logging trong tất cả modules, xóa ad-hoc imports và f-string violations.

---

## 1. VẤN ĐỀ PHÁT HIỆN TRƯỚC SESSION

| File | Vấn đề |
|---|---|
| `cli/ingestion/sources/vps_client.py` | Ad-hoc `import logging as _log_mod` trong `except` block; `get_stock_list()` không có try/except; `@retry` không có `after=_log_retry` callback |
| `cli/ingestion/sources/okx_client.py` | Dùng `logging.getLogger(__name__)` — không match YAML logger name do `__name__` phụ thuộc import context |
| `cli/ingestion/sources/vps_client.py` | Như trên — `__name__` không ổn định |
| `platforms/processing/spark_stack/spark_session.py` | `import logging as _log_mod` ad-hoc trong 2 `except` blocks |
| `cli/flows/serving/bronze_flow.py` | 7 f-string violations trong logger calls |
| `cli/flows/serving/backfill_flow.py` | 5 f-string violations trong logger calls |
| `cli/flows/serving/daily_pipeline.py` | 7 f-string violations trong logger calls |
| `platforms/prefect_orchestra/prefect_main.py` | 2 f-string violations trong logger calls |
| `cli/flows/serving/metadata.py` | `setup_logging()` tự gọi `logging.config.dictConfig()` — không delegate về 1 entry point |
| `log/config/logger_config.yaml` | Thiếu logger entries cho `okx_client`, `vps_client`, `spark_session`, `prefect_main` và nhiều modules khác |
| `log/logger_setup.py` | **Chưa tồn tại** — cần tạo mới làm Head Logger Manager |

---

## 2. FILE ĐÃ TẠO MỚI

### `dataops_batch_api/log/logger_setup.py`

**Head Logger Manager** — entry point duy nhất cho logging của toàn bộ pipeline.

**3 public symbols:**
- `setup_pipeline_logging(config_path, log_storage)` — gọi 1 lần tại entrypoint
- `get_logger(name)` — convenience alias cho `logging.getLogger(name)`
- `_bootstrap_fallback()` — private, console-only fallback khi YAML không load được
- `_patch_file_handler_paths(cfg, log_storage)` — private, convert relative filename → absolute path

**Tính năng quan trọng:**
```python
# 1. Tự tạo thư mục log_storage nếu chưa tồn tại
resolved_storage.mkdir(parents=True, exist_ok=True)

# 2. Patch filename handlers sang absolute path — không phụ thuộc working directory
# YAML khai báo: filename: "log/log_storage/pipeline.log"
# Sau patch:     filename: "/abs/path/to/dataops_batch_api/log/log_storage/pipeline.log"

# 3. Graceful fallback — KHÔNG crash pipeline nếu YAML lỗi
except Exception as exc:
    _bootstrap_fallback()   # console-only, level INFO
    _log.warning("Failed to load logger_config.yaml: %s", exc)
```

**Usage pattern:**
```python
# Entrypoint (gọi 1 lần duy nhất):
from log.logger_setup import setup_pipeline_logging
setup_pipeline_logging()

# Mọi module khác (chỉ getLogger, không setup):
import logging
logger = logging.getLogger("tên_module")
logger.info("msg — key=%s", value)   # %s format, KHÔNG f-string
```

---

## 3. FILES ĐÃ SỬA

### 3.1 `log/config/logger_config.yaml` — Từ 5 → 15 loggers

**Trước:** Chỉ có 5 loggers (`okx-batch-ingestion`, `bronze_flow`, `silver_flow`, `backfill_flow`, `daily_pipeline`). Entry `okx-batch-ingestion` không match tên logger nào trong code.

**Sau:** 15 loggers đủ cho mọi module trong pipeline:

| Logger Name | Level | Handlers | Module |
|---|---|---|---|
| `bronze_flow` | INFO | console + pipeline + errors | `bronze_flow.py` |
| `okx_client` | DEBUG | console + pipeline + errors | `okx_client.py` |
| `vps_client` | DEBUG | console + pipeline + errors | `vps_client.py` |
| `delta_utils` | INFO | console + pipeline + errors | `delta_utils.py` |
| `silver_flow` | INFO | console + pipeline + errors | `silver_flow.py` |
| `backfill_flow` | INFO | console + pipeline | `backfill_flow.py` |
| `daily_pipeline` | INFO | console + pipeline + errors | `daily_pipeline.py` |
| `prefect_main` | INFO | console + pipeline + errors | `prefect_main.py` |
| `dataops_batch_api.cli.ingestion.schema.models` | WARNING | console + pipeline + errors | `models.py` |
| `dataops_batch_api.platforms.processing.spark_stack.spark_session` | INFO | console + pipeline + errors | `spark_session.py` |
| `dataops_batch_api.config` | INFO | console + pipeline | `config.py` |
| `dataops_batch_api.cli.flows.serving.metadata` | INFO | console + pipeline | `metadata.py` |
| `dataops_batch_api.log.logger_setup` | DEBUG | console | `logger_setup.py` |
| `dataops_batch_api.cli.ingestion.indicators.processor` | WARNING | console + errors | `processor.py` |
| `dataops_batch_api.cli.ingestion.indicators.calculator` | WARNING | console | `calculator.py` |

**Lý do `backfill_flow` không có `file_errors`:**
> Backfill thất bại ở instrument level là **expected** và sẽ retry. Không cần alert riêng. Lỗi đã được log trong `bronze_flow` và `okx_client` handlers.

### 3.2 `cli/ingestion/sources/okx_client.py`

```python
# TRƯỚC — __name__ không ổn định, phụ thuộc import context
logger = logging.getLogger(__name__)

# SAU — short named logger, luôn match YAML entry "okx_client"
logger = logging.getLogger("okx_client")
```

Thêm cho `vps_client.py`:
- Module-level `logger = logging.getLogger("vps_client")`
- Module-level `_log_retry` callback
- `get_stock_list()`: thêm try/except + DEBUG log trước/sau request + ERROR log khi fail
- `get_market_data()`: xóa ad-hoc `import logging as _log_mod` trong except block
- Constructor: thêm `logger.debug("VPSRestClient initialized ...")`
- `@retry` decorator: thêm `after=_log_retry`

### 3.3 `platforms/processing/spark_stack/spark_session.py`

```python
# TRƯỚC — import trong except block (anti-pattern)
except FileNotFoundError as exc:
    import logging as _log_mod
    _log_mod.getLogger(__name__).warning(...)
except Exception as exc:
    import logging as _log_mod
    _log_mod.getLogger(__name__).error(...)

# SAU — module-level logger
import logging
logger = logging.getLogger(__name__)
...
except FileNotFoundError as exc:
    logger.warning("stack.yaml not found, falling back to env vars only: %s", exc)
except Exception as exc:
    logger.error("Unexpected error loading stack.yaml: %s", exc, exc_info=True)
```

> **Note:** `spark_session.py` dùng `__name__` (full path) vì đây là module nằm sâu trong package hierarchy và được import đúng như package — YAML entry dùng full path `dataops_batch_api.platforms.processing.spark_stack.spark_session`.

### 3.4 `cli/flows/serving/metadata.py`

```python
# TRƯỚC — tự xử lý logging config trực tiếp
def setup_logging() -> None:
    Paths.LOG_STORAGE.mkdir(parents=True, exist_ok=True)
    try:
        cfg = loader.logger_config()
        logging.config.dictConfig(cfg)
    except Exception as e:
        logging.basicConfig(...)
        logging.getLogger(__name__).warning(f"Logger config load failed: {e}")  # f-string!

# SAU — delegate hoàn toàn về logger_setup
def setup_logging() -> None:
    from log.logger_setup import setup_pipeline_logging
    setup_pipeline_logging()
```

### 3.5 `cli/flows/serving/bronze_flow.py` — F-string fixes

```python
# TRƯỚC
logger.info(f"[bronze_flow:okx] Starting {inst_type} ingestion — {len(inst_ids)} instruments, bar={bar}")
logger.warning(f"[bronze_flow:okx] No data returned for {inst_id}")
logger.warning(f"[bronze_flow:okx] Validation failed {inst_id}: {e}")
logger.info(f"[bronze_flow:okx] {inst_id}: wrote {n} records")
logger.error(f"[bronze_flow:okx] Error fetching {inst_id}: {e}", exc_info=True)
logger.info(f"[bronze_flow:okx] Done — total {total} records written")

# SAU
logger.info("[bronze_flow:okx] Starting %s ingestion — %d instruments, bar=%s", inst_type, len(inst_ids), bar)
logger.warning("[bronze_flow:okx] No data returned for %s", inst_id)
logger.warning("[bronze_flow:okx] Validation failed %s: %s", inst_id, e)
logger.info("[bronze_flow:okx] %s: wrote %d records", inst_id, n)
logger.error("[bronze_flow:okx] Error fetching %s: %s", inst_id, e, exc_info=True)
logger.info("[bronze_flow:okx] Done — total %d records written", total)
```

### 3.6 `cli/flows/serving/backfill_flow.py` — F-string fixes

```python
# TRƯỚC
logger.error(f"[backfill] get_candles failed for {inst_id}: {e}")
logger.warning(f"[backfill] Validation failed {inst_id} ts={int(row[0])}: {e}")
logger.info(f"[backfill] Starting {inst_type} backfill: {len(inst_ids)} instruments, ...")
logger.info(f"[backfill] ✓ {inst_id}: {count:,} records")
logger.error(f"[backfill] ✗ {inst_id}: {e}", exc_info=True)
logger.info(f"[backfill] Complete — total {total_records:,} records written")

# SAU
logger.error("[backfill] get_candles failed for %s: %s", inst_id, e)
logger.warning("[backfill] Validation failed %s ts=%d: %s", inst_id, int(row[0]), e)
logger.info("[backfill] Starting %s backfill: %d instruments, %d days back, bar=%s, workers=%d", ...)
logger.info("[backfill] OK %s: %d records", inst_id, count)
logger.error("[backfill] FAIL %s: %s", inst_id, e, exc_info=True)
logger.info("[backfill] Complete — total %d records written", total_records)
```

### 3.7 `cli/flows/serving/daily_pipeline.py` — F-string fixes

```python
# TRƯỚC
logger.info(f"=== daily_pipeline started at {started_at} ===")
logger.info(f"[daily_pipeline] Step 1 done: {n} SPOT records")
logger.error(f"[daily_pipeline] Step 1 failed: {e}", exc_info=True)
# ... (7 violations tổng)

# SAU
logger.info("=== daily_pipeline started at %s ===", started_at)
logger.info("[daily_pipeline] Step 1 done: %d SPOT records", n)
logger.error("[daily_pipeline] Step 1 failed: %s", e, exc_info=True)
logger.info("=== daily_pipeline complete: %d total records, duration=%.2fs ===", total, duration)
```

### 3.8 `platforms/prefect_orchestra/prefect_main.py` — F-string fixes

```python
# TRƯỚC
logger.info(f"[prefect] daily_bronze_flow started at {datetime.now(timezone.utc).isoformat()}")
logger.info(f"[prefect] daily_bronze_flow done: spot={spot_n}, swap={swap_n}")

# SAU
logger.info("[prefect] daily_bronze_flow started at %s", datetime.now(timezone.utc).isoformat())
logger.info("[prefect] daily_bronze_flow done: spot=%d, swap=%d", spot_n, swap_n)
```

---

## 4. NGUYÊN TẮC NAMING CONVENTION CHO LOGGER

### Short Named Logger (ưu tiên dùng cho ETL flow modules)

Dùng khi module có thể được import từ nhiều contexts khác nhau hoặc `__name__` không ổn định:

```python
# Flow modules
logger = logging.getLogger("bronze_flow")
logger = logging.getLogger("silver_flow")
logger = logging.getLogger("backfill_flow")
logger = logging.getLogger("daily_pipeline")
logger = logging.getLogger("prefect_main")

# Source clients — quan trọng: short name tránh full-path mismatch
logger = logging.getLogger("okx_client")
logger = logging.getLogger("vps_client")

# Infrastructure
logger = logging.getLogger("delta_utils")
```

### Full Path Logger (dùng cho platform/config modules)

Dùng khi module luôn được import như Python package với path cố định:

```python
# Platform modules
logger = logging.getLogger(__name__)
# → dataops_batch_api.platforms.processing.spark_stack.spark_session
# → dataops_batch_api.config
# → dataops_batch_api.cli.flows.serving.metadata
# → dataops_batch_api.cli.ingestion.indicators.processor
```

### Quy tắc quyết định

| Nếu module... | Dùng |
|---|---|
| Là ETL flow, source client, writer | `logging.getLogger("tên_ngắn")` |
| Là platform util, config, pure logic | `logging.getLogger(__name__)` |
| Không chắc | Short name — an toàn hơn, dễ match YAML |

---

## 5. CHUẨN LOGGING ĐÃ ÁP DỤNG TOÀN BỘ `dataops_batch_api/`

| Pattern | ✅ Đúng | ❌ Sai |
|---|---|---|
| Khai báo logger | Module-level `logger = logging.getLogger("name")` | `import logging as _log_mod` trong except |
| Format string | `logger.info("msg — key=%s", value)` | `logger.info(f"msg — key={value}")` |
| Retry log | Tenacity `after=_log_retry` callback | Không log retry |
| Exception | `logger.error("...", exc_info=True)` | `print(exc)` hoặc `pass` |
| Setup logging | `from log.logger_setup import setup_pipeline_logging` tại entrypoint | `logging.basicConfig()` rải rác |

**Lý do dùng `%s` thay f-string:**
```python
# f-string: Python LUÔN tính toán string dù log level bị tắt
logger.debug(f"Processing {len(df)} rows")  # len(df) chạy dù DEBUG bị tắt

# %s formatting: chỉ tính khi log level đủ để emit
logger.debug("Processing %d rows", len(df))  # len(df) không chạy nếu DEBUG off
```

---

## 6. FLOW DIAGRAM — LOGGING INITIALIZATION

```
Entrypoint (daily_pipeline/__main__, prefect_main/__main__, backfill/__main__)
    │
    ▼
metadata.setup_logging()
    │
    ▼
log.logger_setup.setup_pipeline_logging()
    │
    ├── mkdir log/log_storage/
    ├── load log/config/logger_config.yaml
    ├── _patch_file_handler_paths()  ← relative → absolute filename
    └── logging.config.dictConfig(cfg)
            │
            ▼
    Python Logging Registry
    ├── "bronze_flow"     → console + pipeline.log + errors.log
    ├── "okx_client"      → console + pipeline.log + errors.log (DEBUG)
    ├── "vps_client"      → console + pipeline.log + errors.log (DEBUG)
    ├── "delta_utils"     → console + pipeline.log + errors.log
    ├── "silver_flow"     → console + pipeline.log + errors.log
    ├── "backfill_flow"   → console + pipeline.log
    ├── "daily_pipeline"  → console + pipeline.log + errors.log
    ├── "prefect_main"    → console + pipeline.log + errors.log
    └── root              → console (WARNING+ only)
```

---

## 7. VERIFY COMMANDS

```bash
# Syntax check toàn bộ Python files
python -c "
import os, ast, sys
root = 'dataops_batch_api'
ok, errors = 0, []
for d, _, fs in os.walk(root):
    for f in fs:
        if not f.endswith('.py'): continue
        p = os.path.join(d, f)
        try:
            ast.parse(open(p, encoding='utf-8').read())
            ok += 1
        except SyntaxError as e:
            errors.append('%s: %s' % (p, e))
print('ALL %d files pass' % ok if not errors else '\n'.join(errors))
"
# Kết quả cuối session: ALL 31 Python files pass

# Check không còn f-string trong logger calls (PowerShell)
Get-ChildItem -Path "dataops_batch_api" -Recurse -Filter "*.py" |
    Select-String -Pattern 'logger\.(info|debug|warning|error|critical)\(f"' |
    Select-Object Filename, LineNumber, Line
# Kết quả mong muốn: không có output (0 violations)

# Check không còn ad-hoc import logging (PowerShell)
Get-ChildItem -Path "dataops_batch_api" -Recurse -Filter "*.py" |
    Select-String -Pattern "import logging as" |
    Select-Object Filename, LineNumber, Line
# Kết quả mong muốn: không có output

# Validate YAML + đếm loggers
python -c "
import yaml
cfg = yaml.safe_load(open('dataops_batch_api/log/config/logger_config.yaml', encoding='utf-8'))
print('YAML OK —', len(cfg['loggers']), 'loggers:', list(cfg['loggers'].keys()))
"
# Kết quả: YAML OK — 15 loggers
```

---

## 8. KẾT QUẢ CUỐI SESSION

| Metric | Trước | Sau |
|---|---|---|
| Python files pass syntax | 30 | **31** (thêm `logger_setup.py`) |
| F-string logger violations | ~21 | **0** |
| Ad-hoc `import logging as` | 3 | **0** |
| Loggers trong YAML | 5 | **15** |
| Entry point logging setup | Rải rác trong metadata.py | **1 file** (`logger_setup.py`) |
| Short named loggers trong sources | 0 | **2** (`okx_client`, `vps_client`) |

---

## 9. TỒN ĐỌNG / VIỆC TIẾP THEO

| Hạng mục | Priority | Ghi chú |
|---|---|---|
| Test cases cho `calculator.py` và `models.py` | 🔴 High | Vẫn chưa có pytest suite |
| Fix `configuration/` package trong `dataops_backend_api/` | 🔴 High | `Config` redeclared — mọi file import `configuration` không build được |
| `conftest.py` cho pytest — gọi `setup_pipeline_logging()` trong test | 🟡 Medium | Đảm bảo test output có log đúng level |
| `AccountingSvc`, `IndicatorSvc` chưa vào `services.Services` struct | 🟢 Low | Chờ có REST handler |
