# Session Notes — dataops_batch_api Refactor & Code Quality

> **Dự án:** DataOps Backend Financial Trading  
> **Branch:** `25092026_set_up_base_code`  
> **Scope:** `dataops_batch_api/` — toàn bộ Python batch ingestion module  
> **Mục tiêu:** Refactor cấu trúc thư mục theo pattern HomeCredit + code quality review + fix adhoc logging

---

## 1. CÔNG VIỆC ĐÃ THỰC HIỆN

### 1.1 Refactor cấu trúc thư mục

**Trước (flat structure):**
```
dataops_batch_api/
├── delta/schema.py + writer.py
├── indicators/params.py + calculator.py + processor.py
├── pipeline/ingestion.py + spark_jobs.py
└── sources/okx_client.py
```

**Sau (theo pattern HomeCredit):**
```
dataops_batch_api/
├── config.py                              ← Single source of truth
├── .env.example
├── requirements.txt
├── cli/
│   ├── flows/
│   │   ├── config/
│   │   │   └── exchange_config.yaml       ← Pipeline config (renamed từ okx_config.yaml)
│   │   └── serving/
│   │       ├── metadata.py                ← Config loader helpers
│   │       ├── bronze_flow.py             ← T1: OKX/VPS REST → Delta Bronze
│   │       ├── silver_flow.py             ← T3: PySpark VWAP + Z-Score streaming
│   │       ├── backfill_flow.py           ← Historical backfill (tách riêng)
│   │       └── daily_pipeline.py          ← Master orchestrator
│   └── ingestion/
│       ├── sources/okx_client.py + vps_client.py
│       ├── indicators/params.py + calculator.py + processor.py
│       └── schema/models.py
├── platforms/
│   ├── config/stack.yaml
│   ├── processing/
│   │   ├── spark_stack/spark_session.py   ← SparkSession factory (tách ra)
│   │   └── config/delta_utils.py          ← DeltaBronzeWriter
│   └── prefect_orchestra/prefect_main.py
├── log/
│   ├── config/logger_config.yaml
│   └── log_storage/
└── infra_okx/Dockerfile
```

---

### 1.2 Các file đã tạo mới

| File | Mục đích |
|---|---|
| `config.py` | Single source of truth — `Paths`, `StorageOptions`, `_YAMLLoader` |
| `cli/flows/config/exchange_config.yaml` | Pipeline config: instruments, intervals, Kafka topics, Delta paths, window params |
| `platforms/config/stack.yaml` | Spark + MinIO config |
| `log/config/logger_config.yaml` | Logger routing per module (RotatingFileHandler + JSON format) |
| `cli/flows/serving/metadata.py` | Helper functions: `get_okx_cfg()`, `get_vps_cfg()`, `setup_logging()` |
| `cli/flows/serving/daily_pipeline.py` | Master flow: Step 1 OKX SPOT → Step 2 OKX SWAP → Step 3 VPS HOSE |
| `platforms/prefect_orchestra/prefect_main.py` | Prefect flow definitions + `PrefectETLPipelineConfig` |
| `infra_okx/Dockerfile` | Python 3.11 + Java 17 multi-stage build |
| `.env.example` | Template env vars (OKX, MinIO, Kafka, Spark) |

---

### 1.3 Rename file config

`cli/flows/config/okx_config.yaml` → `cli/flows/config/exchange_config.yaml`

- Lý do: tên `exchange_config` phản ánh đúng hơn — config cho cả OKX lẫn VPS
- Cập nhật tất cả 16 references (docstring + code) trên 7 files

---

### 1.4 Code Quality Review — 9 Issues đã fix

#### Fix 1 — `config.py`: `_YAMLLoader._load()` thiếu try/except
**Trước:** `open(path)` không guard — crash im lặng nếu file không tồn tại  
**Sau:** Raise `FileNotFoundError` + log ERROR (fail-fast, không return `{}` âm thầm)

```python
# Sau
if not path.exists():
    _log.error("Config file not found: %s", path)
    raise FileNotFoundError(f"Required config file missing: {path}")
try:
    content = yaml.safe_load(f) or {}
    _log.debug("Loaded config: %s (%d top-level keys)", path, len(content))
except yaml.YAMLError as exc:
    _log.error("Malformed YAML in %s: %s", path, exc)
    raise
```

#### Fix 2 — `config.py`: `StorageOptions` hardcode `"us-east-1"`
**Trước:** `"AWS_REGION": "us-east-1"` hardcode trong code  
**Sau:** Đọc từ `exchange_config.yaml` section `storage.aws_region`, env override

```yaml
# exchange_config.yaml
storage:
  aws_region: "us-east-1"
  allow_http: "true"
  allow_unsafe_rename: "true"
```

#### Fix 3 — `delta_utils.py`: logger name sai + thiếu try/except
**Trước:** `logger = logging.getLogger("okx-batch-ingestion")` — không match module  
**Sau:** `logger = logging.getLogger("delta_utils")`

`write_okx_candles`, `write_vps_snapshots`, `compact_table` đều bọc trong `try/except` → log ERROR + re-raise

#### Fix 4 — `vps_client.py`: ad-hoc import trong except block
**Trước:**
```python
except Exception as exc:
    import logging as _log_mod          # ← ad-hoc import trong except
    _log_mod.getLogger(__name__).warning(...)
```
**Sau:** Module-level `logger = logging.getLogger(__name__)` + xóa inline import

#### Fix 5 — `bronze_flow.py`: `run_vps_bronze()` thiếu guard
**Trước:** `get_stock_list()` và `get_market_data()` không có try/except → crash toàn flow  
**Sau:** Mỗi call có try/except riêng → return 0 thay vì unhandled exception

#### Fix 6 — `silver_flow.py`: `writeStream.start()` không có try/except
**Trước:** Streaming query start không được guard → Spark exception unhandled  
**Sau:** Bọc trong try/except → log ERROR + re-raise

Fix thêm: `delta_path` hardcode `"s3a://okx-datalake/delta/silver/vwap"` → đọc từ `delta.silver.vwap` trong yaml với env override priority

#### Fix 7 — `processor.py`: không có logger, không có try/except
**Trước:** `process()` không log gì, exception propagate không trace được  
**Sau:**
- `logger = logging.getLogger(__name__)` module-level
- DEBUG log khi skip (< 5 rows) và khi done
- try/except bao toàn bộ computation → log ERROR + re-raise

#### Fix 8 — `spark_session.py`: swallow exception im lặng
**Trước:** `except Exception: stack = {}` — không log, không biết lý do fail  
**Sau:** Tách thành `FileNotFoundError` (WARNING, expected) và `Exception` (ERROR, unexpected)

#### Fix 9 — `backfill_flow.py`: thiếu log + thiếu try/except cho write
**Trước:** `if not raw: break` im lặng; `write_okx_candles()` không có try/except  
**Sau:**
- Log INFO khi empty response (biết cursor dừng ở đâu)
- `write_okx_candles()` trong try/except → log + tiếp tục pagination thay vì abort instrument

---

### 1.5 `okx_client.py` — Thêm logger + try/except + retry hook

**Trước:** Không có logger, không có try/except trong HTTP methods  
**Sau:**

```python
# Module-level logger
logger = logging.getLogger(__name__)

# Tenacity retry callback — log WARNING mỗi lần retry
def _log_retry(retry_state: RetryCallState) -> None:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    logger.warning(
        "OKXRestClient retry attempt %d — fn=%s, error=%s",
        retry_state.attempt_number,
        retry_state.fn.__name__ if retry_state.fn else "unknown",
        exc,
    )

# Decorator dùng after=_log_retry
@retry(stop=stop_after_attempt(5), wait=wait_exponential(...), reraise=True, after=_log_retry)
def get_candles(...):
    logger.debug("get_candles — inst_id=%s, bar=%s, limit=%d", inst_id, bar, limit)
    try:
        ...
        logger.debug("get_candles — %s returned %d candles", inst_id, len(candles))
        return candles
    except Exception as exc:
        logger.error("get_candles failed — inst_id=%s, bar=%s: %s", inst_id, bar, exc)
        raise
```

---

## 2. NGUYÊN TẮC LOGGING ĐÃ CHUẨN HÓA

### Chuẩn áp dụng thống nhất trong `dataops_batch_api/`

| Pattern | ✅ Đúng | ❌ Sai |
|---|---|---|
| Khai báo logger | `logger = logging.getLogger(__name__)` ở module level | `import logging` trong except block |
| Log format | `logger.info("msg — key=%s", value)` (`%s` formatting) | `logger.info(f"msg — key={value}")` (f-string) |
| Retry log | Tenacity `after=_log_retry` callback | Không log retry |
| Exception log | `logger.error("...", exc_info=True)` | `print(exc)` hoặc `pass` |
| Skip/empty | `logger.debug(...)` | Không log |

### Lý do dùng `%s` thay vì f-string trong logging

```python
# f-string: luôn tính string dù log level bị tắt → waste CPU
logger.debug(f"Processing {len(df)} rows")

# %s formatting: chỉ tính khi log level đủ để output
logger.debug("Processing %d rows", len(df))
```

---

## 3. KIẾN TRÚC 12-FACTOR APP ĐÃ TUÂN THỦ

| Factor | Thực hiện |
|---|---|
| **III — Config** | Mọi config đọc từ env vars qua `os.getenv()`, không hardcode |
| **IV — Backing services** | MINIO, KAFKA, VPS URLs qua `.env.example` → env |
| **XI — Logs** | `logger_config.yaml` — JSON format, RotatingFileHandler, không ghi vào file cứng trong code |
| **XII — Admin processes** | `backfill_flow.py`, `compact_table()` chạy độc lập, không trong main flow |

---

## 4. DEPENDENCY MAP SAU REFACTOR

```
daily_pipeline.py
  ├── bronze_flow.py
  │     ├── metadata.py              ← get_okx_cfg(), get_vps_cfg()
  │     ├── okx_client.py            ← logger + retry + try/except
  │     ├── vps_client.py            ← logger module-level (fixed)
  │     ├── models.py                ← Pydantic validation (không cần logger)
  │     └── delta_utils.py           ← logger "delta_utils" (fixed)
  └── metadata.setup_logging()

silver_flow.py
  ├── metadata.py                    ← get_spark_streaming_cfg(), get_delta_cfg()
  └── spark_session.py               ← try/except tách FileNotFoundError vs Exception

backfill_flow.py
  ├── okx_client.py
  └── delta_utils.py

platforms/prefect_orchestra/prefect_main.py
  ├── daily_pipeline.py
  └── bronze_flow.py                 ← Prefect tasks wrap run_okx_bronze/run_vps_bronze

config.py (Paths, StorageOptions, _YAMLLoader)
  └── exchange_config.yaml           ← Single source of truth cho tất cả config
```

---

## 5. TỒN ĐỌNG / VIỆC CHƯA LÀM

| Hạng mục | Lý do chưa làm | Priority |
|---|---|---|
| `infra_okx/Dockerfile` bị xóa externally | Cần tạo lại nếu cần build image | 🟡 Medium |
| `calculator.py` chưa có logger | Pure math functions — không có I/O, không cần log production; chỉ cần trong test | 🟢 Low |
| `metadata.py` dùng `sys.path.insert()` | Workaround cho monorepo — cần `pyproject.toml` khi packaging | 🟢 Low |
| `prefect` chưa có trong `requirements.txt` | Optional dependency — thêm khi muốn dùng Prefect server | 🟡 Medium |
| Test cases cho `calculator.py` và `models.py` | Cần pytest suite — chưa có test file nào | 🔴 High |

---

## 6. LỆNH KIỂM TRA NHANH

```bash
# Syntax check toàn bộ Python files
cd DataOps_Backend_Financial_Trading
python -c "
import os, ast, sys
root = 'dataops_batch_api'
errors = []
for d, _, fs in os.walk(root):
    for f in fs:
        if f.endswith('.py'):
            p = os.path.join(d, f)
            try:
                ast.parse(open(p, encoding='utf-8').read())
            except SyntaxError as e:
                errors.append(f'{p}: {e}')
print(f'All {sum(1 for d,_,fs in os.walk(root) for f in fs if f.endswith(\".py\"))} files OK' if not errors else '\n'.join(errors))
"

# Kiểm tra không còn ad-hoc logging import
grep -rn "import logging as" dataops_batch_api/

# Kiểm tra không còn f-string trong logger calls
grep -rn "logger\.\(info\|debug\|warning\|error\)(f\"" dataops_batch_api/

# Kiểm tra tất cả module có logger module-level
grep -rn "^logger = logging.getLogger" dataops_batch_api/cli/
```

---

## 7. FILES ĐÃ VERIFY SYNTAX PASS

**Kết quả cuối session:** `✓ All 30 Python files pass syntax check`
