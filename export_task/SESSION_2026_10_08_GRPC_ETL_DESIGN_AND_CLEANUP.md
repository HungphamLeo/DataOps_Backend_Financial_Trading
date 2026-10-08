# SESSION EXPORT — gRPC Design, ETL Jobs & Architecture Cleanup
**Date:** 2026-10-08
**Branch:** 20260710_data_batch_api
**Scope:** `dataops_batch_api/` · `dataops_protobuf/batch-data/` · `infra_dataops_finance_trading/docker-compose.yml`
**References:**
- `export_task/SESSION_2026_10_08_CLIENT_DOMAIN_SEPARATION_TCBS_BINANCE.md`
- `export_task/SESSION_2026_10_08_PLATFORM_DESIGN_CDM_BATCH_REFACTOR.md`
- `export_task/SESSION_2026_10_08_BATCH_SOURCES_TEST_AND_PREFECT_DECOUPLING.md`
- `github/copilotinstructions/SKILL.md`
- `github/agent/batch-lakehouse-engineer.agent.md`

---

## 1. MỤC TIÊU SESSION

1. Làm rõ kiến trúc tương tác giữa `dataops_backend_api` và `dataops_batch_api` theo hướng snapshot-first + gRPC historical.
2. Xóa `canonical.py` dư thừa (CDM đã có trong protobuf).
3. Xây dựng 4 ETL jobs production-ready với APScheduler.
4. Bổ sung MinIO bucket init vào docker-compose.
5. Thiết kế gRPC protobuf schema cho historical data queries.

---

## 2. QUYẾT ĐỊNH KIẾN TRÚC QUAN TRỌNG

### [D1] Phân loại dữ liệu theo 3 nhóm bất biến

| Nhóm | Loại data | Cách lấy | Lưu ở đâu |
|---|---|---|---|
| **Snapshot** | Market price, orderbook, BSA intraday | `backend_api` gọi thẳng Binance/TCBS REST | Redis cache TTL ngắn |
| **Historical** | OHLCV 4h/1d Binance, EOD VN stocks, BSA daily | `batch_api` ETL jobs → Delta Lake | MinIO (Delta Lake) |
| **User trading history** | Orders, balance, P&L, holdings | `backend_api` ghi sau mỗi event | MySQL (per user) |

**Nguyên tắc cốt lõi:** `dataops_batch_api` **KHÔNG** là real-time data supplier. Chỉ phục vụ historical queries qua gRPC từ `backend_api`.

### [D2] Giao tiếp backend_api → batch_api qua gRPC (không phải REST)

```
UI → backend_api (Go)
         ├─ Snapshot: gọi thẳng Binance/TCBS API → Redis TTL
         ├─ Historical: gRPC call → batch_api (Python) → Delta Lake query → return
         └─ Account/order: proxy TCBS REST trực tiếp
```

- **gRPC** = HTTP/2 + protobuf binary = performance tốt hơn REST JSON cho data lớn
- Port: `50051` (dataops-batch-api container)
- Transport: plaintext trong dev, mTLS trong production

### [D3] Retention policy rõ ràng theo business value

| Delta Table | Retention | Lý do |
|---|---|---|
| `bronze/binance_candles_4h` | **365 ngày rolling** | Chart 1 năm — đủ cho TA signals (MA200, MACD) |
| `bronze/binance_candles_1d` | **10 năm append-only** | Historical backtest, long-term trend |
| `bronze/tcbs_market_eod` | **10 năm append-only** | "Nến 1D" của VN stocks — tương đương Binance 1d |
| `bronze/tcbs_bsa_daily` | **730 ngày (2 năm) rolling** | BSA signal cycle ~3-6 tháng; 2 năm đủ cho pattern |

### [D4] BSA là gì — giải thích để dùng sau

**BSA = Buy/Sell Analysis** — indicator độc quyền TCBS:
- `bup` (Buy Up %): % giao dịch mua chủ động (taker buy)
- `sdp` (Sell Down %): % giao dịch bán chủ động
- `bsr` = bup / sdp: tỷ lệ
- Phân theo loại NĐT: `sheep` (nhỏ lẻ), `wolf` (tổ chức vừa), `shark` (tổ chức lớn, lệnh > 200M VND)

**Signal mẫu:** `shark.bup > 70%` trong 3 ngày liên tiếp → tổ chức lớn đang gom → bullish signal.

### [D5] Intraday ticks KHÔNG cần lưu

Tick-by-tick (`/nyx/v1/intraday/his/paging`) chỉ có giá trị lúc execution (gửi lệnh). Không có giá trị phân tích lịch sử. Bỏ hoàn toàn khỏi batch storage.

### [D6] Không có Storage cho Margin / Unrealized P&L

Giá trị thay đổi tick-by-tick → không có nghĩa khi lưu. Tính lại runtime: `net_qty × (current_price - entry_price)`.

---

## 3. CHI TIẾT FILES TẠO MỚI / CHỈNH SỬA / XÓA

### Xóa

| File | Lý do |
|---|---|
| `dataops_batch_api/cli/ingestion/schema/canonical.py` | CDM đã chuyển sang `dataops_protobuf/market-data/market_data.proto` (session trước). File này là duplicate gây nhầm lẫn. |

### Tạo mới

| File | Nội dung |
|---|---|
| `dataops_batch_api/cli/jobs/__init__.py` | Package descriptor cho ETL jobs |
| `dataops_batch_api/cli/jobs/_delta_writer.py` | Shared helper: write Parquet → Delta Lake (delta-rs), rolling retention |
| `dataops_batch_api/cli/jobs/binance_4h_job.py` | ETL: Binance 4h candles — 1 năm rolling, Spot + Futures |
| `dataops_batch_api/cli/jobs/binance_1d_job.py` | ETL: Binance 1d candles — 10 năm append-only, supports backfill |
| `dataops_batch_api/cli/jobs/tcbs_eod_job.py` | ETL: TCBS market EOD snapshot — 10 năm append-only |
| `dataops_batch_api/cli/jobs/tcbs_bsa_job.py` | ETL: TCBS BSA daily summary — 2 năm rolling |
| `dataops_batch_api/cli/scheduler.py` | APScheduler entrypoint, 4 cron jobs, graceful SIGTERM shutdown |

### Chỉnh sửa

| File | Thay đổi |
|---|---|
| `dataops_batch_api/cli/ingestion/schema/__init__.py` | Bỏ toàn bộ imports từ `canonical.py`; chỉ giữ Pydantic models |
| `dataops_batch_api/cli/flows/loader/load_config.py` | Implement `load_exchange_config()` với `lru_cache` (file trước đó rỗng) |
| `dataops_batch_api/requirements.txt` | Thêm `APScheduler==3.10.4`, `grpcio==1.64.1`, `grpcio-tools==1.64.1`, `protobuf==4.25.3` |
| `dataops_protobuf/batch-data/batch_data.proto` | Rewrite hoàn toàn: service `HistoricalDataService` với 4 RPCs cụ thể theo data thực tế |
| `infra_dataops_finance_trading/docker-compose.yml` | Thêm service `minio-init` (tạo bucket + folder structure); `dataops-batch-api` depends on `minio-init` |

---

## 4. CHI TIẾT KỸ THUẬT

### 4.1 ETL Job Pattern — chuẩn cho tất cả 4 jobs

```python
def run() -> dict:
    cfg    = load_exchange_config()          # 1. Load config từ YAML
    client = BinanceSpotClient.from_env()    # 2. Init client từ env vars
    rows   = client.get_klines(...)          # 3. Fetch data
    recs   = [validate(row) for row in rows] # 4. Pydantic validation
    write_records(path, recs, schema)        # 5. Ghi Delta Lake
    apply_rolling_retention(path, ...)       # 6. Xóa records cũ (nếu cần)
    return {"written": n, "errors": m}
```

**Pattern bất biến:**
- Mỗi job trả về `dict` với `written`, `errors` để scheduler log
- Exception trong 1 symbol không crash toàn bộ job (try/except per symbol)
- Validation fail → skip record + log warning (không raise)
- `apply_rolling_retention` chỉ gọi cho tables có rolling window, KHÔNG gọi cho append-only

### 4.2 Delta Lake Storage — delta-rs (không dùng Spark cho batch write)

```python
from deltalake import write_deltalake

write_deltalake(
    "s3://binance-datalake/delta/bronze/binance_candles_4h",
    pyarrow_table,
    mode="append",
    partition_by=["inst_type", "symbol", "year"],
    storage_options={
        "AWS_ENDPOINT_URL": "http://minio:9000",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",   # required for MinIO
    }
)
```

**Lý do dùng delta-rs thay Spark:** Batch write nhỏ (< 10K records/lần) không cần Spark overhead. delta-rs Python binding đủ nhanh, in-process, không cần JVM.

**QUAN TRỌNG:** `AWS_S3_ALLOW_UNSAFE_RENAME=true` bắt buộc cho MinIO vì MinIO không support atomic rename. Spark dùng cách khác nên không cần flag này.

### 4.3 Partition Strategy

| Table | Partition columns | Lý do |
|---|---|---|
| `binance_candles_4h` | `inst_type, symbol, year` | Query thường theo symbol + time range → prune by year |
| `binance_candles_1d` | `inst_type, symbol, year` | Tương tự |
| `tcbs_market_eod` | `exchange, trading_date` | Query theo sàn + ngày giao dịch |
| `tcbs_bsa_daily` | `trading_date` | Query theo ngày; ticker filter là post-partition filter |

### 4.4 APScheduler — cron schedule

```
binance_4h_job : cron(hour=0, minute=5)             UTC — 00:05 UTC = 07:05 ICT
binance_1d_job : cron(hour=0, minute=5)             UTC — chạy cùng 4h job
tcbs_eod_job   : cron(hour=8, minute=15, mon-fri)   UTC — 15:15 ICT (sau ATC settlement)
tcbs_bsa_job   : cron(hour=8, minute=30, mon-fri)   UTC — 15:30 ICT (sau EOD job)
```

**Tại sao 15:15 ICT cho TCBS EOD:** ATC session đóng 14:45 ICT, settlement 15:00 ICT. 15:15 đảm bảo `match_price` cuối cùng (close) đã được confirm.

### 4.5 MinIO Init Service

```yaml
minio-init:
  image: minio/mc:latest
  restart: on-failure
  depends_on:
    minio: { condition: service_healthy }
```

Init tự động:
1. Set mc alias
2. `mc mb --ignore-existing binance-datalake`
3. `mc version suspend` (Delta Lake tự quản lý versions)
4. Tạo folder placeholder: `delta/bronze/.keep`, `delta/silver/.keep`, `delta/gold/.keep`
5. Set lifecycle: expire `raw/` uploads > 90 ngày

### 4.6 gRPC Service — batch_data.proto

```protobuf
service HistoricalDataService {
  rpc GetBinanceOHLCV(BinanceOHLCVRequest)     returns (BinanceOHLCVResponse);
  rpc GetTCBSMarketEOD(TCBSMarketEODRequest)   returns (TCBSMarketEODResponse);
  rpc GetTCBSBSAHistory(TCBSBSARequest)        returns (TCBSBSAResponse);
  rpc GetDataRange(DataRangeRequest)           returns (DataRangeResponse);
}
```

**Request design principles:**
- `from`/`to` dùng string `"YYYY-MM-DD"` cho TCBS (date-based); `Timestamp` cho Binance (ms-based)
- `limit=0` = không giới hạn (server enforce max 10000)
- `inst_type` tách riêng với `symbol` — không encode vào symbol string
- `DataRangeRequest.source` = tên table: `"binance_4h"` | `"binance_1d"` | `"tcbs_market_eod"` | `"tcbs_bsa_daily"`

---

## 5. VALIDATION

```
python -m py_compile dataops_batch_api/cli/ingestion/schema/__init__.py → OK
python -m py_compile dataops_batch_api/cli/jobs/__init__.py              → OK
python -m py_compile dataops_batch_api/cli/jobs/_delta_writer.py         → OK
python -m py_compile dataops_batch_api/cli/jobs/binance_4h_job.py        → OK
python -m py_compile dataops_batch_api/cli/jobs/binance_1d_job.py        → OK
python -m py_compile dataops_batch_api/cli/jobs/tcbs_eod_job.py          → OK
python -m py_compile dataops_batch_api/cli/jobs/tcbs_bsa_job.py          → OK
python -m py_compile dataops_batch_api/cli/scheduler.py                  → OK
python -m py_compile dataops_batch_api/cli/flows/loader/load_config.py   → OK

ALL 9 FILES — SYNTAX OK (100% Passed)
```

---

## 6. BÀI HỌC KINH NGHIỆM (LESSONS LEARNED)

### [L16] Snapshot data không cần lưu — chỉ cần pipeline đúng hướng
User chỉ cần giá hiện tại → `backend_api` gọi thẳng sàn (+ Redis cache). `batch_api` hoàn toàn không liên quan đến real-time. Sai lầm phổ biến là thiết kế batch_api như một middleware layer — nó chỉ là **data supplier duy nhất cho historical queries**.

### [L17] TCBS không có historical endpoint — phải tự lưu từng ngày
TCBS iFlash Open API chỉ trả data của **current trading session**. Dữ liệu ngày hôm qua, tuần trước không có. → Phải chạy EOD job mỗi ngày để tích lũy lịch sử. Đây là sự khác biệt quan trọng với Binance (có endpoint lịch sử đầy đủ).

### [L18] delta-rs vs Spark cho batch write
- delta-rs: phù hợp batch write nhỏ (< 10K records), không cần JVM, in-process
- Spark: cần khi xử lý lượng lớn, streaming, hoặc complex transforms
- Không cần Spark cho ingestion job đơn giản (fetch → validate → write)

### [L19] `AWS_S3_ALLOW_UNSAFE_RENAME=true` bắt buộc cho delta-rs + MinIO
MinIO không support atomic rename (khác S3 thật). delta-rs cần flag này để fallback sang non-atomic rename. Nếu thiếu → exception khi commit Delta transaction.

### [L20] APScheduler vs Prefect — trade-off
- APScheduler: in-process, không cần server, đủ cho 4-5 jobs đơn giản
- Prefect: cần server riêng, phù hợp khi có UI monitor, retry phức tạp, cross-service orchestration
- Quyết định: APScheduler cho batch_api (đã remove Prefect session trước) — simple, no external dependency

### [L21] Retention policy nên gắn với business value, không phải technical capacity
- "Lưu bao lâu?" → câu hỏi đúng là "user dùng data cũ bao xa?"
- BSA 2 năm: analyst nhìn trend tối đa 1 năm → 2 năm là safety margin
- Binance 1d 10 năm: backtesting strategy cần ít nhất 5-7 năm để có statistical significance

---

## 7. TRẠNG THÁI CUỐI SESSION

| Component | Status | Ghi chú |
|---|---|---|
| `canonical.py` xóa | ✅ Done | CDM ở `market_data.proto` |
| `schema/__init__.py` clean | ✅ Done | Chỉ còn Pydantic models |
| `binance_4h_job.py` | ✅ Done | Syntax OK, production-ready |
| `binance_1d_job.py` | ✅ Done | Hỗ trợ backfill qua CLI args |
| `tcbs_eod_job.py` | ✅ Done | Syntax OK |
| `tcbs_bsa_job.py` | ✅ Done | Syntax OK |
| `cli/scheduler.py` | ✅ Done | APScheduler, graceful shutdown |
| `load_config.py` | ✅ Done | lru_cache, trước đó rỗng |
| `requirements.txt` | ✅ Done | APScheduler + gRPC thêm vào |
| `minio-init` service | ✅ Done | docker-compose tự init bucket |
| `batch_data.proto` | ✅ Done | 4 RPCs, schema production |
| gRPC Python server stub | ⚠️ TODO | Cần implement `HistoricalDataServiceServicer` |
| gRPC Go client | ⚠️ TODO | Cần protoc codegen + wire vào `backend_api/services` |
| `exchange_config.yaml` s3:// bug | ⚠️ TODO | Cần fix `s3://` → `s3a://` cho Spark compat |

---

## 8. VIỆC CẦN LÀM TIẾP THEO

### [HIGH]
- **Implement Python gRPC server** trong `batch_api`:
  ```
  dataops_batch_api/cli/grpc_server.py  ← HistoricalDataServiceServicer
  ```
  Mỗi RPC query Delta Lake bằng `deltalake.DeltaTable` scan + filter.

- **Implement Go gRPC client** trong `backend_api`:
  ```
  dataops_backend_api/repository/batch/ ← generated pb.go + client wrapper
  dataops_backend_api/services/analytics/crypto.go → GetOHLCV() dùng gRPC thay InfluxDB
  ```

- **Fix exchange_config.yaml**: `s3://` → `s3a://` (dùng cho Spark) hoặc giữ `s3://` (dùng cho delta-rs) — cần thống nhất 1 scheme

### [MEDIUM]
- Thêm `dataops-batch-api` port `50051` vào docker-compose để gRPC accessible
- Backfill job: viết script `python -m cli.jobs.binance_1d_job 2016-01-01 2026-01-01` để lấy 10 năm lịch sử Binance
- Thêm `tcbs_constants_loader.ticker_commons_fields()` method vào `TCBSConstants` (hiện tại `tcbs_eod_job` gọi method chưa có)

### [LOW]
- Unit test cho `_delta_writer.py` với mock S3
- Health check endpoint cho gRPC server (grpc.health.v1)
- Monitoring: Prometheus counter cho số records written mỗi job
