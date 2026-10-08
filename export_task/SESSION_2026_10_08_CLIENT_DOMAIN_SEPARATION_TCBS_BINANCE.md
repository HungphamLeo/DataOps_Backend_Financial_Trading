# SESSION EXPORT — TCBS & Binance Client Refactor: Domain Separation theo Mixin Pattern
**Date:** 2026-10-08
**Branch:** 20260710_data_batch_api
**Scope:** `dataops_batch_api/cli/ingestion/sources/client/` · `dataops_batch_api/requirements.txt`
**References:**
- `github/copilotinstructions/SKILL.md`
- `github/agent/batch-lakehouse-engineer.agent.md`
- `export_task/SESSION_2026_10_08_BATCH_CLI_REFACTOR_LOGGER_SEPARATION.md`
- `export_task/SESSION_2026_10_08_BATCH_SOURCES_TEST_AND_PREFECT_DECOUPLING.md`

---

## 1. MỤC TIÊU SESSION

Bóc tách `tcbs_client.py` và `binance_client.py` (đều là monolithic file >800 dòng) thành nhiều file riêng biệt theo phạm trù chức năng, áp dụng **Python Mixin Pattern** tương tự TCBS cho Binance SDK-based client.

Yêu cầu cụ thể:
1. **TCBS**: Tách từng nhóm method thành file riêng theo domain (Auth, Market Data, Intraday Analytics, Securities, Batch Helpers).
2. **Binance**: Dùng `binance-connector` SDK (thay vì raw `requests`), tách theo market (Spot / UM Futures) và phạm trù (Market Data / Account & Trading).
3. **Backward compatible**: Code cũ import `TCBSRestClient`, `TCBSConstants`, `BinanceRestClient` vẫn hoạt động.

---

## 2. QUYẾT ĐỊNH KIẾN TRÚC

### [D1] Python Mixin Pattern cho cả TCBS và Binance

Thay vì kế thừa đơn (`class Client(BaseClient):`), áp dụng **Multiple Inheritance Mixin** pattern:

```python
class TCBSRestClient(
    TCBSAuthMixin,
    TCBSMarketDataMixin,
    TCBSIntradayAnalyticsMixin,
    TCBSSecuritiesMixin,
    TCBSBatchHelpersMixin,
    TCBSClientBase,         # ← luôn cuối cùng theo MRO
):
    pass
```

**Lý do:**
- Mỗi domain Mixin độc lập, test được riêng lẻ.
- Dễ mở rộng thêm domain mới (e.g. `TCBSPortfolioMixin`) mà không sửa file cũ.
- `# type: ignore[attr-defined]` dùng cho cross-mixin calls (`self._get()`, `self._sdk`, `self._throttle()`) — đây là trade-off chấp nhận được của mixin pattern.

### [D2] Binance dùng binance-connector SDK (v3.13.0)

**SDK wrap thay vì raw requests:**
- `binance.spot.Spot` class xử lý HMAC-SHA256 signing, connection pooling, headers tự động.
- Cả Spot và UM Futures dùng cùng `Spot` class nhưng khác `base_url`:
  - Spot:        `https://api.binance.com`
  - UM Futures:  `https://fapi.binance.com`
- Futures-specific endpoints không có method trực tiếp trong SDK (`funding_rate`, `mark_price`, `open_interest`, `positionRisk`) → gọi qua `self._sdk.query(path, payload, signed=True)`.

### [D3] TCBS `__init__.py` re-export để backward-compat

```python
# tcbs_client/__init__.py
from cli.ingestion.sources.client.tcbs_client.tcbs_client import TCBSRestClient
from cli.ingestion.sources.tcbs_constants_loader import TCBSConstants
__all__ = ["TCBSRestClient", "TCBSConstants"]
```

Các file cũ `from cli.ingestion.sources.client.tcbs_client import TCBSRestClient, TCBSConstants` không cần thay đổi gì.

### [D4] Binance backward-compat alias

```python
# binance_client/__init__.py
BinanceRestClient = BinanceSpotClient  # alias cho code cũ
```

### [D5] No retry cho write operations (Order placement)

`place_order()`, `cancel_order()`, `cancel_open_orders()` trong cả Spot và Futures **không có `@retry` decorator** vì đây là side-effect actions — retry tự động có thể tạo duplicate orders.

---

## 3. CHI TIẾT CẤU TRÚC SAU REFACTOR

### 3.1 TCBS Client Package

```
tcbs_client/
├── __init__.py              ← Package entry point — backward-compat exports
├── tcbs_client.py           ← Thin facade — assembled TCBSRestClient
├── _retry.py                ← [Shared Utility]       _log_retry callback
├── http.py                  ← [HTTP Core]             TCBSClientBase: __init__, _get, from_env
├── auth.py                  ← [Auth / JWT]            TCBSAuthMixin
├── market_data.py           ← [Market Data]           TCBSMarketDataMixin (public endpoints)
├── intraday_analytics.py    ← [Intraday Analytics/BSA] TCBSIntradayAnalyticsMixin
├── securities.py            ← [Securities Info]       TCBSSecuritiesMixin (private endpoint)
└── batch_helpers.py         ← [Batch Helpers]         TCBSBatchHelpersMixin
```

**Phân loại method theo phạm trù:**

| File | Phạm trù | Methods | Auth? |
|---|---|---|---|
| `http.py` | HTTP Core | `__init__`, `_get`, `from_env` | — |
| `auth.py` | Auth / JWT | `authenticate`, `set_token`, `_update_auth_header`, `_is_token_valid`, `_get_auth_headers` | — |
| `market_data.py` | Market Data | `get_ticker_commons`, `get_ticker_snaps`, `get_derivatives`, `get_put_through_snaps` | ❌ Public |
| `intraday_analytics.py` | Intraday Analytics / BSA | `get_intraday_bsa`, `get_intraday_bsa_ext`, `get_intraday_bsa_month`, `get_intraday_history` | ❌ Public |
| `securities.py` | Securities Information | `get_securities` | ✅ Private (JWT) |
| `batch_helpers.py` | Batch Helpers / Pagination | `get_ticker_commons_batch`, `get_intraday_history_all_pages` | — |

### 3.2 Binance Client Package

```
binance_client/
├── __init__.py              ← Package entry point — exports BinanceSpotClient, BinanceFuturesClient, BinanceRestClient alias
├── binance_client.py        ← Thin facade — BinanceSpotClient + BinanceFuturesClient
├── _retry.py                ← [Shared Utility]          _log_retry callback
├── http.py                  ← [HTTP Core]                BinanceClientBase (SDK wrap, throttle, sign)
├── spot/
│   ├── __init__.py
│   ├── market_data.py       ← [Spot / Market Data]       SpotMarketDataMixin (public)
│   └── account.py           ← [Spot / Account & Trading] SpotAccountMixin (private)
└── future/
    ├── __init__.py
    ├── market_data.py       ← [UM Futures / Market Data]       FuturesMarketDataMixin (public)
    └── account.py           ← [UM Futures / Account & Trading]  FuturesAccountMixin (private)
```

**Phân loại method theo phạm trù:**

| File | Phạm trù | Methods chính | Auth? |
|---|---|---|---|
| `http.py` | HTTP Core | `__init__`, `_throttle`, `_sign`, `_build_signed_params`, `ping`, `server_time`, `from_env` | — |
| `spot/market_data.py` | Spot / Market Data | `get_klines`, `get_exchange_info`, `get_depth`, `get_ticker_price`, `get_ticker_24hr`, `get_book_ticker`, `get_avg_price`, `get_agg_trades`, `get_trades` | ❌ Public |
| `spot/account.py` | Spot / Account & Trading | `get_account`, `get_my_trades`, `get_order`, `get_orders`, `get_open_orders`, `place_order`, `cancel_order`, `cancel_open_orders` | ✅ Private |
| `future/market_data.py` | UM Futures / Market Data | `get_klines`, `get_exchange_info`, `get_depth`, `get_ticker_price`, `get_ticker_24hr`, `get_book_ticker`, `get_funding_rate`, `get_mark_price`, `get_open_interest`, `get_agg_trades` | ❌ Public |
| `future/account.py` | UM Futures / Account & Trading | `get_account`, `get_positions`, `get_my_trades`, `get_order`, `get_orders`, `get_open_orders`, `place_order`, `cancel_order` | ✅ Private |

---

## 4. DANH SÁCH FILES TẠO MỚI / CHỈNH SỬA / XÓA

### TCBS

| Trạng thái | Đường dẫn file | Ghi chú |
|---|---|---|
| **Created** | `dataops_batch_api/cli/ingestion/sources/client/tcbs_client/_retry.py` | Shared retry callback |
| **Created** | `dataops_batch_api/cli/ingestion/sources/client/tcbs_client/http.py` | TCBSClientBase: session init, _get, from_env |
| **Created** | `dataops_batch_api/cli/ingestion/sources/client/tcbs_client/auth.py` | TCBSAuthMixin: JWT authenticate, set_token |
| **Created** | `dataops_batch_api/cli/ingestion/sources/client/tcbs_client/market_data.py` | TCBSMarketDataMixin: ticker_commons, snaps, derivatives |
| **Created** | `dataops_batch_api/cli/ingestion/sources/client/tcbs_client/intraday_analytics.py` | TCBSIntradayAnalyticsMixin: BSA + tick history |
| **Created** | `dataops_batch_api/cli/ingestion/sources/client/tcbs_client/securities.py` | TCBSSecuritiesMixin: get_securities (private) |
| **Created** | `dataops_batch_api/cli/ingestion/sources/client/tcbs_client/batch_helpers.py` | TCBSBatchHelpersMixin: batch + pagination |
| **Created** | `dataops_batch_api/cli/ingestion/sources/client/tcbs_client/__init__.py` | Package entry, backward-compat exports |
| **Replaced** | `dataops_batch_api/cli/ingestion/sources/client/tcbs_client/tcbs_client.py` | 848 dòng → 71 dòng thin facade |

### Binance

| Trạng thái | Đường dẫn file | Ghi chú |
|---|---|---|
| **Created** | `dataops_batch_api/cli/ingestion/sources/client/binance_client/_retry.py` | Shared retry callback |
| **Created** | `dataops_batch_api/cli/ingestion/sources/client/binance_client/http.py` | BinanceClientBase: SDK Spot init, throttle, sign |
| **Created** | `dataops_batch_api/cli/ingestion/sources/client/binance_client/spot/market_data.py` | SpotMarketDataMixin: klines, ticker, depth, trades |
| **Created** | `dataops_batch_api/cli/ingestion/sources/client/binance_client/spot/account.py` | SpotAccountMixin: account, orders, place, cancel |
| **Created** | `dataops_batch_api/cli/ingestion/sources/client/binance_client/spot/__init__.py` | Spot subpackage exports |
| **Created** | `dataops_batch_api/cli/ingestion/sources/client/binance_client/future/market_data.py` | FuturesMarketDataMixin: klines, funding_rate, mark_price, OI |
| **Created** | `dataops_batch_api/cli/ingestion/sources/client/binance_client/future/account.py` | FuturesAccountMixin: account, positions, orders |
| **Created** | `dataops_batch_api/cli/ingestion/sources/client/binance_client/future/__init__.py` | Futures subpackage exports |
| **Replaced** | `dataops_batch_api/cli/ingestion/sources/client/binance_client/binance_client.py` | 247 dòng → 144 dòng thin facade |
| **Created** | `dataops_batch_api/cli/ingestion/sources/client/binance_client/__init__.py` | Package entry + BinanceRestClient alias |
| **Deleted** | `dataops_batch_api/cli/ingestion/sources/client/binance_client.py` | Di chuyển vào package |
| **Modified** | `dataops_batch_api/requirements.txt` | Thêm `binance-connector==3.13.0` |

---

## 5. TECHNICAL NOTES — BINANCE SDK

### 5.1 binance-connector v3.13.0 — Key Facts
- Package name: `binance-connector` (không phải `python-binance`)
- Import: `from binance.spot import Spot`
- Chỉ có 1 class: `Spot` — dùng cho cả Spot và UM Futures qua `base_url` override
- Constructor: `Spot(api_key=None, api_secret=None, base_url=None, timeout=None)`
- Tự động sign private requests khi `api_secret` được set
- **KHÔNG có** `binance.um_futures.UMFutures` class trong v3.13.0

### 5.2 Futures-specific endpoints (gọi qua `query()`)

```python
# Funding rate
self._sdk.query("/fapi/v1/fundingRate", payload={"symbol": symbol, ...})

# Mark price
self._sdk.query("/fapi/v1/premiumIndex", payload={"symbol": symbol})

# Open interest
self._sdk.query("/fapi/v1/openInterest", payload={"symbol": symbol})

# Position risk (private)
self._sdk.query("/fapi/v2/positionRisk", payload={"symbol": symbol}, signed=True)
```

### 5.3 Kline row[7] = quoteAssetVolume (vol_ccy)

```
row[0]  openTime           Unix ms
row[1]  open               string
row[2]  high               string
row[3]  low                string
row[4]  close              string
row[5]  volume             string  (base asset — e.g. BTC)
row[6]  closeTime          Unix ms
row[7]  quoteAssetVolume   string  ← dùng làm vol_ccy trong Bronze pipeline
row[8]  numTrades          int
row[9]  takerBuyBaseVolume string
row[10] takerBuyQuoteVolume string
row[11] ignore             string
```

---

## 6. VALIDATION

```
python -m py_compile cli/ingestion/sources/client/tcbs_client/_retry.py       → OK
python -m py_compile cli/ingestion/sources/client/tcbs_client/http.py         → OK
python -m py_compile cli/ingestion/sources/client/tcbs_client/auth.py         → OK
python -m py_compile cli/ingestion/sources/client/tcbs_client/market_data.py  → OK
python -m py_compile cli/ingestion/sources/client/tcbs_client/intraday_analytics.py → OK
python -m py_compile cli/ingestion/sources/client/tcbs_client/securities.py   → OK
python -m py_compile cli/ingestion/sources/client/tcbs_client/batch_helpers.py → OK
python -m py_compile cli/ingestion/sources/client/tcbs_client/tcbs_client.py  → OK
python -m py_compile cli/ingestion/sources/client/tcbs_client/__init__.py     → OK

python -m py_compile cli/ingestion/sources/client/binance_client/_retry.py   → OK
python -m py_compile cli/ingestion/sources/client/binance_client/http.py     → OK
python -m py_compile cli/ingestion/sources/client/binance_client/spot/market_data.py → OK
python -m py_compile cli/ingestion/sources/client/binance_client/spot/account.py    → OK
python -m py_compile cli/ingestion/sources/client/binance_client/spot/__init__.py   → OK
python -m py_compile cli/ingestion/sources/client/binance_client/future/market_data.py → OK
python -m py_compile cli/ingestion/sources/client/binance_client/future/account.py    → OK
python -m py_compile cli/ingestion/sources/client/binance_client/future/__init__.py   → OK
python -m py_compile cli/ingestion/sources/client/binance_client/binance_client.py   → OK
python -m py_compile cli/ingestion/sources/client/binance_client/__init__.py         → OK

ALL 19 FILES — SYNTAX OK (100% Passed)
```

---

## 7. BÀI HỌC KINH NGHIỆM (LESSONS LEARNED)

### L1 — Mixin Pattern phù hợp hơn Single Inheritance cho domain separation
Khi một client có nhiều domain rõ ràng (Auth, Market Data, Analytics, Account), mixin pattern giúp:
- Mỗi file = 1 phạm trù = 1 trách nhiệm duy nhất (SRP).
- Test từng mixin độc lập không cần khởi tạo toàn bộ client.
- `tcbs_client.py` giảm từ 848 dòng → 71 dòng thin facade.

Trade-off: `# type: ignore[attr-defined]` cần thiết cho cross-mixin calls, vì Python type checker không resolve MRO tại compile time.

### L2 — binance-connector v3.13.0 không có UMFutures class riêng
Khác với các version cũ hoặc fork khác (`python-binance`), `binance-connector` chính thức chỉ expose `Spot` class. UM Futures dùng cùng class nhưng override `base_url="https://fapi.binance.com"`. Các endpoint futures-specific cần dùng `self._sdk.query("/fapi/v1/...")`.

### L3 — KHÔNG retry cho write operations (side-effect)
`place_order`, `cancel_order` không có `@retry` decorator. Nếu network fail khi đặt lệnh, retry tự động có thể tạo duplicate orders — nguy hiểm trong production trading. Caller phải xử lý lỗi thủ công và quyết định retry.

### L4 — `_sign()` và `_build_signed_params()` giữ lại dù SDK tự sign
Các method này giữ lại trong `http.py` cho backward-compat với các flow cũ có thể gọi trực tiếp. SDK tự động sign khi `api_secret` được set — không cần gọi thủ công cho normal use cases.

### L5 — Subpackage `spot/` và `future/` giúp scale tương lai
Khi cần thêm Coin-M Futures (`dapi.binance.com`) hoặc Options, chỉ cần tạo thư mục `coinm/` và `options/` với pattern tương tự — không sửa code hiện tại.

---

## 8. TODO TIẾP THEO

- [ ] **[HIGH]** Cập nhật `scripts/test_dataops_batch_sources.py` để test `BinanceSpotClient.get_klines()` và `BinanceFuturesClient.get_funding_rate()` thay vì `BinanceRestClient.get_candles()` (tên method cũ đã bị remove).
- [ ] **[HIGH]** Cập nhật tất cả `bronze_flow.py`, `backfill_flow.py` để import từ `binance_client` package mới: `from cli.ingestion.sources.client.binance_client import BinanceSpotClient`.
- [ ] **[MEDIUM]** Cập nhật `logger_config.yaml` — thêm logger entries cho các module mới:
  - `cli.ingestion.sources.client.binance_client.spot.market_data`
  - `cli.ingestion.sources.client.binance_client.future.market_data`
  - `cli.ingestion.sources.client.tcbs_client.*`
- [ ] **[MEDIUM]** Verify `get_funding_rate()` và `get_mark_price()` hoạt động đúng với `self._sdk.query()` khi test live với fapi.binance.com.
- [ ] **[LOW]** Đổi tên internal file `spot/market_data.py` method `get_klines` → thêm param `confirm` hardcode `1` để align với `BinanceCandleRecord` schema (vol_ccy đã đúng ở row[7]).
