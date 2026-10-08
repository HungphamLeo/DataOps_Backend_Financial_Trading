# SESSION EXPORT — Platform Design, Canonical Data Model & Batch API Refactor
**Date:** 2026-10-08
**Branch:** 20260710_data_batch_api
**Scope:** `dataops_batch_api/` · `dataops_protobuf/` · `dataops_stream_producer/` · `docs/`
**References:**
- `export_task/SESSION_2026_10_07_TCBS_REDIS_CACHE_AND_WSS_CONSUMER.md`
- `docs/openapi-v1.0.0.json` — TCBS iFlash Open API v1.0.0
- `docs/OKX-BINANCE.md` — OKX→Binance migration reference
- `github/copilotinstructions/SKILL.md`
- `github/agent/batch-lakehouse-engineer.agent.md`

---

## 1. MỤC TIÊU SESSION

1. Làm rõ vai trò từng module trong kiến trúc: stream vs batch vs processing vs backend.
2. Refactor `dataops_batch_api` để bổ sung constants, data contracts đầy đủ cho Binance và TCBS.
3. Xây dựng Canonical Data Model (CDM) chung cho cả Python (batch) và Go (stream).
4. Xác định vị trí đúng của CDM trong repo — `dataops_protobuf/market_data.proto` là source of truth.
5. Cập nhật design document từ OKX → Binance+TCBS.
6. Merge 2 TCBS constant files thành 1 file duy nhất.

---

## 2. QUYẾT ĐỊNH KIẾN TRÚC QUAN TRỌNG

### [D1] Phân vai trò 4 module — ranh giới rõ ràng

| Module | Vai trò | KHÔNG làm |
|--------|---------|-----------|
| `dataops_stream_producer` | Real-time gate: WebSocket → Kafka, Redis cache | Không xử lý historical data |
| `dataops_batch_api` | REST pull → Delta Lake Bronze (Prefect, Pydantic) | Không consume Kafka |
| `dataops_processing` | Kafka consume → VWAP/Z-score → Delta Silver + InfluxDB | Không pull source trực tiếp |
| `dataops_backend_api` | Query serving: Redis + InfluxDB + Delta Lake; TCBS account proxy | Không thu dữ liệu |

### [D2] Account/Order data KHÔNG đi qua Kafka hay Delta Lake

TCBS account data (cash balance, holdings, orders) là **transactional data**, không phải market data.
- Luồng đúng: `Client → dataops_backend_api → TCBS REST proxy → Response`
- Không publish lên Kafka, không ghi Delta Lake
- Cache Redis TTL 30s cho account data (không phải realtime)

### [D3] CDM source of truth = `dataops_protobuf/market-data/market_data.proto`

Lý do:
- Protobuf là language-agnostic — không bị lock vào Go hay Python
- `canonical.py` (Python) và `types.go` (Go) là **manual mirrors** của .proto
- Khi thêm enum value mới: sửa .proto trước → update mirrors → chạy protoc khi cần codegen
- `shared/platform/` là Go-only → không thể làm CDM cho Python batch API

### [D4] Canonical form cho các enum hay bị nhầm

| Enum | Binance raw | TCBS raw | Canonical |
|------|-------------|----------|-----------|
| OrderStatus CANCELLED | `"CANCELED"` (1 L) | `"3"` / `"C"` | `CANCELLED` (2 L) — theo ISO/FIX Protocol |
| Side BUY | `m=false` / `"BUY"` | `"NB"` / `"B"` / `"Buy Up"` | `BUY` |
| Side SELL | `m=true` / `"SELL"` | `"NS"` / `"S"` / `"Sell Down"` | `SELL` |
| TCBS position Long | — | `"L"` | `LONG` |
| TCBS position Short | — | `"S"` (vị thế) | `SHORT` |
| Interval 1 phút | `"1m"` | `"1 minute"` (aggregate) | `"1m"` (Binance-style) |

---

## 3. FILES ĐÃ TẠO / CẬP NHẬT

### Tạo mới

| File | Nội dung |
|------|---------|
| `docs/DATAOPS_PLATFORM_DESIGN.md` | Design document mới thay thế `OKX_CRYPTO_PLATFORM_DESIGN.md` — kiến trúc Binance+TCBS, 5 modules, use cases thực tế |
| `dataops_batch_api/cli/ingestion/sources/binance_constants.yaml` | Binance REST/WS endpoints, kline field indices (row[7]=quoteAssetVolume), stream names, order types, rate limits |
| `dataops_batch_api/cli/ingestion/schema/canonical.py` | Canonical Data Model Python — 5 enums, 5 dataclasses, 6 mapper functions |
| `infra_dataops_finance_trading/DOCKER_COMPOSE_ADDITIONS.md` | Hướng dẫn thêm TCBS topics + env vars vào docker-compose |

### Cập nhật

| File | Thay đổi |
|------|---------|
| `dataops_batch_api/cli/ingestion/schema/models.py` | Thêm 5 Pydantic models: `TCBSDerivativeRecord`, `TCBSAccountCashRecord`, `TCBSStockAssetRecord`, `TCBSOrderRecord`, `TCBSOpenPositionRecord` |
| `dataops_batch_api/cli/ingestion/schema/__init__.py` | Export đầy đủ canonical + pydantic models |
| `dataops_batch_api/cli/ingestion/sources/tcbs_constants.yaml` | Merge `tcbs_api_constants.yaml` vào — thêm base_urls, websocket_urls, rate_limits, response field mappings, order_status_codes, kline_intervals, intraday_action_map |
| `dataops_protobuf/market-data/market_data.proto` | Thêm 4 enums: `Interval`, `Side`, `OrderStatus`, `InstrumentType`, `Exchange`; thêm 4 Canonical messages: `CanonicalOHLCV`, `CanonicalTrade`, `CanonicalOrder`, `CanonicalPosition`; đổi string fields → enum trong `Kline`, `MarketTrade` |
| `dataops_stream_producer/pkg/producer/types.go` | Thêm comment trỏ về .proto là source of truth |

### Xóa

| File | Lý do |
|------|-------|
| `dataops_batch_api/cli/ingestion/sources/tcbs_api_constants.yaml` | Trùng lặp với `tcbs_constants.yaml` — đã merge vào file chính |

---

## 4. CANONICAL DATA MODEL — CHI TIẾT

### Enums (canonical.py — Python mirror)

```python
class CanonicalInterval(str, Enum):
    MIN_1 = "1m"   # Binance "1m" | TCBS "1 minute" → đều map về đây

class CanonicalSide(str, Enum):
    BUY   = "BUY"
    SELL  = "SELL"
    LONG  = "LONG"   # TCBS derivative position "L"
    SHORT = "SHORT"  # TCBS derivative position "S"

class CanonicalOrderStatus(str, Enum):
    CANCELLED = "CANCELLED"  # Binance "CANCELED" (1L) + TCBS "3"/"C" → đều map về đây

class CanonicalInstrumentType(str, Enum):
    SPOT       = "SPOT"
    FUTURES    = "FUTURES"
    DERIVATIVE = "DERIVATIVE"  # TCBS VN30F

class CanonicalExchange(str, Enum):
    BINANCE          = "BINANCE"
    BINANCE_FUTURES  = "BINANCE_FUTURES"
    TCBS_HOSE        = "TCBS_HOSE"
    TCBS_HNX         = "TCBS_HNX"
    TCBS_UPCOM       = "TCBS_UPCOM"
    TCBS_DERIV       = "TCBS_DERIV"
```

### Mapper functions (source-specific logic ở 1 nơi duy nhất)

```python
from_binance_kline_row(row, symbol, inst_type, interval) → CanonicalOHLCV
    # row[7] = quoteAssetVolume (KHÔNG phải row[6])
    # confirm = True (REST klines luôn là closed candles)

from_binance_trade_message(msg) → CanonicalTrade
    # msg["m"] = isBuyerMaker: false → BUY, true → SELL

from_tcbs_intraday_tick(tick, ticker) → CanonicalTrade
    # tick["a"] = "Buy Up" → BUY, "Sell Down" → SELL

from_tcbs_stock_order(order, account_no) → CanonicalOrder
    # order["execType"] = "NB" → BUY, "NS" → SELL

from_tcbs_deriv_position(pos, account_id, sub_account_id) → CanonicalPosition
    # pos["side"] = "L" → LONG, "S" → SHORT

from_tcbs_account_cash(response_item, account_no) → CanonicalAccountBalance
```

### Usage pattern

```python
# Mọi downstream code chỉ import từ canonical — không import từ source-specific code
from cli.ingestion.schema.canonical import (
    CanonicalOHLCV, CanonicalInterval, CanonicalSide,
    from_binance_kline_row, from_tcbs_intraday_tick
)

# Khi TCBS đổi "Buy Up" → "BD" (taker buy): chỉ sửa from_tcbs_intraday_tick()
# Downstream không bị ảnh hưởng
```

---

## 5. TCBS CONSTANTS — CẤU TRÚC SAU MERGE

`tcbs_constants.yaml` (280 dòng) là file duy nhất:
- `base_urls`, `websocket_urls` — production + WSS URL template
- `rate_limits` — batch size, timeout, retry, token TTL
- `headers` — HTTP header names
- `endpoints` — 30+ endpoint paths theo service (gaia, tartarus, nyx, aion, akhlys, khronos, hydros, erebos, khaos)
- `index_codes` — HOSE=1, VN30=2, HNX=3...
- `exchanges`, `symbol_format`
- `ticker_commons_fields` — camelCase field names (bidQtty01, matchQtty, changePercent...)
- `derivatives_fields` — field names cho /tartarus/v1/derivatives
- `intraday_tick_fields` — single-char fields: p, v, a, ba, sa, hl, t
- `bsa_fields` — bup, sdp, bsr
- `kline_intervals` — 1m→1d (không có 1s/3m/1w/1M vì TCBS không hỗ trợ)
- `price_types` — LO, ATO, ATC, PLO, MP, MTL, MOK, MAK
- `exec_types` — NB, NS
- `order_sides_deriv` + `position_sides_deriv` — phân biệt B/S (order) vs L/S (position)
- `deriv_order_types`, `condition_order_types`, `condition_order_notes`
- `bsa_investor_types` — sheep, wolf, shark, all
- `intraday_action_map` — "Buy Up"→BUY, "Sell Down"→SELL
- `order_status_codes` — "0"→REJECTED, "3"→CANCELLED, "C"→CANCELLING...
- `deriv_condition_status_codes` — "0"→PENDING, "1"→ACTIVATED...
- `result_codes` — akhlys "0"=success, khronos "1"=success, physis "200"=success
- `session_states`, `intraday_history`, `securities_filter_fields`

---

## 6. PROTOBUF CDM — THAY ĐỔI TRONG market_data.proto

### Enums mới thêm vào

```protobuf
enum Interval { INTERVAL_UNSPECIFIED=0; INTERVAL_1M=2; INTERVAL_5M=4; ... }
enum Side     { SIDE_UNSPECIFIED=0; SIDE_BUY=1; SIDE_SELL=2; SIDE_LONG=3; SIDE_SHORT=4; }
enum OrderStatus { ORDER_STATUS_CANCELLED=6; ... }  // 13 values
enum InstrumentType { INSTRUMENT_TYPE_SPOT=1; INSTRUMENT_TYPE_DERIVATIVE=4; ... }
enum Exchange { EXCHANGE_BINANCE=1; EXCHANGE_TCBS_HOSE=3; EXCHANGE_TCBS_DERIV=6; ... }
```

### Messages mới

```protobuf
message CanonicalOHLCV   { symbol, exchange, inst_type, interval, ts_open_ms, ts_close_ms, OHLCV... }
message CanonicalTrade   { symbol, exchange, trade_id, price, quantity, side, ts_ms... }
message CanonicalOrder   { order_id, account_no, exchange, symbol, inst_type, side, status... }
message CanonicalPosition{ account_no, exchange, symbol, side (LONG/SHORT), net_qty, pnl... }
```

### Breaking changes trong existing messages

| Message | Field cũ | Field mới | Impact |
|---------|----------|-----------|--------|
| `TickerRequest` | `string exchange` | `Exchange exchange` | ⚠️ Breaking — cần regenerate |
| `Kline` | `string interval` | `Interval interval` | ⚠️ Breaking |
| `MarketTrade` | `string side` | `Side side` | ⚠️ Breaking |
| `OrderBookResponse` | `string exchange` | `Exchange exchange` | ⚠️ Breaking |

> Chưa có codegen — Go và Python chưa dùng generated code từ .proto này trong production.
> Nếu cần backward compat trước khi codegen: đọc field dạng `int32` và cast về enum.

---

## 7. REQUEST FLOW — USE CASES ĐÃ CONFIRM

### "Tôi muốn lấy account TCBS hiện tại"
```
Client → dataops_backend_api /api/v1/tcbs/account/cash
       → Check Redis "tcbs_account_{userID}_cash" (TTL 30s)
       → CACHE MISS: forward → TCBS /aion/v1/accounts/{acct}/cashInvestments
       → Validate response với TCBSAccountCashRecord (Pydantic)
       → Map → CanonicalAccountBalance
       → Cache Redis → Return JSON
```

### "Tôi muốn lấy buy/sell volume BTCUSDT"
```
Client → dataops_backend_api /api/v1/crypto/BTCUSDT/vwap
       → Query InfluxDB (written bởi dataops_processing VWAP job)
       → Cache Redis TTL 60s → Return VWAP bars
```

### "Tôi muốn đặt lệnh mua VN30F"
```
Client → dataops_backend_api /api/v1/tcbs/derivative/order/place
       → Validate request body (price within ceil/floor range)
       → Forward → TCBS /khronos/v1/order/place (với service JWT)
       → Audit log → MySQL
       → Return {rc:"1", data:[{orderNo, pk_orderNo}]}
```

---

## 8. LESSON LEARNED

### [L10] TCBS result code khác nhau theo service — không đồng nhất
- `/akhlys` (stock order): field `"error"`, success = `"0"`
- `/khronos` (derivative): field `"rc"`, success = `"1"`
- `/physis` (transfer): field `"code"`, success = `"200"`
- Cần `result_codes` trong `tcbs_constants.yaml` để không hardcode inline

### [L11] TCBS B/S khi đặt lệnh ≠ L/S khi xem vị thế
- Đặt lệnh `/khronos/v1/order/place` → field `side`: `"B"` = Buy, `"S"` = Sell
- Xem vị thế `/khronos/v1/account/portfolio/status` → field `side`: `"L"` = Long, `"S"` = Short
- Phải có 2 section riêng: `order_sides_deriv` và `position_sides_deriv`
- Trong CDM: `CanonicalSide.SIDE_BUY/SELL` khi order, `SIDE_LONG/SHORT` khi position

### [L12] TCBS intraday tick field names là single chars
- `"p"` = price, `"v"` = volume, `"a"` = action, `"ba"` = buy_acc, `"sa"` = sell_acc
- Lý do: tiết kiệm bandwidth khi trả về hàng trăm ticks per request
- Luôn đọc qua `intraday_tick_fields` trong `tcbs_constants.yaml` — không hardcode `"p"`, `"v"` trong code

### [L13] TCBS `matchQtty` có typo "Qtty" — không phải "Qty"
- Binance dùng `"quantity"`, TCBS dùng `"matchQtty"`, `"bidQtty01"`, `"offerQtty01"`
- Đây là typo từ phía TCBS API — không được "sửa" — phải dùng đúng như vậy khi parse
- `ticker_commons_fields.bid_qty_01: "bidQtty01"` — chú thích rõ trong YAML

### [L14] CDM không thể ở shared/platform (Go-only)
- `shared/platform/` chỉ dùng được bởi Go services
- `dataops_batch_api` là Python — cần CDM riêng trong Python
- Giải pháp: `.proto` là source of truth agnostic, manual mirrors cho từng ngôn ngữ
- Khi có protoc codegen: replace manual mirrors bằng generated code

### [L15] Không tạo file constants mới nếu đã có file tương tự
- `tcbs_api_constants.yaml` và `tcbs_constants.yaml` có nội dung trùng lặp
- Phải merge ngay, không để 2 file song song — gây confusion về single source of truth
- Trước khi tạo file mới: kiểm tra file cùng loại đã tồn tại chưa

---

## 9. TRẠNG THÁI CUỐI SESSION

| Component | Status | Ghi chú |
|-----------|--------|---------|
| `docs/DATAOPS_PLATFORM_DESIGN.md` | ✅ Done | Design doc chuẩn mới |
| `binance_constants.yaml` | ✅ Done | Kline field indices, WS streams, error codes |
| `tcbs_constants.yaml` | ✅ Done | Merged đầy đủ, xóa duplicate file |
| `canonical.py` | ✅ Done | 5 enums, 5 models, 6 mappers |
| `schema/__init__.py` | ✅ Done | Export đầy đủ |
| `models.py` | ✅ Done | +5 Pydantic models TCBS account/trading |
| `market_data.proto` | ✅ Done | +4 enums, +4 Canonical messages |
| `types.go` | ✅ Done | Comment trỏ .proto là source of truth |
| Canonical OHLCV Bronze flow integration | ⚠️ TODO | bronze_flow.py chưa gọi from_binance_kline_row() |
| Canonical Trade flow integration | ⚠️ TODO | silver_flow.py chưa dùng CanonicalTrade |
| protoc codegen | ⚠️ TODO | Cần khi dataops_processing (Spark) cần typed proto |
| Breaking changes trong .proto | ⚠️ TODO | TickerRequest, Kline, MarketTrade dùng enum thay string |

---

## 10. VIỆC CẦN LÀM TIẾP THEO

### [HIGH]
- Integrate `from_binance_kline_row()` vào `bronze_flow.py` — thay đoạn parse raw row thủ công
- Integrate `from_tcbs_intraday_tick()` vào pipeline tương ứng
- Viết unit tests cho các mapper functions trong `canonical.py`

### [MEDIUM]
- Implement Spark VWAP job trong `dataops_processing` — consume `binance.trade.ticks` → InfluxDB
- Implement `dataops_backend_api` TCBS account proxy endpoints
- Chạy `protoc` codegen khi bắt đầu implement `dataops_processing`

### [LOW]
- Update `TCBSConstants` class trong `tcbs_client.py` để đọc các section mới từ `tcbs_constants.yaml` (ticker_commons_fields, order_status_codes, intraday_action_map...)
- Thêm `tcbs.derivative.contracts` topic vào docker-compose kafka-init
