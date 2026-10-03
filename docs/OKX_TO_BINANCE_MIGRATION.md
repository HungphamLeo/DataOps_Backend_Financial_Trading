# OKX → Binance Migration Reference

> **Dự án:** `okx-vps-platform`  
> **Đối chiếu từ 4 repo thực tế:** `okx-vps-platform` · `trading-bot-background-tradebot` · `market-maker-background` · `trading-bot-api`

---

## Mục lục

1. [Tổng quan — Binance đã tồn tại ở đâu](#1-tổng-quan--binance-đã-tồn-tại-ở-đâu)
2. [Constants — Exchange names & Source keys](#2-constants--exchange-names--source-keys)
3. [REST API — Price URLs](#3-rest-api--price-urls)
4. [Response Structures — OKX vs Binance](#4-response-structures--okx-vs-binance)
5. [ExchangeInfo API — Import Symbols](#5-exchangeinfo-api--import-symbols)
6. [Cronjob Schedule — Tần suất gọi](#6-cronjob-schedule--tần-suất-gọi)
7. [WebSocket — URL, Port, Subscribe Protocol](#7-websocket--url-port-subscribe-protocol)
8. [Stream Channels — OKX vs Binance](#8-stream-channels--okx-vs-binance)
9. [Symbol Format — Naming Convention](#9-symbol-format--naming-convention)
10. [WS Message Response — Field Mapping](#10-ws-message-response--field-mapping)
11. [Avro Schema — Thay đổi cần thiết](#11-avro-schema--thay-đổi-cần-thiết)
12. [Kafka Topics — Rename Plan](#12-kafka-topics--rename-plan)
13. [Configuration .env — Thay đổi đầy đủ](#13-configuration-env--thay-đổi-đầy-đủ)
14. [Checklist Migration — Toàn bộ cross-repo](#14-checklist-migration--toàn-bộ-cross-repo)

---

## 1. Tổng quan — Binance đã tồn tại ở đâu

> ✅ **Binance đã được implement sẵn** trong `trading-bot-background-tradebot` — không cần viết từ đầu, chỉ cần đối chiếu & mở rộng sang `okx-vps-platform`.

| Repo | OKX hiện có | Binance hiện có | Trạng thái |
|------|------------|-----------------|------------|
| `okx-vps-platform/okx-stream-producer` | ✅ WebSocket producer đầy đủ (`websocket/producer.go`), config, Avro schemas | ❌ Không có | ⚠️ Cần xây dựng Binance producer |
| `okx-vps-platform/okx-analytics-api` | ✅ REST endpoints `/crypto/*`, InfluxDB queries cho OKX symbols | ❌ Không có | ⚠️ Cần đổi symbol format + topic names |
| `trading-bot-background-tradebot` | ✅ `GetOkxPrice()` · `OKXFuture` source import · `okx_price` Redis key | ✅ `GetBinancePrice()` · `BinanceFuture` source import · `binance_price` Redis key | ✅ Cả 2 đều tồn tại — tham khảo trực tiếp |
| `market-maker-background` | ❌ Không trực tiếp | ❌ Không trực tiếp — chỉ có `CheckProviderSlowResponse()` dùng Redis key `*_orderbook`, `*_ticker` | Pattern Redis key monitoring cần match |
| `trading-bot-api` | ❌ Không có OKX-specific | ✅ `exchange_sources` table chứa `binance`, `binance_future`; `EXCHANGE` constants đầy đủ | DB schema đã support Binance |

---

## 2. Constants — Exchange names & Source keys

> File: `trading-bot-background-tradebot/constants/source.go` · `constants/exchange_source.go`

| Hạng mục | OKX (hiện tại) | Binance (đã có sẵn) |
|----------|---------------|---------------------|
| Source constant | `OKXFuture = "okx_future"` | `Binance = "binance"` / `BinanceFuture = "binance_future"` |
| SOURCE_ prefix | Không có `SOURCE_OKX` — chỉ có `OKXFuture` | `SOURCE_BINANCE_SPOT = "BINANCE"` / `SOURCE_BINANCE_FUTURE = "BINANCE_FUTURE"` |
| EXCHANGE enum | Không có `OKX_Exchange` trong `exchange_source.go` — OKX chưa có entry Spot | `BinanceSpot_Exchange = "binance_spot"` / `BinanceFuture_Exchange = "binance_future"` |
| Redis price key | `"okx_price"` (redis DB9) | `"binance_price"` (redis DB9) |
| Redis provider monitoring key | `"okx_{symbol}_{channel}"` (trong okx-stream-producer) | `"binance_{symbol}_{stream}"` |

---

## 3. REST API — Price URLs

> File: `trading-bot-background-tradebot/pkg/background-tradebot/configuration/default.go`

| Exchange | Config field | Default URL (hardcode) | Ghi chú |
|----------|-------------|------------------------|---------|
| **Binance** | `cfg.Binance.PriceURL` | `https://fapi.binance.com/fapi/v1/ticker/price` | Futures ticker — trả array tất cả symbols |
| BingX | `cfg.BingX.PriceURL` | `https://open-api.bingx.com/openApi/swap/v2/quote/price` | |
| ByBit | `cfg.ByBit.PriceURL` | `https://api.bybit.com/v5/market/tickers?category=linear` | |
| **OKX** | `cfg.Okx.PriceURL` | `https://www.okx.com/api/v5/market/tickers?instType=SWAP` | REST GET không cần auth |

---

## 4. Response Structures — OKX vs Binance

> File: `trading-bot-background-tradebot/pkg/background-tradebot/responses/okx.go` vs `binance.go`

### OKX — `GetOkxPriceResponse`

```go
// responses/okx.go
type GetOkxPriceResponse struct {
    RetCode string         `json:"code"`
    RetMsg  string         `json:"msg"`
    Data    []OkxPriceData `json:"data"`
}
type OkxPriceData struct {
    Symbol    string `json:"instId"`    // "BTC-USDT-SWAP"
    LastPrice string `json:"last"`
    Time      string `json:"ts"`        // string milliseconds — phải parse
}
// Cache: "okx_price" → []OkxPrice
// Error check: result.RetCode != "0" → return error
```

### Binance — `GetBinancePriceResponse`

```go
// responses/binance.go
type GetBinancePriceResponse struct {
    Symbol string  `json:"symbol"`  // "BTCUSDT"
    Price  string  `json:"price"`
    Time   float64 `json:"time"`    // float64, không phải string
}
// Response là ARRAY trực tiếp (không có wrapper object)
// result := []*GetBinancePriceResponse{}
// Cache: "binance_price" → result (raw array)
// Không cần error check RetCode
```

### Bảng đối chiếu chi tiết

| Điểm khác biệt | OKX response | Binance response |
|----------------|-------------|-----------------|
| Wrapper | `{"code","msg","data":[...]}` — có wrapper object | `[{"symbol","price","time"},...]` — array trực tiếp |
| Error check | `result.RetCode != "0"` → return error | Không có RetCode — chỉ check `resp.StatusCode == 200` |
| Symbol field | `instId` (json tag) = `"BTC-USDT-SWAP"` | `symbol` (json tag) = `"BTCUSDT"` |
| Price field | `last` (json tag) → string | `price` (json tag) → string |
| Timestamp type | `ts` → `string` → parse sang `int64` thủ công | `time` → `float64` trực tiếp |
| Unmarshal target | `&responses.GetOkxPriceResponse{}` (con trỏ single struct) | `[]*responses.GetBinancePriceResponse{}` (slice) |

---

## 5. ExchangeInfo API — Import Symbols

> File: `trading-bot-background-tradebot/pkg/background-tradebot/services/background/source.go`

| Exchange | ExchangeInfo URL | Response struct | Symbol filter |
|----------|-----------------|-----------------|---------------|
| **Binance Spot** | `https://api.binance.com/api/v3/exchangeInfo` | `BinanceResponse{Symbols []BinanceSymbol}` | Lọc `suffix == "USDT" \|\| "BUSD"` |
| **Binance Future** | `https://fapi.binance.com/fapi/v1/exchangeInfo` | Cùng `BinanceResponse` (reuse struct) | Cùng USDT/BUSD filter |
| **OKX Future** | `https://www.okx.com/api/v5/public/instruments?instType=SWAP` | `OKXFutureResponse{Data []OKXFutureResult}` — field `InstId` | Không filter — lấy tất cả SWAP |
| BingX Spot | `https://open-api.bingx.com/openApi/spot/v1/common/symbols` | `BingXResponse{Data.Symbols[]}` | Không filter |
| ByBit | `https://api.bybit.com/v5/market/instruments-info?category=spot` | `ByBitResponse{Result.List[]}` | Không filter |

> ⚠️ **OKX không có Spot** trong `sourceUrls` map — chỉ có `OKXFuture`. Binance có cả Spot và Future (Spot bị comment). Nếu cần thêm OKX Spot: URL = `https://www.okx.com/api/v5/public/instruments?instType=SPOT`, parse field `instId`.

---

## 6. Cronjob Schedule — Tần suất gọi

> File: `trading-bot-background-tradebot/app/background-tradebot/server/background.go`

| Job | Schedule | Hàm được gọi | Đã bao gồm OKX? | Đã bao gồm Binance? |
|-----|----------|--------------|-----------------|---------------------|
| Get crypto price | `@every 10s` | `GetCryptoCurrencyPrice()` | ✅ `GetOkxPrice()` | ✅ `GetBinancePrice()` |
| Import symbols | `@every 6h` | `ImportSymbols()` | ✅ `OKXFuture` | ✅ `BinanceFuture` (Spot bị comment) |
| Check transaction | `@every 30s` | `CheckCryptoCurrencyTransaction()` | gRPC call tới accounting — không exchange-specific | |
| Midnight job | `@midnight` | `RunAtMidnight()` | Stats, cleanup — không exchange-specific | |

---

## 7. WebSocket — URL, Port, Subscribe Protocol

| Hạng mục | OKX | Binance |
|----------|-----|---------|
| WS Public URL | `wss://ws.okx.com:8443/ws/v5/public` | `wss://stream.binance.com:9443/stream?streams=...` |
| Futures WS URL | Cùng URL — phân biệt qua `instId` suffix `-SWAP` | Riêng: `wss://fstream.binance.com/stream?streams=...` |
| **Port** | **8443** (WSS) | **9443** hoặc **443** (fallback) |
| Subscribe method | JSON: `{"op":"subscribe","args":[{"channel":"trades","instId":"BTC-USDT"}]}` | URL-based: `?streams=btcusdt@trade/ethusdt@trade` — hoặc JSON: `{"method":"SUBSCRIBE","params":["btcusdt@trade"],"id":1}` |
| Ping/Pong | JSON ping: `{"op":"ping"}` mỗi 25s | WS protocol PING frame từ server → client tự trả PONG |
| Max streams/connection | ~100 channel | 1024 streams per combined connection |
| Backup URL | `wss://wsaws.okx.com:8443/ws/v5/public` | `wss://stream.binance.com:443/ws` (port 443 fallback) |

---

## 8. Stream Channels — OKX vs Binance

| Dữ liệu | OKX channel | Binance stream | Tần suất |
|---------|------------|----------------|---------|
| Trade Ticks | `trades` (instId: BTC-USDT) | `btcusdt@trade` | Real-time |
| Order Book Top 5 | `books5` | `btcusdt@depth5@100ms` | ~100ms |
| Order Book full | `books` | `btcusdt@depth@100ms` | 100ms (diff) |
| Funding Rate | `funding-rate` (instId: BTC-USDT-**SWAP**) | `btcusdt@markPrice` (trên fstream) — field `"r"` | 3s |
| Liquidation | `liquidation-orders` (instType: ANY) | `btcusdt@forceOrder` hoặc `!forceOrder@arr` | Real-time |
| Mark Price | `mark-price` | `btcusdt@markPrice@1s` | 1s hoặc 3s |
| Kline/OHLCV | `candle1m`, `candle5m` | `btcusdt@kline_1m` | Per update |
| Ticker 24h | `tickers` | `btcusdt@ticker` hoặc `!ticker@arr` | ~1s |

---

## 9. Symbol Format — Naming Convention

| Loại | OKX format | Binance format | Go conversion |
|------|-----------|----------------|--------------|
| Spot | `BTC-USDT` | `BTCUSDT` | `strings.ReplaceAll(s, "-", "")` |
| Perpetual Futures | `BTC-USDT-SWAP` | `BTCUSDT` (trên fstream domain) | Bỏ `-SWAP` suffix |
| WS stream name | Truyền `instId` trong JSON subscribe | `btcusdt@trade` (lowercase + @stream) | `strings.ToLower(sym) + "@trade"` |
| instType field | Có: `SPOT`, `SWAP`, `FUTURES`, `OPTION` | Không có — phân biệt qua domain URL | Set hardcode khi parse |
| DB storage | `instId = "BTC-USDT-SWAP"` trong `exchange_sources.symbol` | `symbol = "BTCUSDT"` | Filter: `suffix == "USDT"` |

---

## 10. WS Message Response — Field Mapping

### OKX Trade Tick

```json
{
  "arg": { "channel": "trades", "instId": "BTC-USDT" },
  "data": [{
    "instId": "BTC-USDT",
    "tradeId": "130639474",    // string
    "px": "43000.5",           // price
    "sz": "0.1",               // size
    "side": "buy",             // trực tiếp
    "ts": "1711234567890"      // string milliseconds
  }]
}
// Route: msg.Arg.Channel → topic
```

### Binance Trade Tick (Combined Stream)

```json
{
  "stream": "btcusdt@trade",   // route key
  "data": {
    "e": "trade",
    "E": 1711234567890,        // event time, int64
    "s": "BTCUSDT",
    "t": 12345,                // tradeId, int64 (không phải string)
    "p": "43000.5",            // price
    "q": "0.1",                // quantity
    "T": 1711234567890,        // trade time, int64
    "m": false                 // isBuyerMaker: false=buy, true=sell
  }
}
// Route: msg.Stream → topic
```

### Bảng field mapping Trade Tick

| Field | OKX | Binance | Logic chuyển đổi |
|-------|-----|---------|-----------------|
| Symbol | `instId = "BTC-USDT"` | `s = "BTCUSDT"` | Bỏ dấu `-` |
| Trade ID | `tradeId` → `string` | `t` → `int64` | Convert khi cần |
| Price | `px` | `p` | Đổi tên field |
| Qty/Size | `sz` | `q` | Đổi tên field |
| Side | `side = "buy"/"sell"` | `m bool`: false=buy, true=sell | `if m { "sell" } else { "buy" }` |
| Timestamp | `ts` → string → parse int64 | `T` → int64 trực tiếp | Không cần parse |
| Route key | `msg.Arg.Channel` | `msg.Stream` | Switch case thay đổi |

### OKX OrderBook (books5)

```json
{
  "arg": { "channel": "books5", "instId": "BTC-USDT" },
  "data": [{
    "asks": [["43001", "0.5", "0", "1"]],   // [price, size, deprecated, numOrders]
    "bids": [["43000", "1.2", "0", "2"]],
    "ts": "1711234567890",
    "checksum": -855196281
  }]
}
```

### Binance OrderBook (depth5@100ms)

```json
{
  "lastUpdateId": 160,
  "bids": [["43000.00", "1.20000000"]],   // chỉ 2 phần tử, không có numOrders
  "asks": [["43001.00", "0.50000000"]]
  // Không có "ts" field — dùng "E" từ combined stream wrapper
}
```

### OKX Funding Rate

```json
{
  "arg": { "channel": "funding-rate" },
  "data": [{
    "instId": "BTC-USDT-SWAP",
    "instType": "SWAP",
    "fundingRate": "0.0001",
    "nextFundingRate": "0.00005",
    "fundingTime": "1711234800000"
  }]
}
```

### Binance Mark Price (chứa funding rate — trên fstream)

```json
{
  "e": "markPriceUpdate",
  "E": 1711234567000,
  "s": "BTCUSDT",
  "p": "43000.12",   // mark price
  "P": "43001.00",   // estimated settle price
  "i": "43005.50",   // index price
  "r": "0.00010000", // funding rate
  "T": 1711238400000 // next funding time
}
```

---

## 11. Avro Schema — Thay đổi cần thiết

> File: `okx-vps-platform/okx-stream-producer/pkg/schemas/*.avsc`

| File | OKX fields | Binance fields cần đổi | Mức độ |
|------|-----------|------------------------|--------|
| `okx_trade_event.avsc` | `instId`, `tradeId`(string), `size`, `side` enum[buy,sell], `instType` enum[SPOT,SWAP,FUTURES,OPTION] | Đổi `tradeId`→long; bỏ `instType` enum; đổi `size`→`quantity`; thêm `isBuyerMaker` bool; namespace → `com.binance.vps.events` | 🟡 Vừa |
| `okx_orderbook_snapshot.avsc` | `checksum` nullable int, `numOrders` trong PriceLevel | `checksum`→`lastUpdateId` long; bỏ `numOrders` | 🟡 Vừa |
| `okx_liquidation_event.avsc` | `bkLoss`, `totalLoss`, `instType` string | Binance `forceOrder` khác hoàn toàn: `s`,`S`,`o`,`f`,`q`,`p`,`ap`,`X`,`l`,`z`,`T` | 🔴 Viết mới |
| `okx_funding_rate.avsc` | `fundingRate`, `nextFundingRate`, `fundingTime` | Lấy từ `markPrice` stream: field `r`(rate), `T`(nextFundingTime), `p`(markPrice) | 🟡 Vừa |

---

## 12. Kafka Topics — Rename Plan

| OKX topic (hiện tại) | Binance topic (đề xuất) | Partitions | Retention |
|---------------------|------------------------|------------|-----------|
| `okx.trade.ticks` | `binance.trade.ticks` | 12 | 7 days |
| `okx.orderbook.snapshot` | `binance.orderbook.snapshot` | 6 | 1 day |
| `okx.liquidation.events` | `binance.liquidation.events` | 3 | 30 days |
| `okx.funding.rates` | `binance.funding.rates` | 3 | 90 days |
| `okx.ohlcv.batch` | `binance.ohlcv.batch` | 3 | 7 days |
| `okx.alerts.funding-anomaly` | `binance.alerts.funding-anomaly` | 1 | 7 days |
| `okx.alerts.liquidation` | `binance.alerts.liquidation` | 1 | 7 days |
| `vps.market.snapshot` | `vps.market.snapshot` ✅ giữ nguyên | 6 | 3 days |
| `platform.dlq` | `platform.dlq` ✅ giữ nguyên | 3 | 30 days |

**Consumer Groups** (đổi tên tương ứng):

| Group ID cũ | Group ID mới |
|-------------|-------------|
| `spark-streaming-group` | giữ nguyên |
| `alert-group` | giữ nguyên |
| `influxdb-group` | giữ nguyên |
| `api-query-group` | giữ nguyên |

---

## 13. Configuration .env — Thay đổi đầy đủ

### OKX `.env` (okx-stream-producer — hiện tại)

```env
OKX_WS_URL=wss://ws.okx.com:8443/ws/v5/public
OKX_INSTRUMENTS=BTC-USDT,ETH-USDT,SOL-USDT
OKX_SWAP_INSTRUMENTS=BTC-USDT-SWAP,ETH-USDT-SWAP,SOL-USDT-SWAP
OKX_WS_PING_INTERVAL=25
OKX_WS_RECONNECT_DELAY=5

# background-tradebot:
OKX_PRICE_URL=https://www.okx.com/api/v5/market/tickers?instType=SWAP
```

### Binance `.env` (thay thế)

```env
# Spot WebSocket
BINANCE_WS_URL=wss://stream.binance.com:9443/stream
# Futures (USD-M) WebSocket — domain riêng
BINANCE_FUTURES_WS_URL=wss://fstream.binance.com/stream
# REST API
BINANCE_REST_URL=https://api.binance.com/api/v3
BINANCE_FUTURES_REST_URL=https://fapi.binance.com/fapi/v1
# Symbols (Binance format — không dấu gạch ngang)
BINANCE_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
BINANCE_FUTURES_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
BINANCE_WS_RECONNECT_DELAY=5
# Không cần API key cho public market data

# background-tradebot (đã có sẵn):
BINANCE_PRICE_URL=https://fapi.binance.com/fapi/v1/ticker/price
```

---

## 14. Checklist Migration — Toàn bộ cross-repo

| # | Hạng mục | File/Repo | Độ phức tạp |
|---|----------|-----------|-------------|
| 1 | Đổi WS URL + port `8443`→`9443`, domain okx→binance | `okx-stream-producer/.env`, `configuration.go` | 🟢 Dễ |
| 2 | Symbol format: `BTC-USDT`→`BTCUSDT` (viết helper func) | `okx-stream-producer/pkg/sources/binance/` | 🟢 Dễ |
| 3 | Đổi subscribe: JSON args → URL-based combined stream | `producer.go` — `connectAndStream()` | 🟡 Vừa |
| 4 | Route message: `msg.Arg.Channel` → `msg.Stream` | `producer.go` — `handleMessage()` | 🟡 Vừa |
| 5 | Side detection: `side` string → `isBuyerMaker` bool | `producer.go` | 🟢 Dễ |
| 6 | Tách Futures producer riêng (fstream domain) | Tạo mới `futures_producer.go` | 🟡 Vừa |
| 7 | Update 4 Avro schemas (`.avsc`) | `pkg/schemas/*.avsc` | 🟡 Vừa |
| 8 | Rename Kafka topics: `okx.*`→`binance.*` | `constants/topics.go` + `docker-compose.yml` | 🟢 Dễ |
| 9 | Đổi InfluxDB measurement names | `repository/influxdb/influxdb.go` | 🟢 Dễ |
| 10 | Update batch REST client Python | `okx-batch-ingestion/sources/okx_client.py`→`binance_client.py` | 🟡 Vừa |
| 11 | Response struct: OKX có wrapper, Binance là array thẳng | `responses/okx.go` vs `binance.go` — đã có sẵn, chỉ tham khảo | ✅ Có sẵn |
| 12 | Redis price key: `"okx_price"` → `"binance_price"` | `services/background/okx.go` vs `binance.go` — đã có sẵn | ✅ Có sẵn |
| 13 | Constants: thêm `OKX_Exchange` nếu cần Spot import OKX (hiện thiếu) | `constants/exchange_source.go` | 🟢 Dễ (nếu cần) |
| 14 | Redis monitoring key: `okx_{sym}_{ch}` → `binance_{sym}_{stream}` | `okx-stream-producer/producer.go` — match với `market-maker-background/CheckProviderSlowResponse()` | 🟢 Dễ |
| 15 | Spark streaming: đổi topic subscribe `okx.*`→`binance.*` | `okx-spark-streaming/jobs/*.py` | 🟢 Dễ |
