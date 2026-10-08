# SESSION EXPORT — TCBS Redis Cache Fix & WSS Derivative Consumer
**Date:** 2026-10-07 (session tiếp nối)
**Branch:** 06102026_debug_kafka_infra_realtime
**Scope:** `dataops_stream_producer/` — TCBS ticker cache + TCBS WSS consumer
**References:**
- `export_task/SESSION_2026_10_07_STREAM_PRODUCER_TCBS_BINANCE_FIXES.md`
- `github/agent/realtime-streaming-engineer.agent.md`
- `docs/openapi-v1.0.0.json`

---

## 1. MỤC TIÊU SESSION

1. Fix TCBS Redis ticker cache chỉ có `ts` — thêm đầy đủ market data.
2. Làm rõ Binance Redis đã có đủ data hay chưa (verify).
3. Implement TCBS WSS consumer cho phái sinh VN30F (`TCBS_WSS_SYMBOLS`).
4. Đảm bảo WSS consumer chỉ start khi `TCBS_WSS_SYMBOLS` không rỗng — tiết kiệm tài nguyên.

---

## 2. PHÂN TÍCH & QUYẾT ĐỊNH THIẾT KẾ

### Binance Redis — Đã đúng, không cần sửa

Binance hiện có **2 Redis key riêng biệt** (implement từ session 10-07 RCA-7):

| Key pattern | Content | TTL | Mục đích |
|---|---|---|---|
| `binance_{sym}_{stream}` | `{"ts": ms}` only | 60s | Latency monitoring |
| `binance_{sym}_ticker` | price, size, side, buy_vol, sell_vol, trade_id, ts | 10s | Latest trade data cho dashboard/API |

→ Query `GET binance_btcusdt_ticker` là đủ dữ liệu realtime.

### TCBS Redis — Bug: chỉ có `ts`

Trong `poller.go` line 191 (cũ):
```go
// CŨ — chỉ monitor latency
p.redis.Set(cacheKey, map[string]interface{}{"ts": nowMs, "index": indexName}, 120*time.Second)
```

**Fix:** Thêm đầy đủ market data vào cache (xem mục 3).

### TCBS WSS — Chỉ phái sinh có WSS

Theo `openapi-v1.0.0.json`, TCBS chỉ có 1 WSS endpoint:
- `/api/v1/derivatives/contracts` — phái sinh VN30F (VN30F2410, VN30F2411...)

Cổ phiếu cơ sở (VCB, HPG, TCB...) **không có WSS** — chỉ REST poll mỗi 1 phút.

**Thiết kế:** WSS consumer là goroutine riêng, chỉ start khi `cfg.TCBS.WssSymbols` không rỗng.
Nếu `TCBS_WSS_SYMBOLS` empty → không tạo connection, không tốn tài nguyên.

---

## 3. ROOT CAUSE & FIXES

### Fix-1 — TCBS Redis ticker cache đầy đủ

**File:** `pkg/sources/tcbs/market_data/poller.go`

```go
// MỚI — đầy đủ market data, TTL 120s (= interval poll 1 phút + buffer)
cacheKey := fmt.Sprintf("tcbs_%s_ticker", item.Symbol)
p.redis.Set(cacheKey, map[string]interface{}{
    "symbol":      item.Symbol,
    "index":       indexName,
    "match_price": item.MatchPrice,
    "match_qty":   item.MatchQtty,
    "total_vol":   item.TotalVol,
    "total_val":   item.TotalVal,
    "change":      item.Change,
    "change_pct":  item.ChangePercent,
    "bid_price1":  item.BidPrice01,
    "bid_qty1":    item.BidQtty01,
    "ask_price1":  item.OfferPrice01,
    "ask_qty1":    item.OfferQtty01,
    "ref_price":   item.RefPrice,
    "high":        item.High,
    "low":         item.Low,
    "ts":          nowMs,
}, 120*time.Second)
```

**Query:**
```bash
GET tcbs_vcb_ticker
# → {"symbol":"VCB","match_price":87500,"match_qty":12300,"total_vol":1250000,
#    "change":500,"change_pct":0.57,"bid_price1":87400,"ask_price1":87600,...}
```

---

### Fix-2 — TCBS WSS Consumer mới

**File mới:** `pkg/sources/tcbs/websocket/consumer.go`

```
TCBS_WSS_SYMBOLS=VN30F2410,VN30F2411  → goroutine start
TCBS_WSS_SYMBOLS= (empty)             → không start, log info
```

**Luồng hoạt động:**
```
TCBSPoller.Start()
  └─ len(WssSymbols) > 0 → go wssConsumer.Start(ctx)
       └─ runWithReconnect() — exponential backoff 5s→10s→...→60s
            └─ connectAndStream()
                 ├─ WSS dial: wss://openapi.tcbs.com.vn/api/v1/derivatives/contracts?token=<jwt>
                 ├─ SetPingHandler (tránh close 1008 — same pattern Binance)
                 └─ handleMessage()
                      ├─ parse array or single JSON object
                      ├─ isWssSymbol() — filter chỉ symbols trong config
                      ├─ PublishTCBSDerivative() → Kafka topic tcbs.derivative.contracts
                      └─ redis.Set(tcbs_{sym}_ticker, ..., 30s)
```

**WSS Message format** (TCBS push — mirror với REST `/tartarus/v1/derivatives`):
```json
{
  "symbol": "VN30F2410",
  "matchPrice": 1250.5,
  "matchQtty": 45,
  "totalVolume": 12350,
  "change": 3.2,
  "changePercent": 0.26,
  "bidPrice01": 1250.0,
  "bidQtty01": 120,
  "offerPrice01": 1251.0,
  "offerQtty01": 85,
  "refPrice": 1247.3,
  "ceilPrice": 1371.8,
  "floorPrice": 1122.6
}
```

**Redis cache cho WSS derivatives** (TTL 30s — push liên tục):
```bash
GET tcbs_vn30f2410_ticker
# → {"symbol":"VN30F2410","match_price":1250.5,"match_qty":45,"total_vol":12350,
#    "change":3.2,"change_pct":0.26,"bid_price1":1250.0,"ask_price1":1251.0,...}
```

---

### Fix-3 — Kafka topic + types mới cho derivatives

**`constants/topics.go`:**
```go
TopicTCBSDerivContracts    = "tcbs.derivative.contracts"
TopicTCBSDerivContractsDLQ = "tcbs.derivative.contracts.dlq"
```

**`pkg/producer/types.go`:**
```go
type TCBSDerivativeEvent struct {
    Symbol     string  `json:"symbol"`
    CeilPrice  float64 `json:"ceilPrice"`
    FloorPrice float64 `json:"floorPrice"`
    RefPrice   float64 `json:"refPrice"`
    MatchPrice float64 `json:"matchPrice"`
    MatchQty   float64 `json:"matchQty"`
    BidPrice01 float64 `json:"bidPrice01"`
    BidQty01   float64 `json:"bidQty01"`
    AskPrice01 float64 `json:"askPrice01"`
    AskQty01   float64 `json:"askQty01"`
    Change     float64 `json:"change"`
    ChangePct  float64 `json:"changePct"`
    TotalVol   float64 `json:"totalVol"`
    TsMs       int64   `json:"tsMs"`
    ProducedMs int64   `json:"producedAtMs"`
}
```

**`pkg/producer/producer.go`** — thêm vào interface và implementation:
```go
PublishTCBSDerivative(ctx context.Context, symbol string, event TCBSDerivativeEvent) error
```

---

## 4. FILES ĐÃ THAY ĐỔI

| File | Loại thay đổi |
|---|---|
| `pkg/sources/tcbs/market_data/poller.go` | Fix Redis cache + wire WSS consumer |
| `pkg/sources/tcbs/websocket/consumer.go` | **TẠO MỚI** — TCBS WSS derivative consumer |
| `pkg/producer/types.go` | Thêm `TCBSDerivativeEvent` |
| `pkg/producer/producer.go` | Thêm `PublishTCBSDerivative` interface + impl + schema reg |
| `constants/topics.go` | Thêm `TopicTCBSDerivContracts` + DLQ topic |

---

## 5. REDIS KEY PATTERN — TỔNG HỢP

| Key | Nguồn | Content | TTL |
|---|---|---|---|
| `binance_{sym}_{stream}` | Binance WSS | `{"ts": ms}` monitor only | 60s |
| `binance_{sym}_ticker` | Binance WSS (@trade) | price, size, side, buy_vol, sell_vol, trade_id, ts | 10s |
| `tcbs_{sym}_ticker` (cổ phiếu) | TCBSPoller REST poll | match_price, match_qty, total_vol, change, bid/ask top1, high, low, ts | 120s |
| `tcbs_{sym}_ticker` (phái sinh) | TCBSWSSConsumer WSS | match_price, match_qty, total_vol, change, bid/ask top1, ts | 30s |

---

## 6. LESSON LEARNED

### [L7] TCBS WSS — JWT qua query param, không phải header

WebSocket handshake không hỗ trợ `Authorization: Bearer` header theo chuẩn browser WS API.
TCBS WSS nhận token qua query string:
```
wss://openapi.tcbs.com.vn/api/v1/derivatives/contracts?token=<jwt>
```
Nếu token expire (TTL 8h) → connect thất bại ngay lúc dial → reconnect loop sẽ retry.
**Cần monitor log** `[TCBSWSConsumer] connection error: dial error` để phát hiện token hết hạn.

### [L8] Guard tài nguyên trước khi start goroutine

Pattern chuẩn cho optional features:
```go
if len(cfg.SomeSymbols) > 0 {
    go consumer.Start(ctx)
} else {
    log.Printf("feature disabled — no symbols configured")
}
```
Không start goroutine khi không cần — tránh idle connection, tránh log noise.

### [L9] Redis TTL phải phản ánh update frequency

| Source frequency | TTL nên đặt |
|---|---|
| WSS real-time (sub-second) | 10–30s |
| REST poll mỗi 1 phút | 90–120s (= interval × 1.5) |
| REST EOD 1 lần/ngày | 24h+ |

TTL quá ngắn → key expire giữa poll cycles → consumer thấy miss.
TTL quá dài → stale data khi source down.

---

## 7. TRẠNG THÁI CUỐI SESSION

| Component | Status | Ghi chú |
|---|---|---|
| Binance WSS BTCUSDT/ETHUSDT | ✅ Running | Redis ticker key đầy đủ |
| TCBSPoller market snapshot | ✅ Running | Redis cache fix với full market data |
| TCBSPoller BSA + Intraday | ✅ Scheduled | 15:05 ICT |
| TCBS WSS consumer (VN30F) | ✅ Implemented | Start khi TCBS_WSS_SYMBOLS != empty |
| Kafka topic tcbs.derivative.contracts | ✅ Defined | Schema + DLQ topic đã có |
| Avro schema tcbs_derivative_event.avsc | ⚠️ TODO | Cần tạo file .avsc cho Schema Registry |

---

## 8. VIỆC CẦN LÀM TIẾP THEO

### [HIGH]
- **Tạo `pkg/schemas/tcbs_derivative_event.avsc`** — Schema Registry fallback JSON hiện tại,
  cần file `.avsc` để đăng ký schema chính thức (tránh warning log khi khởi động)
- **Cập nhật `.env`**: thêm `TCBS_WSS_SYMBOLS=VN30F2410,VN30F2411` nếu muốn bật WSS consumer
- **Test TCBS WSS**: kết nối thực tế với token hợp lệ để verify message format
  (OpenAPI spec không có schema chi tiết cho `/api/v1/derivatives/contracts` WSS push)

### [MEDIUM]
- **Monitor log token expire**: nếu thấy `dial error` lặp đi lặp lại → refresh JWT token
- **Thêm Kafka topic `tcbs.derivative.contracts` vào docker-compose** (topic creation config)
- **Guard BinanceProducer**: không start goroutine nếu `cfg.Binance.Symbols` rỗng
- **Guard TCBSPoller**: không start cron nếu `Indexes` và `RestSymbols` đều rỗng

### [LOW]
- Rebuild + test end-to-end theo `docs/TEST_PLAN_BINANCE_E2E.md`
- Verify Redis keys sau rebuild: `KEYS binance_*_ticker`, `KEYS tcbs_*_ticker`
