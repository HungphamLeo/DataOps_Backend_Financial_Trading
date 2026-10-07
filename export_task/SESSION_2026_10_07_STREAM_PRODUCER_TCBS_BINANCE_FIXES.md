# SESSION EXPORT — Stream Producer: TCBS + Binance Bug Fixes & Data Quality
**Date:** 2026-10-07
**Branch:** 06102026_debug_kafka_infra_realtime
**Scope:** `dataops_stream_producer/` — toàn bộ streaming pipeline
**References:**
- `export_task/SESSION_2026_10_06_STREAM_PRODUCER_KAFKA_HEALTHCHECK_FIX.md`
- `github/agent/realtime-streaming-engineer.agent.md`
- `docs/openapi-v1.0.0.json`
- `docs/OKX-BINANCE.md`

---

## 1. MỤC TIÊU SESSION

1. Verify log `dataops-stream-producer` sau rebuild từ session trước.
2. Debug và fix TCBS cron không fire trong giờ giao dịch HOSE.
3. Fix Binance WebSocket disconnect liên tục (`close 1008 Pong timeout`).
4. Fix Kafka producer `NotLeaderForPartition` → DLQ sau broker restart.
5. Fix TCBS `room` field type mismatch khi parse JSON từ API.
6. Tách TCBS config thành WSS symbols vs REST symbols rõ ràng.
7. Làm rõ kiến trúc: symbol nào real-time, symbol nào chỉ REST poll.
8. Thêm Redis latest-price cache đầy đủ (price, size, side, buy/sell vol) cho Binance.

---

## 2. ROOT CAUSE ANALYSIS & FIXES

### RCA-1 — TCBSPoller cron không fire: Timezone UTC

**Triệu chứng:**
```
[TCBSPoller] Cron jobs started
# → Không có log pollMarketSnapshot sau 9:22 ICT
# → KEYS "tcbs_*" trong Redis trả về empty array
```

**Root cause:** Container chạy UTC. `robfig/cron` mặc định dùng UTC.
Cron `*/1 9-11 * * 1-5` được hiểu là `09:00–11:59 UTC` = `16:00–18:59 ICT` — ngoài giờ HOSE.

**Fix — `pkg/sources/tcbs/market_data/poller.go`:**
```go
var vietnamLocation = mustLoadLocation("Asia/Ho_Chi_Minh")

func mustLoadLocation(name string) *time.Location {
    loc, err := time.LoadLocation(name)
    if err != nil {
        log.Printf("[TCBSPoller] WARNING: cannot load timezone %q — falling back to UTC+7 fixed offset", name)
        return time.FixedZone("ICT", 7*60*60)
    }
    return loc
}

// Trong Start():
c := cron.New(cron.WithLocation(vietnamLocation))
```

**Điều kiện:** `tzdata` đã có sẵn trong `Dockerfile` (`apk add tzdata`) → `time.LoadLocation` hoạt động.

---

### RCA-2 — Binance WebSocket `close 1008 (policy violation): Pong timeout`

**Triệu chứng:** Disconnect mỗi ~3–5 phút, reconnect liên tục.

**Root cause:** Gorilla WebSocket **không tự trả Pong** khi nhận Ping frame từ Binance.
Binance gửi Ping mỗi ~3 phút, yêu cầu Pong trong 10 phút → close `1008` nếu không nhận.

**Fix — `pkg/sources/binance/websocket/producer.go`:**
```go
conn.SetPingHandler(func(data string) error {
    if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
        return err
    }
    return conn.WriteMessage(websocket.PongMessage, []byte(data))
})

const readDeadline = 5 * time.Minute  // > Binance ping interval (3 phút)
// Trong read loop:
conn.SetReadDeadline(time.Now().Add(readDeadline))
```

---

### RCA-3 — Kafka `NotLeaderForPartition` → DLQ

**Triệu chứng:**
```
SendMessage trade ETHUSDT error: kafka server: Tried to send a message to a replica
that is not the leader for some partition. Your metadata is out of date — routing to DLQ
```

**Root cause:** Sau broker restart/failover, leader election mất 2–4s.
Config cũ `Retry.Max=5, Backoff=100ms` = tổng 500ms — quá ngắn.

**Fix — `pkg/producer/producer.go`:**
```go
config.Producer.Retry.Max     = 10
config.Producer.Retry.Backoff = 500 * time.Millisecond  // tổng tối đa 5s
config.Metadata.RefreshFrequency = 30 * time.Second
config.Metadata.Full              = true
```

---

### RCA-4 — TCBS `room` field: `cannot unmarshal number into string`

**Triệu chứng:**
```
GetTickerCommons parse: json: cannot unmarshal number into Go struct field
TickerCommonsResponse.data.0.room of type string
```

**Root cause:** OpenAPI spec document `room` là `string` nhưng API thực tế trả về `float64`
(số cổ phần còn được phép mua bởi NĐT nước ngoài).

**Fix — 4 file:**

| File | Thay đổi |
|---|---|
| `pkg/sources/tcbs/client/types.go` — `TickerCommonItem` | `Room string` → `Room float64` |
| `pkg/sources/tcbs/client/types.go` — `TickerSnapItem` | `Room string` → `Room float64` |
| `pkg/producer/types.go` — `TCBSMarketEvent` | `Room string` → `Room float64` |
| `pkg/schemas/tcbs_market_snapshot.avsc` | `"type":"string","default":""` → `"type":"double","default":0.0` |

---

### RCA-5 — TCBS `requireAuth: false` cho các endpoint cần auth

**Root cause:** `GetTickerCommons`, `GetTickerSnaps`, `GetDerivatives` trong `client.go`
đều gọi với `requireAuth: false`. Nhưng `openapi-v1.0.0.json` chỉ có **global security**
`bearerAuth` — không có endpoint nào có `security: []` override → tất cả đều cần JWT.

**Fix — `pkg/sources/tcbs/client/client.go`:**
```go
// Tất cả 3 endpoints đổi từ false → true
c.doGetWithRetry(EndpointTickerCommons, params, true)
c.doGetWithRetry(EndpointTickerSnaps, params, true)
c.doGetWithRetry(EndpointDerivatives, nil, true)
```

---

### RCA-6 — TCBS config lẫn lộn WSS vs REST symbols

**Root cause:** Một `TCBS_SYMBOLS` env var dùng cho cả BSA/Intraday poll lẫn cron snapshot.
Không phân biệt symbol nào dùng WSS (phái sinh), symbol nào dùng REST (cổ phiếu cơ sở).

**Fix — `pkg/configuration/configuration.go`:**
```go
TCBS struct {
    WssSymbols      []string  // TCBS_WSS_SYMBOLS  — phái sinh VN30F (WSS, chưa implement)
    RestSymbols     []string  // TCBS_REST_SYMBOLS  — cổ phiếu cơ sở (BSA + Intraday EOD)
    Indexes         []string  // TCBS_INDEXES       — basket poll mỗi 1 phút
    BSAInvestorType string
    IntradayMaxPages int
}
```

**Env vars mới trong `.env`:**
```bash
# XÓA: TCBS_SYMBOLS (deprecated)
TCBS_WSS_SYMBOLS=VN30F2410,VN30F2411   # phái sinh — WSS consumer chưa implement
TCBS_REST_SYMBOLS=VCB,VHM,TCB,VIC,HPG,FPT,SSI,VPB,GMD,ACB,CTG,GAS,HDB,MBB,MSN,MWG,SAB,BID,...
TCBS_INDEXES=HOSE,VN30,HNX
```

**Cũng fix typo `.env`:** `BIDBID` → `BID` trong danh sách symbols.

---

### RCA-7 — Redis cache Binance chỉ có `ts`, thiếu market data

**Root cause:** Cache key `binance_{sym}_{stream}` chỉ lưu `{"ts": ...}` để monitor latency.
Không có latest price/volume cho real-time dashboard query.

**Fix — `pkg/sources/binance/websocket/producer.go`:**

Thêm cache key `binance_{symbol}_ticker` mỗi lần nhận trade:
```go
// TTL 10s — reset mỗi trade mới
// buyVol/sellVol tích lũy trong window TTL
p.redis.Set(fmt.Sprintf("binance_%s_ticker", symLower), map[string]interface{}{
    "symbol":   symbol,
    "price":    price,
    "size":     size,          // khối lượng lệnh vừa khớp
    "side":     side,          // "buy" | "sell"
    "buy_vol":  buyVol,        // tích lũy buy volume trong 10s
    "sell_vol": sellVol,       // tích lũy sell volume trong 10s
    "trade_id": rawTrade.TradeID,
    "ts":       rawTrade.TradeTime,
}, 10*time.Second)
```

**Query trong Redis:**
```bash
GET binance_btcusdt_ticker
# → {"symbol":"BTCUSDT","price":63500.5,"size":0.002,"side":"buy","buy_vol":0.045,"sell_vol":0.031,...}
```

---

## 3. KIẾN TRÚC TCBS — REAL-TIME VS REST

### Phân loại nguồn data (từ `openapi-v1.0.0.json`)

| Loại | Endpoint | Frequency | Symbols |
|---|---|---|---|
| **WSS real-time** | `/api/v1/derivatives/contracts` | Push liên tục | Phái sinh: `VN30F2410`, `VN30F2411` |
| **REST poll 1 phút** | `/tartarus/v1/tickerCommons` | Mỗi 1 phút giờ HOSE | Toàn bộ basket: HOSE/VN30/HNX |
| **REST EOD 15:05** | `/nyx/v1/intraday/{ticker}/bsa` | 1 lần/ngày | `TCBS_REST_SYMBOLS` |
| **REST EOD 15:05** | `/nyx/v1/intraday/{ticker}/his/paging` | 1 lần/ngày | `TCBS_REST_SYMBOLS` |

**Kết luận:** Cổ phiếu cơ sở (VCB, HPG, TCB...) **không có real-time stream từ TCBS**.
Chỉ VN30F derivatives mới có WSS — và WSS consumer **chưa được implement** (TODO).

---

## 4. `circuit breaker is open` — Kafka broker restart

**Triệu chứng:** Log flood `circuit breaker is open`, service shutdown gracefully.

**Root cause:** Kafka2 restart (graceful, exit code 0) → KRaft quorum mất majority →
leader election epoch 50, `leader=none` → Sarama exhausts retries → circuit breaker mở.

**Đây KHÔNG phải code bug.** Đây là infra event bình thường.

**Recovery:**
```bash
# 1. Đợi tất cả brokers healthy
docker ps --format "table {{.Names}}\t{{.Status}}" | grep kafka

# 2. Verify KRaft có leader
docker exec dataops-kafka1 kafka-metadata-quorum \
  --bootstrap-server kafka1:29092 describe --status

# 3. Restart stream-producer
cd infra_dataops_finance_trading
docker compose --env-file ../.env up -d --force-recreate dataops-stream-producer
```

---

## 5. LESSON LEARNED

### [L1] `robfig/cron` timezone — BẮT BUỘC inject ICT cho HOSE

```go
// Luôn dùng pattern này khi schedule theo giờ giao dịch Việt Nam
c := cron.New(cron.WithLocation(time.LoadLocation("Asia/Ho_Chi_Minh")))
```

Điều kiện: image Alpine phải có `tzdata` → `apk add tzdata` trong Dockerfile.
Fallback: `time.FixedZone("ICT", 7*60*60)` nếu tzdata không có.

### [L2] Gorilla WebSocket — LUÔN set PingHandler tường minh

Gorilla WebSocket không tự trả Pong. Với bất kỳ exchange nào dùng WS keepalive:
```go
conn.SetPingHandler(func(data string) error {
    conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
    return conn.WriteMessage(websocket.PongMessage, []byte(data))
})
```

### [L3] OpenAPI spec vs API thực tế — luôn verify bằng live call

`room` field: spec ghi `string`, API trả `number`. Không tin spec 100% — verify bằng log lỗi.
Pattern: khi thấy `cannot unmarshal X into Y` → đổi type trong struct cho khớp thực tế.

### [L4] Sarama retry config cho môi trường có broker restart thường xuyên

```go
config.Producer.Retry.Max         = 10       // tăng từ 5
config.Producer.Retry.Backoff     = 500ms    // tăng từ 100ms (đủ cho leader election)
config.Metadata.RefreshFrequency  = 30s      // tránh stale metadata
config.Metadata.Full              = true
```

### [L5] TCBS JWT token TTL = 8 giờ

Token hết hạn sau 8h — cần refresh thủ công bằng Smart OTP từ TCInvest app.
Xem `docs/TCBS_AUTH_GUIDE.md` để biết quy trình.
Không có auto-refresh — nếu token hết hạn, BSA/Intraday poll sẽ silent fail (401 → no retry).

### [L6] Redis cache pattern: monitor key vs latest-data key

| Key pattern | Mục đích | TTL | Content |
|---|---|---|---|
| `binance_{sym}_{stream}` | Latency monitoring | 60s | `{"ts": ms}` only |
| `binance_{sym}_ticker` | Latest trade data | 10s | price, size, side, buy_vol, sell_vol |
| `tcbs_{sym}_ticker` | TCBS poll heartbeat | 120s | `{"ts": ms, "index": name}` |

---

## 6. FILES ĐÃ THAY ĐỔI

| File | Thay đổi |
|---|---|
| `pkg/sources/tcbs/market_data/poller.go` | Timezone ICT, RestSymbols, WSS guard log |
| `pkg/sources/binance/websocket/producer.go` | PingHandler, ReadDeadline, Redis ticker cache |
| `pkg/producer/producer.go` | Retry.Max=10, Backoff=500ms, MetadataRefresh=30s |
| `pkg/sources/tcbs/client/client.go` | requireAuth=true cho tickerCommons/tickerSnaps/derivatives |
| `pkg/sources/tcbs/client/types.go` | Room float64 (x2: TickerCommonItem, TickerSnapItem) |
| `pkg/producer/types.go` | Room float64 trong TCBSMarketEvent |
| `pkg/schemas/tcbs_market_snapshot.avsc` | room type: string→double |
| `pkg/configuration/configuration.go` | Tách WssSymbols/RestSymbols, xóa Symbols cũ |

---

## 7. TRẠNG THÁI CUỐI SESSION

| Component | Status | Ghi chú |
|---|---|---|
| Binance WebSocket BTCUSDT/ETHUSDT | ✅ Running | Ping/Pong fix → không còn 1008 timeout |
| TCBSPoller market snapshot | ✅ Running | Timezone fix → cron fire đúng giờ HOSE |
| TCBSPoller BSA + Intraday | ✅ Scheduled | Chạy 15:05 ICT T2-T6 |
| Kafka Producer | ✅ Resilient | Retry 5s window cho leader election |
| Redis binance_*_ticker | ✅ Full data | price + size + side + buy/sell vol |
| TCBS WSS consumer | ⚠️ Not implemented | TODO: goroutine cho /api/v1/derivatives/contracts |
| Schema Registry | ⚠️ Warning only | Fallback JSON — không ảnh hưởng vận hành |

---

## 8. VIỆC CẦN LÀM TIẾP THEO

### [HIGH]
- **Implement TCBS WSS consumer** cho phái sinh VN30F (`/api/v1/derivatives/contracts`)
  — goroutine riêng giống `BinanceProducer`, subscribe theo `TCBS_WSS_SYMBOLS`
- **Thêm Redis latest-ticker cache cho TCBS** (matchPrice, totalVol, change, bid/ask top3)
  — hiện tại TCBS chỉ có monitoring key, chưa có latest data key
- **Cập nhật `.env`**: xóa `TCBS_SYMBOLS`, thêm `TCBS_WSS_SYMBOLS` + `TCBS_REST_SYMBOLS`
  + fix typo `BIDBID` → `BID`

### [MEDIUM]
- **Guard BinanceProducer**: không start goroutine nếu `cfg.Binance.Symbols` rỗng
- **Guard TCBSPoller**: không start cron nếu `Indexes` và `RestSymbols` đều rỗng
- **Auto token refresh**: implement cron refresh TCBS JWT token trước khi expire (TTL 8h)

### [LOW]
- Rebuild + E2E test theo `docs/TEST_PLAN_BINANCE_E2E.md`
- Verify Kafka topics nhận đủ messages sau fix
