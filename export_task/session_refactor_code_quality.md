# SESSION LOG — Code Quality Refactor

> **Dự án:** DataOps Backend Financial Trading (OKX Crypto + VPS Securities VN)
> **Branch:** `25092026_set_up_base_code`
> **Mục tiêu phiên:** Nâng chất lượng codebase: docstrings, constants tập trung, symbol registry, structured logging, xóa hardcode.

---

## 1. BỐI CẢNH & MỤC TIÊU

| Vấn đề ban đầu | Giải pháp |
|---|---|
| Magic numbers (`24h`, `7d`, `8080`) rải rác trong handlers | Tập trung vào `constants/defaults.go` |
| Symbol string hardcode (`"BTC-USDT"`, `"VCB"`) trong từng file | Symbol registry tập trung `constants/symbols.go` |
| `fmt.Printf` dùng làm log trong production code | Thay bằng `logging.NewLogger` (structured JSON) |
| `const port = 8080` sống lẻ trong `app/server.go` | Chuyển vào `constants.DefaultHTTPPort` |
| VPS symbols bao gồm HNX + UPCOM ngoài scope | Rút gọn chỉ còn HOSE |
| `infrastructure/` dùng `log.Printf` thay vì structured logger | Inject `*logging.Logger` qua parameter |
| `logging/` package bị nhầm là duplicate của `shared/platform/observability` | Làm rõ vai trò: internal adapter, không phải duplicate |
| Thiếu docstring trên nhiều file | Bổ sung package doc + function doc toàn bộ |

---

## 2. CÁC FILE ĐÃ TẠO MỚI

### `okx-analytics-api/constants/defaults.go`
Tập trung **toàn bộ magic numbers** — không còn hardcode trong handlers:

```
DefaultVWAPLookback          = -24h
DefaultOHLCVLookback         = -7×24h
DefaultFundingLookback       = -30×24h
DefaultLiquidationLookback   = -7×24h
DefaultVNStockOHLCVLookback  = -90×24h

MaxVWAPLookback              = 7×24h
MaxOHLCVLookback             = 365×24h
MaxFundingLookback           = 90×24h
MaxLiquidationLookback       = 30×24h
MaxVNStockOHLCVLookback      = 365×24h

DefaultCryptoVWAPInterval    = TF_5M
DefaultCryptoOHLCVInterval   = TF_1H
DefaultVNStockInterval       = TF_1D

DefaultPageSize = 20 | MaxPageSize = 100
DefaultTopMoversLimit = 10 | MaxTopMoversLimit = 50
DefaultPnLDays = 30 | MaxPnLDays = 365

DefaultHTTPPort = 8080          ← từ app/server.go chuyển sang

WSReadLimit = 512
WSPingDeadline = 60s
WSBroadcastBufferSize = 1024

DefaultCryptoExchange = "OKX"
DefaultVNExchange     = "HOSE"
DefaultCurrency       = "USDT"
```

### `okx-analytics-api/constants/symbols.go`
**Symbol registry tập trung** — validate 1 chỗ, không rải rác:

**OKX:**
- `okxSpotSymbols`: BTC/ETH/SOL/BNB/XRP/ADA/DOGE/AVAX/DOT/LINK
- `okxSwapSymbols`: BTC/ETH/SOL/BNB/XRP/ADA/DOGE (perpetual swaps)
- `IsValidOKXSymbol()`, `IsValidOKXSpotSymbol()`, `IsValidOKXSwapSymbol()`
- `GetOKXSpotSymbols()`, `GetOKXSwapSymbols()`

**VPS / HOSE (chỉ HOSE, HNX + UPCOM đã xóa):**
- `hoseSymbols`: 30 mã (VN30 bluechips + mid-cap)
- `hoseIndices`: `VNINDEX`, `VN30` (xóa `HNXINDEX`, `UPCOMINDEX`)
- `IsValidHOSESymbol()`, `IsMarketIndex()`, `GetHOSESymbols()`

---

## 3. CÁC FILE ĐÃ SỬA

### `app/server.go`
| Trước | Sau |
|---|---|
| `const port = 8080` hardcode | `constants.DefaultHTTPPort` |
| `fmt.Printf(...)` × 3 | `logger.Info(...)` từ `logging.NewLogger` |
| Import `utils`, `log/slog`, `os` | Import `constants`, `logging` |
| Không có docstring cho `NewServer()` | Full docstring + comment từng bước |

**Thứ tự khởi động rõ ràng (đánh số 1–7):**
```
1. Logger (logging.NewLogger)
2. Config (configuration.FromEnv)
3. Database (repository.InitializeDatabase)
4. Redis (redis.NewServer + Ping)
5. Middlewares JWT (reference)
6. Services (services.NewServices)
7. HTTP Server (http.Server + ListenAndServe)
```

### `api/rest/rest.go`
| Trước | Sau |
|---|---|
| `fmt.Printf(...)` × 2 | `logger.Println(...)` |
| `NewAPI(svc, cfg, redis)` | `NewAPI(svc, cfg, redis, logger *log.Logger)` |
| Import `fmt`, `utils` | Import `log` |

### `api/rest/indicators_accounting.go`
| Trước | Sau |
|---|---|
| `"OKX"` hardcode × 3 | `constants.DefaultCryptoExchange` |
| `"USDT"` hardcode | `constants.DefaultCurrency` |
| `"1h"` hardcode | `constants.TF_1H` |
| `days > 365` hardcode | `constants.MaxPnLDays` |
| `days = 30` hardcode | `constants.DefaultPnLDays` |

### `api/rest/crypto.go`
- Tất cả time range lookback → `constants.Default*Lookback`
- Symbol validation: `constants.IsValidOKXSymbol()`, `IsValidOKXSwapSymbol()`
- Full package doc + function docstrings

### `api/rest/vnstock.go`
| Trước | Sau |
|---|---|
| `constants.IsValidVPSSymbol()` × 3 | `constants.IsValidHOSESymbol()` |
| `constants.IsValidVNExchange()` | Removed (chỉ HOSE → không cần validate) |
| `exchange` nhận từ query param | `exchange := constants.DefaultVNExchange` (hardlock HOSE) |
| Doc đề cập HNXINDEX | Cập nhật: chỉ VNINDEX + VN30 |

### `services/vnstock/vnstock.go`
```go
// Trước
indices := []string{"VN30", "VNINDEX", "HNXINDEX"}

// Sau
indices := []string{"VN30", "VNINDEX"}  // HOSE only
```

### `infrastructure/gateway.go`
| Trước | Sau |
|---|---|
| `RequestLogger()` không param | `RequestLogger(logger *logging.Logger)` |
| `RecoveryMiddleware()` không param | `RecoveryMiddleware(logger *logging.Logger)` |
| `log.Printf(...)` trong middleware | `logger.Info/Error(action, msg, fields)` |

### `infrastructure/load_balancer.go`
| Trước | Sau |
|---|---|
| `NewLoadBalancer(addresses)` | `NewLoadBalancer(addresses, logger *logging.Logger)` |
| `log.Printf(...)` × 5 | `lb.logger.Info/Warn/Error(...)` |
| Logger dùng global `log` package | Logger được inject, lưu trong struct |

### `logging/logger.go`
- Thêm **package doc rõ ràng** giải thích vai trò:
  > *Internal adapter cho okx-analytics-api. `shared/platform/observability` dùng go.uber.org/zap chưa có trong go.mod. Khi upgrade, chỉ cần sửa `logging/logger.go` — không đụng business logic.*

---

## 4. QUYẾT ĐỊNH THIẾT KẾ QUAN TRỌNG

### Tại sao giữ `logging/` thay vì dùng `shared/platform/observability`?

```
shared/platform/observability  →  dùng go.uber.org/zap
okx-analytics-api/go.mod       →  KHÔNG có go.uber.org/zap
```

`shared/platform` không có `go.mod` riêng nên không thể import như external module.
Giải pháp: `logging/` là **adapter nội bộ** với cùng interface JSON structured.
**Migration path:** Khi thêm `go.uber.org/zap` vào `go.mod`, chỉ sửa `logging/logger.go` để wrap zap — tất cả callers giữ nguyên.

### Tại sao bỏ HNX + UPCOM khỏi symbols.go?

User xác nhận scope hiện tại chỉ cần HOSE. Việc giữ HNX/UPCOM:
- Tạo validation functions không bao giờ được dùng
- Gây hiểu nhầm về scope của service
- `GetTopMovers` nhận `exchange` query param → đơn giản hóa thành hardlock `"HOSE"`

**Migration path:** Khi cần mở rộng, thêm `hnxSymbols` map và expose `IsValidHNXSymbol()`.

### Tại sao `DefaultHTTPPort` nằm trong `constants/defaults.go` chứ không phải `configuration/`?

`configuration/` chứa **runtime config** (đọc từ `.env`, có thể thay đổi theo môi trường).
`DefaultHTTPPort = 8080` là **compile-time default** — không đọc từ env, không thay đổi giữa dev/prod.
→ Đúng chỗ là `constants/defaults.go`.

---

## 5. PATTERN CHUNG ÁP DỤNG TRONG SESSION

### Pattern 1: Magic number → Named constant
```go
// Trước (handler)
from := time.Now().Add(-24 * time.Hour)

// Sau (handler)
from := time.Now().Add(constants.DefaultVWAPLookback)

// Định nghĩa (defaults.go)
DefaultVWAPLookback = -24 * time.Hour
```

### Pattern 2: Hardcoded string → Symbol registry
```go
// Trước
if symbol != "BTC-USDT" && symbol != "ETH-USDT" { ... }

// Sau
if !constants.IsValidOKXSpotSymbol(symbol) { ... }
```

### Pattern 3: fmt.Printf → Structured logger
```go
// Trước
fmt.Printf("%s [API] Starting\n", utils.GetTimeNow())

// Sau
logger.Info("server.start", "Starting okx-analytics-api",
    map[string]interface{}{"version": "v1.0.0"})
// Output JSON: {"ts":"...","level":"INFO","service":"okx-analytics-api","action":"server.start","msg":"Starting...","fields":{"version":"v1.0.0"}}
```

### Pattern 4: Global log → Injected logger
```go
// Trước
func NewLoadBalancer(addresses []string) *LoadBalancer { ... }
log.Printf("[LoadBalancer] error: %v", err)  // global

// Sau
func NewLoadBalancer(addresses []string, logger *logging.Logger) *LoadBalancer { ... }
lb.logger.Error("lb.init", "error", fields)  // injected, testable
```

---

## 6. TRẠNG THÁI CUỐI SESSION

### ✅ Hoàn thành
| File | Thay đổi |
|---|---|
| `constants/defaults.go` | Tạo mới — toàn bộ magic numbers |
| `constants/symbols.go` | Tạo mới → refactor: chỉ HOSE, xóa HNX/UPCOM |
| `app/server.go` | `const port` → constant; `fmt.Printf` → `logging.Logger` |
| `api/rest/rest.go` | `fmt.Printf` → `*log.Logger` injected |
| `api/rest/indicators_accounting.go` | `"OKX"/"USDT"/"1h"` → constants |
| `api/rest/crypto.go` | Time ranges → constants; symbol validation |
| `api/rest/vnstock.go` | `IsValidVPSSymbol` → `IsValidHOSESymbol`; xóa exchange validation |
| `api/rest/auth.go` | Full docstring |
| `services/vnstock/vnstock.go` | Xóa `HNXINDEX` khỏi indices list |
| `infrastructure/gateway.go` | `log.Printf` → `logging.Logger` injected |
| `infrastructure/load_balancer.go` | `log.Printf` → `logging.Logger` injected |
| `logging/logger.go` | Làm rõ vai trò: internal adapter, không phải duplicate |
| `shared/platform/observability/observability.go` | Package doc + fix artifact `t61` |

### ⬜ Chưa làm (phát hiện cuối session)
| File | Việc cần làm |
|---|---|
| `middlewares/auth_tiers.go` | `log.Printf` dòng 99 → structured logger |
| `middlewares/jwt_auth.go` | Thêm package doc, docstring cho `RequireAuth`, `RequireRole` |
| `middlewares/rate_limiter.go` | Thêm docstring cho struct + methods |
| `middlewares/security.go` | Mở rộng docstring: giải thích CORS headers, preflight |
| `constants/constants.go` | Thêm `USER_STATUS_*`, `ROLE_ADMIN` constants (đang dùng trong `auth_tiers.go` nhưng chưa thấy định nghĩa) |

---

## 7. DEPENDENCY MAP SAU REFACTOR

```
app/server.go
  ├── constants.DefaultHTTPPort
  ├── logging.NewLogger          ← logging/logger.go (internal adapter)
  └── rest.NewAPI(..., *log.Logger)
          └── middlewares.*

infrastructure/
  ├── gateway.go     ← logging.Logger injected
  └── load_balancer.go ← logging.Logger injected

api/rest/
  ├── crypto.go      ← constants.Default*, constants.IsValidOKX*
  ├── vnstock.go     ← constants.IsValidHOSESymbol, constants.DefaultVNExchange
  └── indicators_accounting.go ← constants.DefaultCryptoExchange, DefaultCurrency, TF_1H

constants/
  ├── constants.go   ← AUTH_ROLE, ASSET_TYPE, EXCHANGE, Timeframe (TF_*)
  ├── defaults.go    ← magic numbers, DefaultHTTPPort, WS config
  └── symbols.go     ← OKX + HOSE symbol registry
```

---

## 8. LỆNH KIỂM TRA NHANH

```bash
# Syntax check tất cả files đã sửa
cd okx-analytics-api
gofmt -l app/server.go constants/defaults.go constants/symbols.go \
  api/rest/vnstock.go api/rest/indicators_accounting.go \
  infrastructure/gateway.go infrastructure/load_balancer.go \
  logging/logger.go
# Output rỗng = tất cả pass

# Tìm fmt.Printf còn sót lại trong production code
grep -rn "fmt\.Printf" --include="*.go" \
  --exclude-dir=".git" . | grep -v "_test.go"

# Tìm hardcoded "OKX", "USDT", "1h" còn sót
grep -rn '"OKX"\|"USDT"\|"1h"' --include="*.go" api/rest/ services/

# Kiểm tra không còn import log.Printf trong infrastructure
grep -n "\"log\"" infrastructure/gateway.go infrastructure/load_balancer.go
# Không có kết quả = đã clean
```
