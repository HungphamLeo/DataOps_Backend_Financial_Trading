# Session Notes — Middlewares, Constants & Services Refactor

**Ngày:** 2025  
**Branch:** `25092026_set_up_base_code`  
**Scope:** `okx-analytics-api/` — constants, middlewares, services

---

## 1. Tổng quan công việc trong session

Session này thực hiện 3 nhóm thay đổi độc lập trên `okx-analytics-api/`:

| Nhóm | Mô tả | Files |
|---|---|---|
| **A** | Bổ sung missing constants + docstring toàn bộ `constants/` và `middlewares/` | `constants/constants.go`, `middlewares/*.go` |
| **B** | Fix hardcode `"USDT"` và `"OKX"` trong accounting service | `services/accounting/accounting.go` |
| **C** | Inject `*logging.Logger` vào tất cả services, thêm structured log tại error/key ops | `services/**/*.go`, `services/services.go`, `app/server.go` |

---

## 2. Nhóm A — Constants & Middleware docstrings

### 2.1 `constants/constants.go` — constants còn thiếu

**Vấn đề:** `auth_tiers.go` import `constants.USER_STATUS_BLOCKED`, `constants.USER_STATUS_INACTIVE`, `constants.USER_STATUS_TRIAL`, `constants.ROLE_ADMIN` nhưng các constants này **chưa tồn tại** trong `constants.go`.

**Fix đã thực hiện:**

```go
// ROLE_* — alias uint cho JWTClaims.RoleID / UserSession.RoleID
const (
    ROLE_USER        uint = 1
    ROLE_ADMIN       uint = 2
    ROLE_SUPER_ADMIN uint = 3
)

// USER_STATUS_* — trạng thái tài khoản trong Redis session
const (
    USER_STATUS_ACTIVE   = "active"
    USER_STATUS_INACTIVE = "inactive"
    USER_STATUS_BLOCKED  = "blocked"
    USER_STATUS_TRIAL    = "trial"
)
```

**Lưu ý quan trọng:**
- `ROLE_*` (type `uint`) và `AUTH_ROLE` (type `int`) song song tồn tại vì hai middleware dùng hai kiểu khác nhau. Phải giữ giá trị đồng bộ.
- `USER_STATUS_*` là `string` — match với giá trị lưu trong Redis JSON.

### 2.2 Docstrings đã thêm

| File | Nội dung docstring mới |
|---|---|
| `constants/constants.go` | Package doc, inline comment từng `EXCHANGE`, `TF_*`, `ASSET_TYPE` |
| `middlewares/jwt_auth.go` | Package doc giải thích khác biệt vs `auth_tiers.go`; struct/constructor docs; nil-safety note |
| `middlewares/rate_limiter.go` | Struct doc: thuật toán fixed-window, key format, trade-off vs sliding window; `NewRateLimiterMiddleware`; `Limit()` |
| `middlewares/security.go` | `Secure()`: lý do handle OPTIONS 204; `SecureNoRestrict()`: cảnh báo KHÔNG dùng production |

---

## 3. Nhóm B — Fix hardcode trong `accounting.go`

### Vấn đề

`RecordTrade` hardcode `"USDT"` tại 3 chỗ, `RecordFundingFee` hardcode cả `"OKX"` lẫn `"USDT"`.

### Thay đổi

```go
// TRƯỚC
bal, _ := s.repo.GetBalance(userID, exchange, "USDT")
Currency: "USDT"
bal, _ := s.repo.GetBalance(userID, "OKX", "USDT")
Exchange: "OKX"

// SAU
bal, _ := s.repo.GetBalance(userID, exchange, constants.DefaultCurrency)
Currency: constants.DefaultCurrency
bal, _ := s.repo.GetBalance(userID, exchange, constants.DefaultCurrency) // exchange là tham số
Exchange: exchange
```

### Interface thay đổi (breaking)

```go
// TRƯỚC
RecordFundingFee(userID uint, symbol string, amount float64, refID string) error

// SAU — thêm tham số exchange
RecordFundingFee(userID uint, symbol, exchange string, amount float64, refID string) error
```

> **⚠️ Nếu có caller nào gọi `RecordFundingFee` cần update:** truyền thêm `exchange` (thường là `string(constants.OKX_FUTURE)` hoặc `"OKX"`).

---

## 4. Nhóm C — Inject Logger vào Services

### Pattern áp dụng

Mỗi service struct thêm field `logger *logging.Logger`, constructor nhận thêm tham số, wired qua `services.go` → `server.go`.

```
server.go
  └─ logging.NewLogger(...)  →  logger
  └─ services.NewServices(repo, cfg, redis, logger)
       └─ authentication.NewService(repo, cfg, redis, logger)
       └─ analytics.NewCryptoService(cfg, logger)
       └─ vnstock.NewService(cfg, logger)
       (accounting và indicator inject khi cần thêm vào Services struct)
```

### Log points thêm vào từng service

#### `authentication.go`
| Level | Action | Trigger |
|---|---|---|
| WARN | `Login` | user not found (username), invalid password (userID) |
| ERROR | `Login` | DB error, token generation fail |
| INFO | `Login` | login thành công (userID, username) |
| WARN | `RefreshToken` | parse fail, session not found |
| ERROR | `RefreshToken` | token gen fail |
| INFO | `RefreshToken` | token refreshed (userID) |
| ERROR | `Logout` | Redis del fail |
| INFO | `Logout` | session deleted |

> **Security note:** WARN log khi user not found **có log username** — nếu cần tăng privacy, xóa field `username` khỏi fields map.

#### `accounting.go`
| Level | Action | Trigger |
|---|---|---|
| ERROR | `RecordTrade` | RecordTransaction fail (trade), fee fail, UpsertBalance fail |
| INFO | `RecordTrade` | trade recorded thành công |
| ERROR | `RecordFundingFee` | RecordTransaction fail |
| INFO | `RecordFundingFee` | funding fee recorded |
| ERROR | `UpdateDailyPnL` | GetTransactionsByUser fail |

#### `analytics/crypto.go`
| Level | Action | Trigger |
|---|---|---|
| ERROR | `GetVWAP` | InfluxDB query fail |
| ERROR | `GetOHLCV` | InfluxDB query fail |
| ERROR | `GetFundingRate` | InfluxDB query fail |

#### `vnstock/vnstock.go`
| Level | Action | Trigger |
|---|---|---|
| ERROR | `GetMarketSnapshot` | InfluxDB query fail |
| ERROR | `GetOHLCV` | InfluxDB query fail |
| ERROR | `GetTopMovers` | InfluxDB query fail |

#### `indicator/indicator.go`
| Level | Action | Trigger |
|---|---|---|
| ERROR | `ListGroups` | repo fail |
| WARN | `GetGroup` | not found |
| WARN | `CreateGroup` | duplicate short_name |
| ERROR | `CreateGroup` | repo fail |
| ERROR | `UpdateGroup` | repo fail |
| ERROR | `DeleteGroup` | repo fail |
| INFO | `DeleteGroup` | deleted thành công |

---

## 5. Files chưa inject logger (còn bỏ qua)

`accounting.Service` và `indicator.Service` đã được sửa nhưng **chưa được thêm vào `services.Services` struct**. Khi muốn expose qua REST API:

```go
// services/services.go — thêm vào struct và NewServices()
type Services struct {
    AuthSvc       authentication.Service
    CryptoSvc     analytics.CryptoService
    VNStockSvc    vnstock.Service
    AccountingSvc accounting.Service   // TODO: thêm khi có handler
    IndicatorSvc  indicator.Service    // TODO: thêm khi có handler
}
```

---

## 6. Pre-existing issues (không sửa trong session này)

| Package | Vấn đề | Tác động |
|---|---|---|
| `configuration/` | `Config` redeclared (`config.go` vs `configuration.go`), missing `utils.GetEnvAsString/Int` | Mọi package import `configuration` không build được |
| `logging.go`, `ratelimit.go`, `recovery.go`, `request_id.go` | `//go:build ignore` — file bị exclude khỏi build | Cần quyết định: xóa hoặc implement |
| `middlewares/auth.go` | `//go:build ignore` | Tương tự trên |

> Build lỗi `configuration/` là pre-existing, không phải do session này gây ra.

---

## 7. Structured log format tham khảo

Logger dùng `logging.Logger` (internal JSON logger tại `logging/logger.go`):

```json
{
  "ts": "2025-01-01T00:00:00.000000000Z",
  "level": "ERROR",
  "service": "okx-analytics-api",
  "action": "RecordTrade",
  "msg": "failed to record trade transaction",
  "fields": {
    "userID": 42,
    "symbol": "BTC-USDT-SWAP",
    "exchange": "okx_future",
    "refID": "ord_12345",
    "err": "connection refused"
  },
  "caller": "accounting.go:82"
}
```

---

## 8. Checklist verify sau session

- [x] `constants.go` compile sạch (`go build okx-analytics-api/constants`)
- [x] `logging` package compile sạch
- [x] `gofmt` không báo lỗi syntax trên tất cả 6 service files
- [ ] Fix `configuration/` package (next session)
- [ ] Verify `go build ./...` khi configuration được fix
- [ ] Kiểm tra caller của `RecordFundingFee` cũ đã truyền thêm `exchange` param
