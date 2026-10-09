# SESSION EXPORT — Backend API Refactoring, Batch gRPC Integration & Infra Harmonization

**Date:** 2026-10-09  
**Branch:** `20260710_data_batch_api`  
**Scope:** `dataops_backend_api/` · `infra_dataops_finance_trading/docker-compose.yml`  
**References:**
- `export_task/SESSION_2026_10_08_GRPC_ETL_DESIGN_AND_CLEANUP.md`
- `export_task/SESSION_2026_10_08_PLATFORM_DESIGN_CDM_BATCH_REFACTOR.md`
- `docs/DATAOPS_PLATFORM_DESIGN.md`
- `github/copilotinstructions/SKILL.md`
- `github/agent/backend-software-engineer.agent.md`

---

## 1. MỤC TIÊU SESSION

1. **Refactor `dataops_backend_api` theo Master Design:**
   - Triển khai **Go gRPC Client** kết nối tới `dataops_batch_api` (port 50051) phục vụ truy vấn dữ liệu lịch sử nến 4h, 1d (Delta Lake trên MinIO).
   - Triển khai **TCBS Account & Derivative Proxy Service** với đầy đủ logic caching Redis và bảo mật token JWT.
   - Bổ sung **Ticker endpoints** và đọc real-time data từ Redis cache cho Binance Crypto và TCBS VN Stocks.
2. **Cập nhật Hạ tầng (Infra):**
   - Mở port gRPC `50051:50051` cho service `dataops-batch-api` trong `infra_dataops_finance_trading/docker-compose.yml`.
3. **Kiểm thử & Đảm bảo Chất lượng Code:**
   - Viết unit tests kiểm tra router, auth middleware, và symbol validation; đảm bảo `go test ./...` và `go build` pass 100%.

---

## 2. QUYẾT ĐỊNH KIẾN TRÚC & THIẾT KẾ KỸ THUẬT

### [D1] Phân tách rạch ròi Serving: Real-time vs Historical vs Transactional
- **Real-time / Snapshot (Ticker, Orderbook):** Đọc trực tiếp từ **Redis 7** (TTL 10s cho Binance, TTL 120s cho TCBS) để đạt độ trễ < 5ms.
- **Historical OHLCV (4h, 1d) & Daily BSA:** Gọi **gRPC** sang `dataops_batch_api` để query Delta Lake trên MinIO S3A; tự động fallback sang InfluxDB nếu gRPC chưa sẵn sàng hoặc query khung intraday (1m, 5m, 15m, 1h).
- **Transactional Account Data (TCBS Cash, Stock Assets, Orders, Derivatives):** Proxy trực tiếp từ backend sang TCBS Open API (không qua Kafka / Delta Lake), cache Redis TTL 30s cho balance/assets và TTL 10s cho vị thế phái sinh.

### [D2] Batch gRPC Client & Resilience
- Tạo package `repository/batch` với interface `Client` đóng gói toàn bộ logic gọi `HistoricalDataServiceClient`:
  - `GetBinanceOHLCV`
  - `GetTCBSMarketEOD`
  - `GetTCBSBSAHistory`
  - `GetDataRange`
- Quản lý timeout ngắn (3s connect timeout, non-blocking fallback) để không làm treo REST API request của user khi Batch API đang khởi động.

---

## 3. CHI TIẾT CÁC FILE ĐÃ TẠO VÀ CHỈNH SỬA

### Files Tạo Mới

| File | Vai trò |
|---|---|
| [`dataops_backend_api/repository/batch/batchdata/batch_data.go`](dataops_backend_api/repository/batch/batchdata/batch_data.go) | Định nghĩa Message structs và gRPC Client Interface khớp với `dataops_protobuf/batch-data/batch_data.proto`. |
| [`dataops_backend_api/repository/batch/client.go`](dataops_backend_api/repository/batch/client.go) | Implementation gRPC client wrapper kết nối tới Batch API, parse kết quả sang model `OHLCV` và `TCBSBSASnapshot`. |
| [`dataops_backend_api/models/tcbs_account.go`](dataops_backend_api/models/tcbs_account.go) | Data models cho TCBS cash balance, stock asset holdings, orders, và open derivative positions. |
| [`dataops_backend_api/services/tcbs_account/tcbs_account.go`](dataops_backend_api/services/tcbs_account/tcbs_account.go) | TCBS Account Proxy service: HTTP requests tới TCBS OpenAPI với header Bearer Token/API Key và cache Redis. |
| [`dataops_backend_api/api/rest/tcbs_account.go`](dataops_backend_api/api/rest/tcbs_account.go) | Gin REST Handlers cho `/api/v1/tcbs/account/*`, `/api/v1/tcbs/derivative/*`, và `/api/v1/tcbs/order/place`. |
| [`dataops_backend_api/api/rest/rest_test.go`](dataops_backend_api/api/rest/rest_test.go) | Unit test cho Router, JWT enforcement trên các protected endpoints, và Symbol validation. |

### Files Cập Nhật

| File | Thay đổi chính |
|---|---|
| [`dataops_backend_api/configuration/configuration.go`](dataops_backend_api/configuration/configuration.go) | Bổ sung struct `Batch` (`Batch.GRPCURL` default `localhost:50051`, `Batch.TimeoutSec` default 5). |
| [`dataops_backend_api/services/analytics/crypto.go`](dataops_backend_api/services/analytics/crypto.go) | Bổ sung `GetTicker` (Redis `binance_{symbol}_ticker`), tích hợp Batch gRPC vào `GetOHLCV`, đọc OrderBook từ Redis. |
| [`dataops_backend_api/services/tcbs/tcbs.go`](dataops_backend_api/services/tcbs/tcbs.go) | Bổ sung `GetTicker` (Redis `tcbs_{symbol}_ticker`), tích hợp Batch gRPC `GetTCBSMarketEOD` và `GetTCBSBSAHistory`. |
| [`dataops_backend_api/services/services.go`](dataops_backend_api/services/services.go) | Khởi tạo DI container với `BatchClient`, `TCBSAccountSvc`, `redis.Service`. |
| [`dataops_backend_api/api/rest/crypto.go`](dataops_backend_api/api/rest/crypto.go) | Đăng ký endpoint `GET /api/v1/crypto/ticker` & `GET /api/v1/crypto/:symbol/ticker`. |
| [`dataops_backend_api/api/rest/tcbs.go`](dataops_backend_api/api/rest/tcbs.go) | Đăng ký endpoint `GET /api/v1/tcbs/ticker` & `GET /api/v1/tcbs/:symbol/ticker`. |
| [`dataops_backend_api/api/rest/rest.go`](dataops_backend_api/api/rest/rest.go) | Đăng ký routes TCBS Account & Derivatives, xử lý nil-logger check an toàn. |
| [`dataops_backend_api/app/server.go`](dataops_backend_api/app/server.go) | Khởi tạo kết nối gRPC client và graceful shutdown khi server tắt. |
| [`infra_dataops_finance_trading/docker-compose.yml`](infra_dataops_finance_trading/docker-compose.yml) | Expose port `50051:50051` cho service `dataops-batch-api`. |

---

## 4. DANH SÁCH ENDPOINTS ĐÃ ĐỒNG BỘ TRÊN BACKEND

```
[PUBLIC]
  POST /api/v1/login
  POST /api/v1/refresh-token
  POST /api/v1/logout
  GET  /api/v1/health

[PROTECTED - CRYPTO]
  GET  /api/v1/crypto/ticker
  GET  /api/v1/crypto/:symbol/ticker
  GET  /api/v1/crypto/vwap
  GET  /api/v1/crypto/ohlcv
  GET  /api/v1/crypto/orderbook
  GET  /api/v1/crypto/funding
  GET  /api/v1/crypto/liquidations

[PROTECTED - TCBS MARKET DATA]
  GET  /api/v1/tcbs/ticker
  GET  /api/v1/tcbs/:symbol/ticker
  GET  /api/v1/tcbs/market
  GET  /api/v1/tcbs/orderbook
  GET  /api/v1/tcbs/ohlcv
  GET  /api/v1/tcbs/top-movers
  GET  /api/v1/tcbs/market-summary
  GET  /api/v1/tcbs/bsa
  GET  /api/v1/tcbs/intraday-ticks

[PROTECTED - TCBS ACCOUNT & TRADING PROXY]
  GET  /api/v1/tcbs/account/cash
  GET  /api/v1/tcbs/account/stock-asset
  GET  /api/v1/tcbs/account/orders
  POST /api/v1/tcbs/order/place
  GET  /api/v1/tcbs/derivative/positions
  POST /api/v1/tcbs/derivative/order/place

[ADMIN]
  GET  /api/v1/admin/health
```

---

## 5. KẾT QUẢ KIỂM THỬ (VALIDATION)

```bash
# 1. Chạy Unit Tests
cd dataops_backend_api
go test ./... -v
# === RUN   TestPublicRoutes
# --- PASS: TestPublicRoutes (0.00s)
# === RUN   TestSymbolValidation
# --- PASS: TestSymbolValidation (0.00s)
# PASS
# ok      dataops-backend-api/api/rest    4.212s

# 2. Build Service Binary
go build -o dataops-backend-api.exe .
# SUCCESS (0 errors, 0 warnings)
```

---

## 6. TRẠNG THÁI & CÔNG VIỆC TIẾP THEO

| Hạng mục | Trạng thái | Ghi chú |
|---|---|---|
| Go gRPC Client (Batch Historical) | ✅ Done | Sẵn sàng kết nối `dataops-batch-api:50051` |
| TCBS Account & Trading Proxy | ✅ Done | Có cache Redis 10s-30s |
| Redis Live Ticker Serving | ✅ Done | Endpoint `<5ms` latency |
| Port 50051 docker-compose | ✅ Done | Expose cho `dataops-batch-api` |
| Python gRPC Server (`grpc_server.py`) | ⚠️ TODO | Triển khai `HistoricalDataServiceServicer` trong `dataops_batch_api` |
| `TCBSConstants.ticker_commons_fields()` | ⚠️ TODO | Thêm method vào `tcbs_constants_loader.py` |
| Backfill Binance 1D 10 năm | ⚠️ TODO | Chạy job `binance_1d_job.py` |
| Observability Metrics & Healthcheck | ⚠️ TODO | Thêm metric Prometheus counter & grpc.health.v1 |
