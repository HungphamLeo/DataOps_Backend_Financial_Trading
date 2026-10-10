# TEST PLAN — DataOps Backend Financial Trading (All Layers)

**Branch:** `20261010_data_processing`  
**Date:** 2026-10-10  
**Scope:** 5 modules — `dataops-stream-producer`, `dataops_backend_api`, `dataops_batch_api`, `dataops_processing`, `dataops_protobuf`  
**Tham chiếu:** `docs/BATCH_API_TESTING_GUIDE.md`, `docs/TEST_PLAN_BINANCE_E2E.md`

---

## 1. Mục tiêu & Phạm vi

Tài liệu này định nghĩa **chiến lược test 4 tầng** cho toàn bộ data pipeline từ WebSocket ingestion đến REST API serving. Mỗi tầng độc lập, có thể chạy mà không cần tầng trên fully healthy.

| Tầng | Tên | Môi trường | Mục tiêu |
|------|-----|------------|----------|
| T1 | Unit Tests | Offline, không container | Pure logic, không I/O |
| T2 | Integration Tests | Mocked services | Interfaces, error handling |
| T3 | Component Tests | Partial infra (MinIO, Redis) | Service boundary |
| T4 | End-to-End Tests | Full docker-compose stack | Pipeline integrity |

---

## 2. Trạng thái test hiện tại (Baseline)

### 2.1 Test files đang tồn tại

| Module | File | Số tests | Coverage hiện tại |
|--------|------|----------|-------------------|
| `dataops-stream-producer` | `pkg/configuration/configuration_test.go` | 3 | CSV parsing logic |
| `dataops-stream-producer` | `pkg/producer/partitioner_test.go` | 3 | Symbol → partition mapping |
| `dataops_backend_api` | `api/rest/rest_test.go` | 6 | Route existence, JWT, symbol validation |
| `dataops_batch_api` | `tests/test_delta_writer.py` | 13 | S3 URI, AWS env, PyArrow schema, Delta write |
| `dataops_processing` | `tests/test_vwap_job.py` | 10 | Header strip, JSON parse, InfluxDB line protocol |
| `dataops_processing` | `tests/test_volume_bars_job.py` | 6 | Volume bar cumsum algorithm (O(n)) |
| **Tổng** | | **41** | ~5–10% overall |

### 2.2 Khoảng trống test quan trọng

- ❌ Không có test cho service layer (authentication, analytics, TCBS proxy)
- ❌ Không có test cho Kafka producer (DLQ, retry, error paths)
- ❌ Không có test cho WebSocket reconnect logic
- ❌ Không có test cho Pydantic model validation (`models.py`)
- ❌ Không có test cho Canonical Data Model mapping (`canonical.py`)
- ❌ Không có test cho gRPC server handlers
- ❌ Không có test cho Spark streaming window aggregation
- ❌ Không có contract test cho enum consistency (Proto ↔ Go ↔ Python)

---

## 3. Tầng 1 — Unit Tests (Offline, No I/O)

**Điều kiện chạy:** Không cần container. Chạy bằng `go test ./...` (Go) hoặc `pytest` (Python).

**Tiêu chí pass:** 100% test pass, không có dependency ngoài.

---

### 3.1 dataops-stream-producer (Go)

#### 3.1.1 Configuration Parser ✅ (Đã có)
**File:** `pkg/configuration/configuration_test.go`

| Test case | Input | Expected |
|-----------|-------|----------|
| `TestSplitCSV_Normal` | `"BTCUSDT,ETHUSDT,SOLUSDT"` | `["BTCUSDT","ETHUSDT","SOLUSDT"]` |
| `TestSplitCSV_Single` | `"BTCUSDT"` | `["BTCUSDT"]` |
| `TestSplitCSV_Empty` | `""` | `[]` |

#### 3.1.2 BinancePartitioner ✅ (Đã có)
**File:** `pkg/producer/partitioner_test.go`

| Test case | Input | Expected |
|-----------|-------|----------|
| `TestBinancePartitioner_BTC` | `"BTCUSDT"`, 10 partitions | partition 0 |
| `TestBinancePartitioner_ETH` | `"ETHUSDT"`, 10 partitions | partition 1 |
| `TestBinancePartitioner_Altcoin` | `"SOLUSDT"`, 10 partitions | partition 2..9 (FNV32a hash) |

#### 3.1.3 BinancePartitioner — Edge Cases ❌ (Cần bổ sung)
**File:** `pkg/producer/partitioner_test.go`

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestBinancePartitioner_LowPartition` | numPartitions = 1 | partition 0 |
| `TestBinancePartitioner_LowPartition2` | numPartitions = 2 | partition 0 (fallback) |
| `TestBinancePartitioner_TCBSSymbol` | `"VCB"`, 10 partitions | partition 2..9 (FNV32a) |
| `TestBinancePartitioner_Deterministic` | Same symbol, chạy 2 lần | Cùng partition |

#### 3.1.4 Constants & Topics ❌ (Cần bổ sung)
**File:** `constants/topics_test.go` (mới)

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestTopicConstants_DLQPattern` | Tất cả DLQ topics kết thúc bằng `.dlq` | true |
| `TestTopicConstants_NoDuplicate` | Không có 2 topic constant trùng nhau | true |

---

### 3.2 dataops_backend_api (Go)

#### 3.2.1 REST Routes ✅ (Đã có một phần)
**File:** `api/rest/rest_test.go`

| Test case | Mô tả |
|-----------|-------|
| `TestPublicRoutes` | Tất cả route public tồn tại (health, login, refresh, logout) |
| `TestProtectedRoutes_RequireJWT` | Route protected trả về 401 khi không có token |
| `TestSymbolValidation` | Symbol hợp lệ/không hợp lệ (uppercase, regex) |

#### 3.2.2 Symbol & Constants Validation ❌ (Cần bổ sung)
**File:** `constants/symbols_test.go` (mới)

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestSymbol_Binance_Valid` | `"BTCUSDT"`, `"ETHUSDT"` | valid |
| `TestSymbol_Binance_Invalid` | `"btcusdt"`, `""`, `"BTC-USDT"` | invalid |
| `TestSymbol_TCBS_Valid` | `"VCB"`, `"VHM"`, `"HPG"` | valid |
| `TestSymbol_TCBS_Invalid` | `"vcb"`, `""`, `"V C B"` | invalid |

#### 3.2.3 JWT Token Logic ❌ (Cần bổ sung)
**File:** `middlewares/jwt_auth_test.go` (mới)

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestJWT_ValidToken` | Token hợp lệ, chưa hết hạn | Pass middleware |
| `TestJWT_ExpiredToken` | Token hết hạn | HTTP 401 |
| `TestJWT_WrongSignature` | Token bị tamper | HTTP 401 |
| `TestJWT_MissingHeader` | Request không có Authorization | HTTP 401 |
| `TestJWT_RefreshToken` | Token kiểu refresh không dùng được cho API | HTTP 401 |

#### 3.2.4 TraceID Middleware ❌ (Cần bổ sung)
**File:** `middlewares/request_id_test.go` (mới)

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestTraceID_Generated` | Request không có `X-Trace-ID` header | UUID được tạo mới |
| `TestTraceID_Propagated` | Request có `X-Trace-ID: abc-123` | Header `abc-123` được giữ nguyên |
| `TestTraceID_InResponse` | Sau middleware | `X-Trace-ID` có trong response header |

---

### 3.3 dataops_batch_api (Python)

#### 3.3.1 Delta Writer ✅ (Đã có — 13 tests)
**File:** `tests/test_delta_writer.py`

| Test group | Số tests | Mô tả |
|------------|----------|-------|
| `TestTablePath` | 2 | S3 URI format: `s3a://bucket/delta/bronze/table_name` |
| `TestStorageOptions` | 3 | AWS env vars, defaults, missing keys |
| `TestWriteRecords` | 5 | Delta write modes (append, overwrite), empty list |
| `TestApplyRollingRetention` | 2 | TTL deletion by date, 365/730 ngày |
| `TestSchemaEnforcement` | 1 | PyArrow auto-cast types |

#### 3.3.2 Pydantic Model Validation ❌ (Cần bổ sung)
**File:** `tests/test_models.py` (mới)

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestBinanceKline_Valid` | Row `[open_time, open, high, low, close, vol, ...]` chuẩn | Model hợp lệ |
| `TestBinanceKline_MissingField` | Thiếu field `close` | `ValidationError` |
| `TestBinanceKline_NegativePrice` | `close = -1.0` | `ValidationError` |
| `TestBinanceKline_QuoteVolume` | `row[7]` = quoteAssetVolume (không phải `row[6]`) | Đúng field |
| `TestTCBSSnapshot_Valid` | TCBS market snapshot chuẩn | Model hợp lệ |
| `TestTCBSSnapshot_ZeroPrice` | `match_price = 0` | Model hợp lệ (giá sàn đóng cửa) |

#### 3.3.3 Canonical Data Model Mapping ❌ (Cần bổ sung)
**File:** `tests/test_canonical.py` (mới)

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestCanonical_BinanceTradeToCanonical` | Binance trade event → `CanonicalTrade` | fields khớp (symbol, price, qty, side, ts_ms) |
| `TestCanonical_SideBuy` | `is_buyer_maker = False` → `SIDE_BUY` | `side = 1` |
| `TestCanonical_SideSell` | `is_buyer_maker = True` → `SIDE_SELL` | `side = 2` |
| `TestCanonical_OHLCVInterval_1d` | Interval enum `INTERVAL_1D` | value = 13 |
| `TestCanonical_ExchangeBinance` | Exchange enum `EXCHANGE_BINANCE` | value = 1 |
| `TestCanonical_TCBS_EOD` | TCBS EOD snapshot → `CanonicalOHLCV` | fields khớp |

---

### 3.4 dataops_processing (Python)

#### 3.4.1 Confluent Header Strip ✅ (Đã có — 3 tests)
**File:** `tests/test_vwap_job.py`

| Test case | Mô tả |
|-----------|-------|
| `TestStripConfluentHeader_WithMagicByte` | 5-byte header `[0x00][schema_id]` bị strip |
| `TestStripConfluentHeader_WithoutMagicByte` | Không có magic byte → giữ nguyên |
| `TestStripConfluentHeader_ShortPayload` | Payload < 5 bytes → không strip |

#### 3.4.2 Trade Payload Parser ✅ (Đã có — 5 tests)
**File:** `tests/test_vwap_job.py`

| Test case | Mô tả |
|-----------|-------|
| `TestParseTradePayload_Valid` | JSON chuẩn → struct đúng fields |
| `TestParseTradePayload_MissingPrice` | `price` = null → None |
| `TestParseTradePayload_InvalidJSON` | Malformed JSON → None |
| `TestParseTradePayload_ZeroSize` | `size = 0` → None (filter out) |
| `TestParseTradePayload_NegativePrice` | `price < 0` → None |

#### 3.4.3 InfluxDB Line Protocol ✅ (Đã có — 2 tests)
**File:** `tests/test_vwap_job.py`

| Test case | Mô tả |
|-----------|-------|
| `TestWriteToInfluxDB_LineProtocol` | Output format: `vwap,symbol=BTCUSDT,interval=5m vwap=50000.0,volume=10.5,num_trades=100` |
| `TestWriteToInfluxDB_EmptyBatch` | Batch rỗng → không ghi, không raise exception |

#### 3.4.4 Volume Bars Algorithm ✅ (Đã có — 6 tests)
**File:** `tests/test_volume_bars_job.py`

| Test case | Mô tả |
|-----------|-------|
| `test_assign_volume_bars_empty` | List rỗng → `[]` |
| `test_assign_volume_bars_threshold` | cumsum đúng bar boundary |
| `test_assign_volume_bars_partial_bar` | Partial bar cuối → gán đúng |
| `test_assign_volume_bars_multiple_full_bars` | Nhiều bar đầy đủ |
| `test_assign_volume_bars_single_row_exact` | 1 row = 1 threshold → 1 bar |
| `test_assign_volume_bars_large_input_performance` | 10,000 rows < 0.1s (O(n) regression) |

---

## 4. Tầng 2 — Integration Tests (Mocked Services)

**Điều kiện chạy:** Không cần container. Dùng mock objects cho external services (Redis, InfluxDB, MinIO, Kafka).

**Tiêu chí pass:** All pass, không kết nối mạng thực.

---

### 4.1 dataops_backend_api — Service Layer

#### 4.1.1 Authentication Service ❌
**File:** `services/authentication/auth_test.go` (mới)  
**Mock:** MySQL (sqlmock), Redis (miniredis)

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestLogin_Success` | Username/password đúng → JWT pair | `access_token`, `refresh_token` |
| `TestLogin_WrongPassword` | Password sai → error | HTTP 401 |
| `TestLogin_UserNotFound` | Username không tồn tại | HTTP 401 |
| `TestRefreshToken_Valid` | Refresh token hợp lệ → new access token | 200 OK |
| `TestRefreshToken_Revoked` | Token đã logout (revoked in Redis) | HTTP 401 |
| `TestLogout_BlacklistToken` | Logout → token bị thêm vào Redis blacklist | Token trong blacklist |

#### 4.1.2 Crypto Analytics Service ❌
**File:** `services/analytics/crypto_test.go` (mới)  
**Mock:** Redis mock, InfluxDB mock client

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestGetTicker_CacheHit` | Key `binance_BTCUSDT_ticker` tồn tại trong Redis | Trả về cache (không query DB) |
| `TestGetTicker_CacheMiss` | Key không tồn tại → fallback | HTTP 404 hoặc error |
| `TestGetVWAP_ValidQuery` | Query InfluxDB Flux trả về data | VWAP value đúng |
| `TestGetVWAP_EmptyResult` | InfluxDB trả về empty series | HTTP 404 |
| `TestGetSignal_RSI_Oversold` | RSI < 30 | Signal `"BUY"` |
| `TestGetSignal_RSI_Overbought` | RSI > 70 | Signal `"SELL"` |
| `TestGetSignal_RSI_Neutral` | 30 ≤ RSI ≤ 70 | Signal `"NEUTRAL"` |

#### 4.1.3 TCBS Account Proxy Service ❌
**File:** `services/tcbs_account/tcbs_account_test.go` (mới)  
**Mock:** HTTP server mock (httptest), Redis mock

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestGetCashBalance_CacheHit` | Redis TTL 30s còn hiệu lực | Trả về cache |
| `TestGetCashBalance_ProxySuccess` | TCBS API trả về 200 | Data được cache + trả về |
| `TestGetCashBalance_TCBSError` | TCBS API trả về 500 | HTTP 502 (bad gateway) |
| `TestGetCashBalance_Timeout` | TCBS API timeout | HTTP 504 |
| `TestGetOrders_NoJWT` | Thiếu TCBS JWT token | HTTP 401 |

---

### 4.2 dataops_batch_api — Job Pipeline

#### 4.2.1 Binance Client ❌
**File:** `tests/test_binance_client.py` (mới)  
**Mock:** `requests.Session` (responses library hoặc `unittest.mock`)

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestFetchKlines_Success` | API trả về 500 rows JSON | 500 CanonicalOHLCV objects |
| `TestFetchKlines_RateLimit` | API trả về 429 → retry 3 lần | Raise sau 3 lần |
| `TestFetchKlines_QuoteVolume` | `row[7]` = quoteAssetVolume | Đúng field |
| `TestFetchKlines_EmptyResponse` | API trả về `[]` | `[]` |
| `TestFetchKlines_NetworkTimeout` | Timeout exception | Raise với message |

#### 4.2.2 TCBS Client ❌
**File:** `tests/test_tcbs_client.py` (mới)  
**Mock:** `requests.Session`

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestTCBSClient_AuthSuccess` | JWT token hợp lệ → header được set | `Authorization: Bearer {token}` |
| `TestTCBSClient_GetEOD` | Market snapshot response chuẩn | Parsed `CanonicalOHLCV` list |
| `TestTCBSClient_TokenExpired` | JWT hết hạn → auto-refresh | Refresh được gọi |

#### 4.2.3 gRPC Server Handlers ❌
**File:** `tests/test_grpc_server.py` (mới)  
**Mock:** MinIO không cần (dùng in-memory Delta mock)

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestGRPC_GetBinanceOHLCV_Valid` | Request với symbol + interval + range | Response chứa đúng records |
| `TestGRPC_GetBinanceOHLCV_UnknownSymbol` | Symbol không tồn tại trong Delta | gRPC `NOT_FOUND` |
| `TestGRPC_HealthCheck` | gRPC health check | `SERVING` status |

---

### 4.3 dataops_processing — Spark Logic

#### 4.3.1 VWAP Calculation (Pure Math) ❌
**File:** `tests/test_vwap_calculation.py` (mới)  
**Mock:** Không cần (pure function test)

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestVWAP_SingleTrade` | 1 trade: price=50000, size=1 | VWAP = 50000 |
| `TestVWAP_MultiTrade` | prices=[50000,51000], sizes=[1,2] | VWAP = (50000+102000)/3 = 50666.67 |
| `TestVWAP_ZeroVolume` | Tất cả sizes = 0 | None hoặc raise ZeroDivisionError |
| `TestVWAP_WeightedCorrect` | Nhấn mạnh rằng NOT simple average | `Σ(p×q)/Σq` khác `avg(p)` |

---

## 5. Tầng 3 — Component Tests (Partial Infra)

**Điều kiện chạy:** Cần các service cụ thể được liệt kê cho từng test group.

**Setup chung:**
```bash
# Khởi động chỉ services cần thiết
docker compose -f infra_dataops_finance_trading/docker-compose.yml up -d \
  dataops-minio dataops-minio-init dataops-redis dataops-influxdb
```

---

### 5.1 dataops_batch_api — Delta Lake (Cần MinIO)

**Services cần:** `dataops-minio` (port 9010), `dataops-minio-init`

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestDeltaWrite_Binance1d_Integration` | Ingest 10 Binance 1d klines vào Bronze | Delta table xuất hiện tại `s3a://binance-datalake/delta/bronze/binance_candles_spot` |
| `TestDeltaWrite_Idempotent` | Chạy cùng ingestion 2 lần | Không duplicate rows |
| `TestDeltaWrite_SchemaEnforcement` | Schema sai type (string thay float) | Raise `SchemaError` hoặc cast đúng |
| `TestRollingRetention_365Days` | Insert data > 365 ngày trước → apply retention | Rows cũ bị xóa |

**Lệnh chạy:**
```bash
cd dataops_batch_api
MINIO_ENDPOINT=http://localhost:9010 \
AWS_ACCESS_KEY_ID=prod_minio_admin \
AWS_SECRET_ACCESS_KEY=dataops_fintrading@123 \
pytest tests/test_integration_delta.py -v
```

---

### 5.2 dataops_backend_api — Redis Cache (Cần Redis)

**Services cần:** `dataops-redis` (port 6379)

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestRedis_SetGet_Ticker` | Set `binance_BTCUSDT_ticker`, TTL 10s → GET trong 5s | Value giống |
| `TestRedis_TTL_Expired` | Get sau 11s | `nil` (expired) |
| `TestRedis_RateLimit` | 100 requests/10s từ cùng IP | 429 sau threshold |

---

### 5.3 dataops_processing — InfluxDB Sink (Cần InfluxDB)

**Services cần:** `dataops-influxdb` (port 8086)

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestInfluxDB_WriteVWAP` | Write 1 VWAP point (BTCUSDT, 5m) | Point xuất hiện trong bucket `market-data` |
| `TestInfluxDB_QueryVWAP` | Query by `symbol=BTCUSDT`, `interval=5m`, last 1h | >= 1 result |
| `TestInfluxDB_DuplicateWrite` | Write cùng timestamp 2 lần | Ghi đè (overwrite) |

**Lệnh chạy:**
```bash
cd dataops_processing
INFLUXDB_URL=http://localhost:8086 \
INFLUXDB_TOKEN=PROD_SUPER_SECRET_INFLUXDB_AUTH_TOKEN \
INFLUXDB_ORG=binance-vps-platform \
INFLUXDB_BUCKET=market-data \
pytest tests/test_influxdb_sink.py -v
```

---

### 5.4 dataops_backend_api — gRPC Client (Cần batch_api)

**Services cần:** `dataops-batch-api` (port 50051) + MinIO

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestGRPC_GetBinanceOHLCV_Live` | Query BTCUSDT 1d, last 7 days | JSON response với OHLCV rows |
| `TestGRPC_Timeout` | batch_api không respond trong 5s | Graceful timeout, HTTP 504 |
| `TestGRPC_HealthCheck` | gRPC healthcheck | `SERVING` |

---

## 6. Tầng 4 — End-to-End Tests (Full Stack)

**Điều kiện chạy:** Full docker-compose stack.

**Setup:**
```bash
cd infra_dataops_finance_trading
docker compose up -d --build
# Đợi tất cả services healthy (~2 phút)
docker compose ps
```

**Script tự động:** `scripts/smoke_test.sh`

---

### 6.1 Luồng Binance Real-time

```
Binance WebSocket → stream_producer → Kafka → Redis
                                                ↓
                                    backend_api GET /api/v1/crypto/ticker
```

| Bước | Hành động | Kiểm tra |
|------|-----------|----------|
| E2E-B1 | `docker compose logs dataops-stream-producer` | Log không có ERROR, có `published trade: BTCUSDT` |
| E2E-B2 | Kiểm tra topic Kafka | `kafka-topics.sh --list` chứa `binance.trade.ticks` |
| E2E-B3 | Đọc message từ Kafka | `kafka-console-consumer.sh --topic binance.trade.ticks --max-messages 5` → JSON hợp lệ |
| E2E-B4 | Kiểm tra Redis | `redis-cli GET binance_btcusdt_ticker` → JSON có `price`, `size`, `side` |
| E2E-B5 | Gọi Backend API | `curl -H "Authorization: Bearer {JWT}" http://localhost:8000/api/v1/crypto/ticker?symbol=BTCUSDT` → HTTP 200 |

---

### 6.2 Luồng Batch Ingestion (Binance → Delta Bronze)

```
Binance REST /klines → batch_api job → Delta Bronze (MinIO)
                                               ↓
                              backend_api GET /api/v1/crypto/:symbol/ohlcv
```

| Bước | Hành động | Kiểm tra |
|------|-----------|----------|
| E2E-D1 | Trigger ingestion job | `docker exec dataops-batch-api python cli/jobs/binance_1d_job.py --symbol BTCUSDT` |
| E2E-D2 | Kiểm tra MinIO | MinIO Console (http://localhost:9011) → bucket `binance-datalake` có folder `delta/bronze/binance_candles_spot` |
| E2E-D3 | Đọc Delta table | `docker exec dataops-batch-api python -c "from deltalake import DeltaTable; dt=DeltaTable('s3a://binance-datalake/delta/bronze/binance_candles_spot', ...) ; print(dt.to_pandas().head())"` |
| E2E-D4 | Gọi Backend API OHLCV | `curl -H "Authorization: Bearer {JWT}" "http://localhost:8000/api/v1/crypto/BTCUSDT/ohlcv?from=2024-01-01&to=2024-01-07"` → HTTP 200 |

---

### 6.3 Luồng VWAP Processing (Kafka → Spark → InfluxDB)

```
Kafka (binance.trade.ticks) → processing (Spark Streaming) → InfluxDB + Delta Silver
                                                                        ↓
                                               backend_api GET /api/v1/crypto/:symbol/vwap
```

| Bước | Hành động | Kiểm tra |
|------|-----------|----------|
| E2E-V1 | Stream producer đang chạy | Kafka có messages trong `binance.trade.ticks` |
| E2E-V2 | Processing job log | `docker compose logs dataops-processing` có `vwap_job.start` |
| E2E-V3 | Đợi 1 VWAP window (5 phút + 30s watermark) | Log có `foreachBatch: wrote N points to InfluxDB` |
| E2E-V4 | Kiểm tra InfluxDB | Grafana / InfluxDB UI: query `from(bucket:"market-data") |> filter(fn: (r) => r._measurement == "vwap")` → có data |
| E2E-V5 | Gọi Backend API VWAP | `curl -H "Authorization: Bearer {JWT}" "http://localhost:8000/api/v1/crypto/BTCUSDT/vwap?interval=5m"` → HTTP 200 với `vwap` field |

---

### 6.4 Luồng Authentication

| Bước | Hành động | Kiểm tra |
|------|-----------|----------|
| E2E-A1 | `POST /api/v1/login` với credentials | HTTP 200, `access_token` + `refresh_token` |
| E2E-A2 | Gọi protected endpoint với `access_token` | HTTP 200 |
| E2E-A3 | Gọi protected endpoint không có token | HTTP 401 |
| E2E-A4 | `POST /api/v1/refresh-token` với `refresh_token` | HTTP 200, `access_token` mới |
| E2E-A5 | `POST /api/v1/logout` | HTTP 200 |
| E2E-A6 | Dùng `access_token` cũ sau logout | HTTP 401 (blacklisted) |

---

## 7. Contract Tests (Proto ↔ Go ↔ Python)

**Mục tiêu:** Đảm bảo enum values và field names nhất quán giữa 3 implementation.

**File test mới cần tạo:** `tests/contract/test_enum_consistency.py`

| Test case | Mô tả | Expected |
|-----------|-------|----------|
| `TestEnum_Side_BUY` | Proto `SIDE_BUY=1` ↔ Go `SideBuy=1` ↔ Python `SIDE_BUY=1` | Tất cả = 1 |
| `TestEnum_Side_SELL` | Proto `SIDE_SELL=2` ↔ ... | Tất cả = 2 |
| `TestEnum_Interval_1D` | Proto `INTERVAL_1D=13` ↔ Python `INTERVAL_1D=13` | Tất cả = 13 |
| `TestEnum_Exchange_BINANCE` | Proto `EXCHANGE_BINANCE=1` ↔ Python `EXCHANGE_BINANCE=1` | Tất cả = 1 |
| `TestEnum_Exchange_TCBS_HOSE` | Proto `EXCHANGE_TCBS_HOSE=3` ↔ Python `EXCHANGE_TCBS_HOSE=3` | Tất cả = 3 |
| `TestEnum_OrderStatus_CANCELLED` | `CANCELLED` (2 chữ L) — không phải `CANCELED` | Exact match |
| `TestField_OHLCV_QuoteVolume` | Binance `row[7]` = `quote_volume` (không phải `row[6]`) | Index đúng |

**Ghi chú:** `OrderStatus_CANCELLED` có 2 chữ L — đây là ngoại lệ của Binance (`SESSION_2026_10_08`).

---

## 8. CI/CD Commands

### 8.1 Chạy tất cả unit tests (Offline)

```bash
# Go — stream producer
cd dataops_stream_producer && go test ./... -v

# Go — backend api
cd dataops_backend_api && go test ./... -v

# Python — batch api
cd dataops_batch_api && python -m pytest tests/ -v

# Python — processing
cd dataops_processing && python -m pytest tests/ -v
```

### 8.2 Chạy integration tests

```bash
# Khởi động minimal infra
docker compose -f infra_dataops_finance_trading/docker-compose.yml up -d \
  dataops-minio dataops-minio-init dataops-redis dataops-influxdb

# Đợi services healthy (30s)
sleep 30

# Chạy component tests
cd dataops_batch_api && pytest tests/test_integration_delta.py -v -m integration
cd dataops_processing && pytest tests/test_influxdb_sink.py -v -m integration
```

### 8.3 Chạy E2E smoke test

```bash
# Full stack
cd infra_dataops_finance_trading && docker compose up -d --build

# Đợi tất cả healthy
./scripts/wait_for_healthy.sh

# Chạy smoke test script
./scripts/smoke_test.sh
```

---

## 9. Ma trận ưu tiên

| Priority | Test | Module | Effort | Impact |
|----------|------|--------|--------|--------|
| 🔴 HIGH | `test_models.py` (Pydantic validation) | batch_api | Low | Catch data ingestion bugs |
| 🔴 HIGH | `test_canonical.py` (CDM mapping) | batch_api | Low | Data contract integrity |
| 🔴 HIGH | `test_enum_consistency.py` (contract) | cross-module | Medium | Proto sync bugs |
| 🟡 MEDIUM | JWT middleware tests | backend_api | Low | Security coverage |
| 🟡 MEDIUM | Authentication service tests | backend_api | Medium | Core auth logic |
| 🟡 MEDIUM | VWAP calculation unit test | processing | Low | Analytics correctness |
| 🟢 LOW | Binance client mock tests | batch_api | Medium | API contract |
| 🟢 LOW | TCBS client mock tests | batch_api | Medium | API contract |
| 🟢 LOW | gRPC server handler tests | batch_api | High | Service layer |
| 🟢 LOW | Spark streaming integration | processing | High | Requires Spark session |

---

## 10. Định nghĩa Done (Definition of Done)

Một tầng test được coi là **DONE** khi:

- [ ] Tất cả test cases trong tầng đó **PASS**
- [ ] Không có test case nào bị **SKIP** trừ khi có lý do ghi rõ
- [ ] Không có **regression** so với baseline 41 tests hiện tại
- [ ] Test chạy độc lập — không phụ thuộc thứ tự chạy
- [ ] Mỗi test có **assert message** rõ ràng khi fail
- [ ] Test mới được commit kèm với code change tương ứng

---

## 11. Tài liệu liên quan

| File | Mô tả |
|------|-------|
| [`docs/TEST_PLAN_BINANCE_E2E.md`](TEST_PLAN_BINANCE_E2E.md) | E2E test guide từng bước cho Binance pipeline |
| [`docs/BATCH_API_TESTING_GUIDE.md`](BATCH_API_TESTING_GUIDE.md) | 4-tier strategy cho batch_api |
| [`docs/openapi-v1.0.0.json`](openapi-v1.0.0.json) | Swagger spec REST API |
| [`dataops_protobuf/market-data/market_data.proto`](../dataops_protobuf/market-data/market_data.proto) | CDM source of truth |
| [`export_task/SESSION_2026_10_10_COMPLATE_BASE_CODE.md`](../export_task/SESSION_2026_10_10_COMPLATE_BASE_CODE.md) | Session log mới nhất |
