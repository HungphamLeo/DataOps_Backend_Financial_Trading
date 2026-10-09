# SESSION EXPORT — Batch API Testing Guide, Infra Config Harmonization & Sources Test Refactor

**Date:** 2026-10-09 (Session 3)  
**Branch:** `20260710_data_batch_api`  
**Scope:** `dataops_batch_api/` · `scripts/` · `dataops_protobuf/` · `docs/` · `infra_dataops_finance_trading/`  
**References:**
- `github/agent/batch-lakehouse-engineer.agent.md`
- `github/agent/devops-infra-engineer.agent.md`
- `github/copilotinstructions/SKILL.md`
- `export_task/SESSION_2026_10_09_BATCH_API_GRPC_METRICS_COMPLETION.md`

---

## 1. MỤC TIÊU SESSION

1. **Tạo tài liệu hướng dẫn kiểm thử toàn diện:** Xây dựng [`docs/BATCH_API_TESTING_GUIDE.md`](../docs/BATCH_API_TESTING_GUIDE.md) bao gồm các container cần chạy, phân cấp các tầng test (Unit test mock, Live batch ingestion, gRPC serving, Idempotency).
2. **Harmonize Biến môi trường (.env):** Rà soát toàn bộ các service trong repo và xuất bản template [`.env.example`](../.env.example) chuẩn 12 nhóm biến.
3. **Giải quyết xung đột Dependency:** Khắc phục lỗi `pip install` conflict giữa `grpcio-tools 1.64.1` và `protobuf 4.25.3`.
4. **Fix lỗi Protobuf Codegen:** Loại bỏ directive `option python_package` không hợp lệ trong `.proto` files gây lỗi `Option unknown`.
5. **Fix lỗi Pydantic Cross-Field Validation:** Chuyển `high_gte_low` sang `@model_validator(mode="after")` trong [`models.py`](../dataops_batch_api/cli/ingestion/schema/models.py).
6. **Refactor toàn bộ kịch bản kiểm thử Data Sources:** Viết lại hoàn toàn [`scripts/test_dataops_batch_sources.py`](../scripts/test_dataops_batch_sources.py) chia thành 20 hàm test đơn lẻ, cô lập từng endpoint của Binance (Spot & USD-M Futures) và TCBS Securities, hỗ trợ CLI `--list`, `--test <tên>`, `--source <nguồn>`, kèm bảng tổng kết thời gian thực thi.

---

## 2. CHI TIẾT CÁC FILE ĐÃ TẠO VÀ CHỈNH SỬA

### 2.1 Files Tạo Mới

| File | Vai trò |
|---|---|
| [`docs/BATCH_API_TESTING_GUIDE.md`](../docs/BATCH_API_TESTING_GUIDE.md) | Tài liệu hướng dẫn test 4 tầng: Unit Test (Mock I/O), Live Delta Lake Ingestion (MinIO S3A), gRPC Service & Healthcheck, Checklist tính toàn vẹn dữ liệu. |
| [`.env.example`](../.env.example) | Template biến môi trường toàn diện cho 12 thành phần hệ thống (MinIO, Spark, Batch API, MySQL, Redis, InfluxDB, Kafka, Grafana, Binance, TCBS). |
| [`export_task/SESSION_2026_10_09_BATCH_TESTING_GUIDE_AND_SOURCES_TEST_REFACTOR.md`](./SESSION_2026_10_09_BATCH_TESTING_GUIDE_AND_SOURCES_TEST_REFACTOR.md) | Báo cáo đóng gói chi tiết các thay đổi trong session này. |

### 2.2 Files Cập Nhật

| File | Thay đổi chính |
|---|---|
| [`dataops_batch_api/requirements.txt`](../dataops_batch_api/requirements.txt) | Nâng cấp `protobuf>=5.26.1,<6.0.0` để tương thích hoàn toàn với `grpcio-tools==1.64.1` và `grpcio-health-checking==1.64.1`. |
| [`dataops_protobuf/batch-data/batch_data.proto`](../dataops_protobuf/batch-data/batch_data.proto) | Xóa bỏ directive `option python_package` không hợp lệ trong proto3. |
| [`dataops_protobuf/market-data/market_data.proto`](../dataops_protobuf/market-data/market_data.proto) | Xóa bỏ directive `option python_package` không hợp lệ trong proto3. |
| [`dataops_batch_api/cli/ingestion/schema/models.py`](../dataops_batch_api/cli/ingestion/schema/models.py) | Cập nhật `high_gte_low` trong `BinanceCandleRecord` dùng `@model_validator(mode="after")` để validate cross-field an toàn khi parse. |
| [`scripts/test_dataops_batch_sources.py`](../scripts/test_dataops_batch_sources.py) | Rewrite 100%: Tách 20 hàm test độc lập cho từng endpoint Binance Spot, Futures, TCBS Tickers, Snaps, Derivatives, BSA, Ticks, Pydantic validation và in bảng tổng kết. |

---

## 3. CÁC QUYẾT ĐỊNH KỸ THUẬT & BUG FIXES

### [D1] Phân biệt rõ Docker Service Name và Container Name
- **Vấn đề:** Khi chạy `docker compose build/up`, việc truyền `container_name` (`dataops-minio`, `dataops-batch-api`) thay vì `service_name` (`minio`, `dataops-batch-api`) gây lỗi `no such service`.
- **Quy tắc:**
  - Đối số cho Docker Compose CLI phải là khóa khai báo dưới `services:` trong [`docker-compose.yml`](../infra_dataops_finance_trading/docker-compose.yml).
  - Các service dùng prebuilt image (`minio`, `minio-init`, `spark-master`, `spark-worker`) không có chỉ dẫn `build:`, chỉ cần `docker compose up -d`.

### [D2] Dependency Conflict Protobuf v5 & grpcio-tools
- **Vấn đề:** `grpcio-tools 1.64.1` đòi hỏi `protobuf>=5.26.1`, trong khi `requirements.txt` cũ ghim `protobuf==4.25.3`.
- **Khắc phục:** Cập nhật `protobuf>=5.26.1,<6.0.0` trong `requirements.txt`. Đã test build thành công trên môi trường Python 3.11/3.12.

### [D3] Loại bỏ `option python_package` trong Protobuf
- **Vấn đề:** `protoc` báo lỗi `Option "python_package" unknown` khi biên dịch file `.proto`.
- **Nguyên nhân:** Trong chuẩn proto3, directive package cho Python được quyết định qua cờ `--proto_path` và output dir, không có directive file-level `option python_package`.
- **Khắc phục:** Đã dọn dẹp khỏi `batch_data.proto` và `market_data.proto`. Lệnh codegen chuẩn:
  ```bash
  cd dataops_protobuf
  python -m grpc_tools.protoc -I batch-data --python_out=../dataops_batch_api --grpc_python_out=../dataops_batch_api batch-data/batch_data.proto
  ```

### [D4] Pydantic v2 Cross-Field Validation Order
- **Vấn đề:** `@field_validator("high")` kiểm tra `v < info.data["low"]` bị lỗi do `high` được parse trước trường `low` trong class definition, khiến `info.data` chưa có khóa `"low"`.
- **Khắc phục:** Chuyển sang `@model_validator(mode="after")` để kiểm tra `self.high < self.low` sau khi toàn bộ fields đã được gán giá trị hợp lệ.

---

## 4. KẾT QUẢ KIỂM THỬ THỰC TẾ

### 4.1 Unit Test Suite
- `pytest tests/test_delta_writer.py -v`: **13/13 PASSED** (Mock MinIO I/O, storage options, schema enforcement, rolling retention).

### 4.2 Data Sources Test Suite (`scripts/test_dataops_batch_sources.py`)

| Nhóm Kiểm thử | Hàm Test | Trạng thái | Ghi chú |
|---|---|:---:|---|
| **Binance Spot** | `test_binance_spot_klines` | ✅ **PASS** | Lấy 5 nến Spot BTCUSDT, bóc tách chính xác Quote Volume (row[7]) |
| **Binance Spot** | `test_binance_spot_exchange_info` | ✅ **PASS** | Lấy 3,724 symbols, 506 cặp USDT active |
| **Binance Spot** | `test_binance_spot_ticker_24hr` | ✅ **PASS** | Lấy biến động giá, khối lượng 24h |
| **Binance Spot** | `test_binance_spot_orderbook` | ✅ **PASS** | Lấy Orderbook 5 cấp, tính Spread bid/ask |
| **Binance Futures** | `test_binance_futures_funding_rate` | ✅ **PASS** | Lấy lịch sử Funding rate (chu kỳ 8h) |
| **Binance Futures** | `test_binance_futures_mark_price` | ✅ **PASS** | Lấy Mark price & Index price |
| **Binance Futures** | `test_binance_futures_open_interest` | ✅ **PASS** | Lấy tổng hợp đồng mở (OI) BTCUSDT |
| **Binance Validation** | `test_binance_pydantic_validation` | ✅ **PASS** | Bắt chính xác vi phạm High < Low qua Model Validator |
| **TCBS Loader** | `test_tcbs_constants_loader` | ✅ **PASS** | Nạp 31 mapping fields, 6 session states, index mapping |
| **TCBS Validation** | `test_tcbs_pydantic_validation` | ✅ **PASS** | Validate thành công `TCBSMarketRecord` và `TCBSBSARecord` |
| **TCBS Live Endpoints** | `test_tcbs_ticker_commons_*` | ℹ **AUTH/401** | Bắt lỗi xác thực chuẩn khi chưa truyền `TCBS_JWT_TOKEN` |

---

## 5. HƯỚNG DẪN THỰC THI CHO SESSION TIẾP THEO

1. **Khởi động cụm MinIO Lakehouse:**
   ```bash
   cd infra_dataops_finance_trading
   docker compose --env-file ../.env up -d minio minio-init
   ```

2. **Chạy Live Ingestion Job vào MinIO Delta Lake:**
   ```bash
   cd dataops_batch_api
   python -m cli.jobs.binance_1d_job 2024-01-01 2024-01-10
   python -m cli.jobs.binance_4h_job
   ```

3. **Chạy gRPC Server phục vụ Backend API:**
   ```bash
   cd dataops_batch_api
   python grpc_server.py
   ```
