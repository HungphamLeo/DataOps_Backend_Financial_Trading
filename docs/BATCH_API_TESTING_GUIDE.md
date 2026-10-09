# HƯỚNG DẪN TEST VÀ KIỂM THỬ DATAOPS_BATCH_API

Tài liệu này hướng dẫn chi tiết quy trình kiểm thử từ tầng Unit Test (Mock I/O), Integration Test (MinIO Delta Lake), gRPC Service cho đến kiểm thử toàn diện các Batch Ingestion Jobs (Binance OHLCV & TCBS Securities) trong module `dataops_batch_api`.

---

## 1. Tổng quan chiến lược kiểm thử (Testing Strategy)

Kiến trúc kiểm thử của `dataops_batch_api` được chia làm 4 cấp độ:

```
┌────────────────────────────────────────────────────────────────────────┐
│                        CÁC CẤP ĐỘ TEST                                 │
├────────────────────────────────┬───────────────────────────────────────┤
│ 1. Unit Tests (Offline/Mock)   │ Mock toàn bộ S3 I/O, test Delta writer│
│                                │ logic, schema casting, rolling delete.│
├────────────────────────────────┼───────────────────────────────────────┤
│ 2. Integration Tests (MinIO)   │ Cần MinIO: Ingest Binance/TCBS data   │
│                                │ thực tế → ghi Delta Table trên S3A.   │
├────────────────────────────────┼───────────────────────────────────────┤
│ 3. gRPC Service & Healthcheck  │ Cần MinIO + gRPC Stubs: Expose port   │
│                                │ 50051 query Delta tables & health.    │
├────────────────────────────────┼───────────────────────────────────────┤
│ 4. Scheduler & Retention       │ Chạy APScheduler điều phối cron jobs,  │
│                                │ kiểm tra Idempotency & TTL retention. │
└────────────────────────────────┴───────────────────────────────────────┘
```

---

## 2. Yêu cầu Hạ tầng: Các Container Cần Khởi Động

Tất cả container được định nghĩa trong file [`infra_dataops_finance_trading/docker-compose.yml`](../infra_dataops_finance_trading/docker-compose.yml).

### 2.1 Bảng danh sách Container liên quan

| Tên Container (`container_name`) | Image | Port Mapping (Host:Container) | Vai trò đối với Batch API | Bắt buộc khi test |
|---|---|---|---|:---:|
| **`dataops-minio`** | `minio/minio:latest` | `9010:9000` (API)<br>`9011:9001` (Console) | Object Storage lưu trữ Delta Lake (`s3://binance-datalake/`) | **Bắt buộc** (Integration/E2E) |
| **`dataops-minio-init`** | `minio/mc:latest` | Không có (Init container) | Tự động tạo bucket `binance-datalake` & cấu trúc thư mục delta | **Bắt buộc** (Chạy 1 lần cùng MinIO) |
| **`dataops-batch-api`** | Python 3.11 (Custom Dockerfile) | `50051:50051` (gRPC) | Container chạy gRPC server và scheduler trong Docker stack | Tùy chọn (nếu test trong Docker) |
| **`dataops-backend-api`** | Go 1.27 (Custom Dockerfile) | `8000:8000` | Service Go gọi gRPC client sang Batch API | Tùy chọn (khi test E2E flow với Backend) |
| **`dataops-spark-master`** | `apache/spark:3.5.0` | `7087:7077`<br>`8095:8080` (Web UI) | Spark Master phục vụ biến đổi dữ liệu phân tán (Silver/Gold) | Tùy chọn (khi test Spark transformations) |
| **`dataops-spark-worker`** | `apache/spark:3.5.0` | Không có | Spark Worker xử lý tác vụ | Tùy chọn (khi chạy Spark job) |

> **Lưu ý về Port Harmonization:**
> MinIO API được expose ra host tại cổng **`9010`** (bên trong Docker là `9000`), MinIO Web UI Console là **`9011`**. Tránh nhầm lẫn gọi vào cổng `9000` của host.

---

### 2.2 Lệnh khởi động Container phục vụ Test

Di chuyển vào thư mục hạ tầng:
```bash
cd infra_dataops_finance_trading
```

#### A. Khởi động môi trường tối thiểu (Minimal Stack - Khuyên dùng khi test Batch API):
Chỉ cần MinIO và Init script để tạo bucket lưu Delta Lake:
```bash
docker compose up -d minio minio-init
```

Kiểm tra trạng thái container và logs tạo bucket:
```bash
docker compose ps dataops-minio dataops-minio-init
docker compose logs dataops-minio-init
```
*Kết quả hợp lệ:* `=== [minio-init] Done — bucket binance-datalake ready ===`

Truy cập Web Console MinIO để xem dữ liệu trực quan:
* **URL:** `http://localhost:9011`
* **Username:** `minioadmin`
* **Password:** `minioadmin123`

#### B. Khởi động toàn bộ Batch & Lakehouse stack:
Bao gồm MinIO, Batch API (gRPC Server) và Spark Cluster:
```bash
docker compose up -d minio minio-init spark-master spark-worker batch-api
```

---

## 3. Cấu hình Biến Môi Trường (Environment Variables)

Khi chạy test từ máy host (ngoài Docker), bạn cần thiết lập các biến môi trường sau:

### 3.1 File `.env` hoặc Export biến trên Terminal

**Dành cho PowerShell (Windows):**
```powershell
$env:AWS_ACCESS_KEY_ID="minioadmin"
$env:AWS_SECRET_ACCESS_KEY="minioadmin123"
$env:AWS_ENDPOINT_URL="http://localhost:9010"
$env:AWS_REGION="us-east-1"
$env:AWS_ALLOW_HTTP="true"
$env:AWS_S3_ALLOW_UNSAFE_RENAME="true"
$env:DELTA_BUCKET="binance-datalake"
$env:GRPC_PORT="50051"
$env:TCBS_JWT_TOKEN="<OPTIONAL_TCBS_JWT_NẾU_TEST_PRIVATE>"
```

**Dành cho Bash (Linux/macOS/Git Bash):**
```bash
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin123"
export AWS_ENDPOINT_URL="http://localhost:9010"
export AWS_REGION="us-east-1"
export AWS_ALLOW_HTTP="true"
export AWS_S3_ALLOW_UNSAFE_RENAME="true"
export DELTA_BUCKET="binance-datalake"
export GRPC_PORT="50051"
```

---

## 4. Hướng dẫn chi tiết từng bước kiểm thử

### Bước 1: Chuẩn bị môi trường Python

Cài đặt các thư viện cần thiết cho `dataops_batch_api`:
```bash
cd dataops_batch_api
pip install -r requirements.txt
pip install pytest
```

---

### Bước 2: Chạy Unit Tests (Tầng 1 - Hoàn toàn Offline, không cần container)

Tập test [`dataops_batch_api/tests/test_delta_writer.py`](../dataops_batch_api/tests/test_delta_writer.py) kiểm tra:
1. `_table_path`: Định dạng URI S3A đúng chuẩn `s3://<bucket>/delta/<layer>/<table>`.
2. `_storage_options`: Đọc chính xác biến môi trường AWS/MinIO và ném `KeyError` nếu thiếu credentials.
3. `write_records`: Ghi đúng số lượng records, bỏ qua danh sách rỗng, chuyển tiếp exception khi có lỗi I/O.
4. `apply_rolling_retention`: Cấu trúc đúng câu lệnh `delete(predicate)` theo ngày cutoff.
5. `Schema Enforcement`: PyArrow casting dữ liệu string sang float64.

**Lệnh thực thi:**
```bash
cd dataops_batch_api
pytest tests/test_delta_writer.py -v
```

*Kết quả mong đợi:*
```text
tests/test_delta_writer.py::TestTablePath::test_format PASSED
tests/test_delta_writer.py::TestTablePath::test_various_layers PASSED
tests/test_delta_writer.py::TestStorageOptions::test_reads_from_env PASSED
tests/test_delta_writer.py::TestStorageOptions::test_defaults_endpoint_and_region PASSED
tests/test_delta_writer.py::TestStorageOptions::test_missing_access_key_raises PASSED
tests/test_delta_writer.py::TestWriteRecords::test_happy_path_returns_count PASSED
tests/test_delta_writer.py::TestWriteRecords::test_write_deltalake_called_with_correct_args PASSED
tests/test_delta_writer.py::TestWriteRecords::test_empty_records_skips_write_returns_zero PASSED
tests/test_delta_writer.py::TestWriteRecords::test_exception_propagates PASSED
tests/test_delta_writer.py::TestWriteRecords::test_no_partition_by_passes_empty_list PASSED
tests/test_apply_rolling_retention::test_delete_called_with_correct_predicate PASSED
tests/test_apply_rolling_retention::test_table_not_found_logs_warning_no_raise PASSED
tests/test_delta_writer.py::TestSchemaEnforcement::test_pa_table_built_from_records_with_schema PASSED

====== 13 passed in 0.45s ======
```

---

### Bước 3: Kiểm thử Live Batch Ingestion Jobs (Tầng 2 - Cần container `dataops-minio`)

Đảm bảo container `dataops-minio` đang chạy trên cổng `9010`.

#### 3.1 Test Binance 1D Klines Job & Chunked Backfill
Hỗ trợ lấy nến ngày của các cặp Spot & Futures (`BTCUSDT`, `ETHUSDT`, `SOLUSDT`, ...).

* **Test lấy nến 1 ngày gần nhất:**
  ```bash
  python -m cli.jobs.binance_1d_job
  ```
* **Test Backfill theo khoảng thời gian tùy chọn (ví dụ 10 ngày):**
  ```bash
  python -m cli.jobs.binance_1d_job 2024-01-01 2024-01-10
  ```
* **Test Backfill nhiều năm (Tự động chia chunk 900 ngày/request):**
  ```bash
  python -m cli.jobs.binance_1d_job 2020-01-01 2024-01-01
  ```

#### 3.2 Test Binance 4H Klines Job
Lấy dữ liệu nến 4 giờ (mặc định lấy 365 ngày gần nhất với cơ chế rolling retention):
```bash
python -m cli.jobs.binance_4h_job
```

#### 3.3 Test TCBS EOD Market Snapshot Job
Lấy giá đóng cửa cuối ngày của cổ phiếu VN (HOSE, HNX, UPCOM):
```bash
python -m cli.jobs.tcbs_eod_job
```

#### 3.4 Test TCBS BSA (Buy/Sell Activity & Supply-Demand) Job
Lấy chỉ số cung cầu mua bán chủ động theo ngày:
```bash
python -m cli.jobs.tcbs_bsa_job
```

#### 3.5 Kiểm tra dữ liệu trong Delta Lake (MinIO)
Bạn có thể kiểm tra trực tiếp qua Python script kiểm tra bảng:
```python
from deltalake import DeltaTable
import os

storage_options = {
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin123",
    "AWS_ENDPOINT_URL": "http://localhost:9010",
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

dt = DeltaTable("s3://binance-datalake/delta/bronze/binance_candles_1d", storage_options=storage_options)
df = dt.to_pandas()
print(f"Tổng số bản ghi: {len(df)}")
print(df.head())
```

---

### Bước 4: Kiểm thử gRPC Server & Healthcheck (Tầng 3)

#### 4.1 Biên dịch Protobuf Stubs (Codegen)
Trước khi chạy gRPC server, sinh mã nguồn Python từ `dataops_protobuf/`:

```bash
cd dataops_protobuf
python -m grpc_tools.protoc \
    -I. \
    --python_out=../dataops_batch_api \
    --grpc_python_out=../dataops_batch_api \
    batch-data/batch_data.proto
```
*Kết quả:* Tạo 2 file `batch_data_pb2.py` và `batch_data_pb2_grpc.py` trong `dataops_batch_api/`.

#### 4.2 Khởi động gRPC Server
```bash
cd dataops_batch_api
python grpc_server.py
```
*Log hiển thị:* `gRPC server started on [::]:50051 (HistoricalDataService + grpc.health.v1)`

#### 4.3 Kiểm tra Healthcheck qua `grpc-health-probe`
Sử dụng tool `grpc-health-probe` (hoặc Python script):
```python
import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc

channel = grpc.insecure_channel('localhost:50051')
stub = health_pb2_grpc.HealthStub(channel)
response = stub.Check(health_pb2.HealthCheckRequest(service=""))
print(f"Server Health Status: {health_pb2.HealthCheckResponse.ServingStatus.Name(response.status)}")
```
*Kết quả mong đợi:* `SERVING`

---

### Bước 5: Kiểm thử Toàn bộ Hệ thống qua Docker Container

Nếu muốn kiểm tra container `dataops-batch-api` chạy độc lập trong Docker:

```bash
cd infra_dataops_finance_trading
docker compose up -d --build batch-api
```

Kiểm tra log của container:
```bash
docker compose logs -f dataops-batch-api
```

Kiểm tra tương tác giữa `dataops-backend-api` (Go) và `dataops-batch-api` (Python gRPC):
```bash
docker compose up -d backend-api
docker compose logs -f dataops-backend-api
```

---

## 5. Danh mục Kiểm tra Tính Toàn Vẹn Dữ Liệu (Checklist)

| Mục kiểm tra | Cách xác minh | Kết quả đạt |
|---|---|:---:|
| **Tính Idempotency** | Chạy job 2 lần liên tiếp với cùng tham số ngày. | Không bị trùng lặp bản ghi trong Delta table. |
| **Pydantic Validation** | Ingest payload sai kiểu dữ liệu hoặc thiếu trường bắt buộc. | Bị bắt lỗi ở tầng schema, không làm hỏng bảng Delta. |
| **Rolling Retention** | Kiểm tra partition cũ hơn số ngày retention (ví dụ 365/730 ngày). | Dữ liệu cũ được dọn dẹp bằng `DeltaTable.delete()`. |
| **Prometheus Metrics** | Kiểm tra metrics qua Pushgateway / Log `record_ingest`. | Ghi nhận đúng `status=success` hoặc `status=error`. |
| **Graceful Shutdown** | Gửi tín hiệu `SIGINT` / `Ctrl+C` đến `grpc_server.py`. | Server chuyển trạng thái `NOT_SERVING` và dừng trong 5s. |

---

## 6. Xử lý Lỗi Thường Gặp (Troubleshooting)

1. **Lỗi `ConnectionRefusedError: [Errno 111] Connection refused (Port 9000)`**
   * *Nguyên nhân:* Khi chạy code từ host máy tính, cổng MinIO là `9010`, không phải `9000`.
   * *Khắc phục:* Đảm bảo set `AWS_ENDPOINT_URL=http://localhost:9010`.

2. **Lỗi `ModuleNotFoundError: No module named 'batch_data_pb2'`**
   * *Nguyên nhân:* Chưa chạy lệnh biên dịch protobuf codegen.
   * *Khắc phục:* Thực hiện lại bước 4.1 để sinh stubs.

3. **Lỗi `DeltaTable not found` khi khởi động gRPC server**
   * *Nguyên nhân:* Bảng Delta chưa được ghi lần nào nên thư mục trên MinIO chưa tồn tại.
   * *Khắc phục:* Chạy ít nhất một lần job ingestion (ví dụ `python -m cli.jobs.binance_1d_job`) để khởi tạo bảng.

4. **Lỗi `401 Unauthorized` từ TCBS API**
   * *Nguyên nhân:* Token hoặc API key của TCBS hết hạn hoặc thiếu.
   * *Khắc phục:* Bổ sung `TCBS_JWT_TOKEN` hợp lệ vào biến môi trường hoặc chạy các endpoints public trước.
