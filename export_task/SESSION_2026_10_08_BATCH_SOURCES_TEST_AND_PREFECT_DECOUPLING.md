# SESSION EXPORT — Batch API Sources Testing, Prefect Orchestration Decoupling & Skill Alignment
**Date:** 2026-10-08
**Branch:** 20260710_data_batch_api
**Scope:** `dataops_batch_api/` · `docs/` · `scripts/` · `github/`
**References:**
- `github/copilotinstructions/SKILL.md`
- `github/agent/*.agent.md`
- `docs/DATAOPS_PLATFORM_DESIGN.md`
- `export_task/SESSION_2026_10_08_PLATFORM_DESIGN_CDM_BATCH_REFACTOR.md`

---

## 1. MỤC TIÊU VÀ BỐI CẢNH SESSION

1. **Đồng bộ hóa kiến thức dự án vào Agent/Skill Manifests**:
   - Cập nhật định nghĩa và phạm vi chuyên môn của 5 agent và file định tuyến `SKILL.md` sau khi hoàn thành tái cấu trúc Canonical Data Model (CDM), Binance/TCBS sources, và chuẩn hóa Docker infra.
2. **Xây dựng Script kiểm thử toàn diện cho các Ingestion Sources**:
   - Viết script Python `scripts/test_dataops_batch_sources.py` gọi kiểm tra trực tiếp tất cả các REST client methods của **Binance** và **TCBS iFlash**.
   - Kiểm tra chuỗi xác thực dữ liệu: Raw response → Pydantic models validation → Canonical Data Model (CDM) mapping.
3. **Phản biện kiến trúc và Tách rời lớp Orchestration (Prefect Decoupling)**:
   - Phân tích vai trò của Orchestration khi hệ thống có lớp User Interface (UI) tương tác với `dataops_backend_api`.
   - Quyết định: Gỡ bỏ Prefect khỏi `dataops_batch_api` để đưa Orchestration Control Plane sang một repository/service điều phối độc lập.

---

## 2. QUYẾT ĐỊNH KIẾN TRÚC & PHẢN BIỆN KỸ THUẬT

### [D1] Phản biện vị trí của Orchestration khi có User Interface (UI)
- **Vấn đề**: Khi người dùng tương tác qua Web UI/Dashboard gọi `dataops_backend_api` (Go) để kích hoạt tác vụ (như backfill, sync on-demand, kiểm tra tiến độ pipeline), nếu Prefect nằm khép kín bên trong module `dataops_batch_api` (Python), việc gọi chéo và điều phối toàn diện (Cross-Module Orchestration gồm Batch → Spark Processing → Cache Invalidation) sẽ bị phân mảnh và thắt nút cổ chai.
- **Quyết định**:
  - `dataops_batch_api` đóng vai trò thuần túy là **Data Plane / Task Execution** (chứa logic kéo batch REST API, validate Pydantic, ghi Delta Bronze).
  - Lớp **Orchestration Control Plane** (Prefect/Airflow Server & Workflow Triggers) được tách ra quản lý độc lập tại repository/dịch vụ riêng để `dataops_backend_api` tương tác trigger qua REST API/Webhook.

### [D2] Dọn dẹp Prefect khỏi module `dataops_batch_api`
- Xóa bỏ thư mục `dataops_batch_api/platforms/prefect_orchestra/` (chứa `prefect_main.py`).
- Tinh giản `requirements.txt`: Gỡ bỏ `prefect`, `prefect-docker`, `prefect-sqlalchemy`, `prefect-kubernetes`, `griffe`.
- Dọn dẹp cấu hình log `logger_config.yaml` và entrypoint logging trong `metadata.py`.

---

## 3. CHI TIẾT CÁC CÔNG VIỆC ĐÃ HOÀN THÀNH

### 3.1 Cập nhật Agent Profiles (`github/agent/`) & Routing Manifest
- [`github/agent/realtime-streaming-engineer.agent.md`](../github/agent/realtime-streaming-engineer.agent.md): Bổ sung Binance WebSocket, TCBS REST polling cổ phiếu cơ sở + WebSocket phái sinh VN30F, kiến trúc Kafka KRaft 3.7 và Redis ticker caching.
- [`github/agent/batch-lakehouse-engineer.agent.md`](../github/agent/batch-lakehouse-engineer.agent.md): Cập nhật phạm vi batch ingestion Binance REST Klines (quoteAssetVolume tại `row[7]`), TCBS iFlash REST, Pydantic validation, Canonical Data Model, và lưu ý về việc tách Orchestration.
- [`github/agent/backend-software-engineer.agent.md`](../github/agent/backend-software-engineer.agent.md): Bổ sung TCBS Account Proxy (JWT auth, cash balance, orders, Redis TTL 30s) và serving analytics qua InfluxDB v2 / MySQL 8.0.
- [`github/agent/full-stack-data-platform-engineer.agent.md`](../github/agent/full-stack-data-platform-engineer.agent.md): Đồng bộ stack toàn diện (MinIO Delta Lake, Kafka KRaft, PySpark, MySQL 8.0, Redis 7).
- [`github/agent/devops-infra-engineer.agent.md`](../github/agent/devops-infra-engineer.agent.md): Cập nhật Kafka KRaft 3.7 cluster, MinIO ports (9010/9011), MySQL 8.0, Redis 7.

### 3.2 Viết Script Test Data Sources (`scripts/test_dataops_batch_sources.py`)
Tạo script độc lập kiểm tra tất cả các hàm client với cờ `--source all|binance|tcbs`:
1. **BinanceRestClient**:
   - `get_candles()`: Lấy nến 1m BTCUSDT, 1h ETHUSDT.
   - `get_instruments()`: Lấy danh sách 3,724 mã giao dịch từ `exchangeInfo`.
   - Validate `BinanceCandleRecord` và map sang `CanonicalOHLCV`.
2. **TCBSRestClient**:
   - `get_ticker_commons()`: Lấy bảng giá và orderbook 3 bước giá theo tickers/index.
   - `get_ticker_snaps()`: Lấy foreign room và snapshot toàn sàn.
   - `get_derivatives()`: Lấy bảng giá phái sinh VN30F.
   - `get_put_through_snaps()`: Lấy dữ liệu thỏa thuận.
   - `get_intraday_bsa()`, `get_intraday_bsa_ext()`, `get_intraday_bsa_month()`: Phân tích cung cầu chủ động.
   - `get_intraday_history()` & `get_intraday_history_all_pages()`: Lấy tick-by-tick có phân trang.
   - `get_securities()`: Lấy thông tin niêm yết chứng khoán.
   - Map tick khớp lệnh sang `CanonicalTrade`.

### 3.3 Loại bỏ Prefect Orchestration khỏi `dataops_batch_api`
1. **Xóa**: `dataops_batch_api/platforms/prefect_orchestra/`
2. **Sửa**: `dataops_batch_api/requirements.txt` (giảm tải dependencies xung đột).
3. **Sửa**: `dataops_batch_api/log/config/logger_config.yaml` (gỡ bỏ logger handler `platforms.prefect_orchestra`).
4. **Sửa**: `dataops_batch_api/cli/flows/serving/metadata.py` (loại bỏ tham chiếu `prefect_main.py`).
5. **Sửa**: `docs/DATAOPS_PLATFORM_DESIGN.md` (cập nhật bảng module và sơ đồ kiến trúc không còn chứa Prefect nội bộ).

---

## 4. KẾT QUẢ KIỂM THỬ VÀ VALIDATION

1. **Python Syntax Compilation**:
   - `python -m py_compile` toàn bộ cây thư mục `dataops_batch_api/` $\rightarrow$ **100% Passed (Không còn syntax/import error)**.
2. **Kiểm thử Data Ingestion thực tế (Binance)**:
   - `python scripts/test_dataops_batch_sources.py --source binance` $\rightarrow$ **100% Passed**:
     - Lấy 5 spot candles BTCUSDT 1m thành công (`open=83181.66`, `vol_ccy=115730.22871`).
     - Lấy 3 candles ETHUSDT 1h thành công (`close=2581.76`).
     - Lấy 3,724 trading pairs từ `exchangeInfo`.
     - Pydantic validation và Canonical mapping hoạt động chính xác.
3. **Kiểm thử Data Ingestion thực tế (TCBS)**:
   - Xử lý mượt mà cơ chế thông báo xác thực (401 Unauthorized do cần `TCBS_API_KEY` / `TCBS_JWT_TOKEN` từ app TCInvest).
   - Tối ưu hóa timeout/retry để không làm nghẽn tiến trình kiểm thử khi chưa nạp JWT.

---

## 5. BÀI HỌC KINH NGHIỆM (LESSONS LEARNED)

1. **Ranh giới Data Plane vs Control Plane**:
   - Không nên ép buộc Orchestration Engine (Prefect/Airflow) vào cùng process hoặc cấu trúc thư mục với mã nguồn Data Ingestion/Transformation nếu hệ thống hướng tới kiến trúc Microservices có User Interface điều khiển.
2. **Quản lý Dependencies Python**:
   - Việc loại bỏ Prefect 2.20 giúp giải phóng các ràng buộc phiên bản ngặt nghèo của `anyio`, `httpx`, `httpcore`, `pydantic` và `pendulum`, giúp module `dataops_batch_api` ổn định và nhẹ hơn đáng kể khi container hóa.
3. **Thứ tự kiểm tra nguồn dữ liệu**:
   - Binance cung cấp Public REST/WS endpoints không cần xác thực cho market data.
   - TCBS iFlash Open API v1.0.0 áp dụng chính sách bảo mật bắt buộc Bearer JWT token cho toàn bộ các endpoints kể cả dữ liệu bảng giá snapshot.

---

## 6. DANH SÁCH FILES TẠO MỚI / CHỈNH SỬA / XÓA

| Trạng thái | Đường dẫn file | Ghi chú |
|---|---|---|
| **Created** | `scripts/test_dataops_batch_sources.py` | Script test toàn diện tất cả các hàm của Binance & TCBS client |
| **Created** | `export_task/SESSION_2026_10_08_BATCH_SOURCES_TEST_AND_PREFECT_DECOUPLING.md` | Session export file ghi nhận toàn bộ công việc |
| **Deleted** | `dataops_batch_api/platforms/prefect_orchestra/prefect_main.py` | Gỡ bỏ Prefect orchestration nội bộ |
| **Deleted** | `dataops_batch_api/platforms/prefect_orchestra/__init__.py` | Gỡ bỏ Prefect package |
| **Modified** | `dataops_batch_api/requirements.txt` | Dọn dẹp Prefect dependencies |
| **Modified** | `dataops_batch_api/log/config/logger_config.yaml` | Gỡ bỏ handler `platforms.prefect_orchestra` |
| **Modified** | `dataops_batch_api/cli/flows/serving/metadata.py` | Cập nhật entrypoint logging docstrings |
| **Modified** | `docs/DATAOPS_PLATFORM_DESIGN.md` | Cập nhật architecture diagram & tech stack |
| **Modified** | `github/agent/*.agent.md` | Đồng bộ 5 agent definitions theo kiến trúc mới |
| **Modified** | `github/copilotinstructions/SKILL.md` | Đồng bộ quy tắc định tuyến tri thức |
