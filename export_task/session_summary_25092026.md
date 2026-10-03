# SESSION LOG & ARCHITECTURE REFACTOR SUMMARY

> **Dự án:** DataOps Backend Financial Trading (OKX Crypto + VPS Securities VN)  
> **Thời gian:** 25/09/2026  
> **Mục tiêu phiên:** Tách rã Monolithic codebase `BackendDataPlatform`, phân rã thành các Microservices độc lập theo chuẩn Production DataOps / Data Fabric, xây dựng sổ tay kiến thức và bộ công cụ debug cho Fresher.

---

## 1. BỐI CẢNH & YÊU CẦU ĐẦU VÀO

1. **Vấn đề ban đầu:**
   - Dự án gốc `BackendDataPlatform` bị nhồi nhét tất cả thành phần (Batch, Streaming, API, Ingestor, Platform utils) vào chung 1 monolithic repo, gây khó khăn cho việc quản trị, bảo trì, debug và mở rộng.
   - Chưa phân tách rõ ràng bounded context của từng microservice theo thiết kế chuẩn trong [`OKX_CRYPTO_PLATFORM_DESIGN.md`](../OKX_CRYPTO_PLATFORM_DESIGN.md).
2. **Yêu cầu của User & Tiêu chuẩn Production:**
   - Tách rời hoàn toàn các thành phần của **Indicators** (Params riêng, Calculation Logic riêng, Data Processing riêng).
   - Tách nhỏ và phân bổ toàn bộ mã nguồn sang các folder/service độc lập.
   - Quản trị tập trung các Utils dùng chung (Config, Load Env) và Observability (Logging Zap, Prometheus Metrics).
   - Tạo riêng phần quản trị Hạ tầng (`infra/`, `docker-compose.yml`, `infra/k8s/`).
   - Cấu hình `.env.example` chứa đầy đủ thông số kết nối sàn OKX và VPS.
   - Bổ sung xác thực HMAC SHA-256 cho sàn và JWT RBAC cho API.
   - Áp dụng Interface, Zap Logger, Dependency Injection và Data Fabric Lineage Tracking để hỗ trợ Fresher dễ debug từng giai đoạn.
   - Tạo sổ tay kiến thức và cẩm nang thực chiến chi tiết.

---

## 2. CÁC CÔNG VIỆC ĐÃ TRIỂN KHAI VÀ HOÀN THÀNH

### 2.1. Phân tách và Tái cấu trúc Technical Indicators (`okx-batch-ingestion`)
Đã chia nhỏ module tính toán chỉ báo thành 3 lớp độc lập:
- [`okx-batch-ingestion/indicators/params.py`](../okx-batch-ingestion/indicators/params.py): Quản lý cấu hình tham số (`SMAParams`, `EMAParams`, `RSIParams`, `ATRParams`, `BBandsParams`, `MACDParams`, `IndicatorConfig`).
- [`okx-batch-ingestion/indicators/calculator.py`](../okx-batch-ingestion/indicators/calculator.py): Pure calculation logic (chỉ nhận Pandas Series / Numpy Arrays và trả về kết quả, không phụ thuộc I/O hay database).
- [`okx-batch-ingestion/indicators/processor.py`](../okx-batch-ingestion/indicators/processor.py): Bộ xử lý gắn kết các chỉ báo vào DataFrame chuẩn hóa.
- [`okx-batch-ingestion/sources/okx_client.py`](../okx-batch-ingestion/sources/okx_client.py): OKX REST API Client hỗ trợ ký HMAC và fetch historical candles.
- [`okx-batch-ingestion/delta/`](../okx-batch-ingestion/delta/): Pydantic Schema Validation ([`schema.py`](../okx-batch-ingestion/delta/schema.py)) và Idempotent Delta Bronze Writer ([`writer.py`](../okx-batch-ingestion/delta/writer.py)) ghi dữ liệu lên MinIO theo partition `symbol/date`.

### 2.2. Nâng cấp Stream Ingestion Layer (`okx-stream-producer`)
- **HMAC Signature:** [`okx-stream-producer/pkg/sources/okx/auth.go`](../okx-stream-producer/pkg/sources/okx/auth.go) sinh chữ ký chuẩn cho WebSocket Private channel và REST API.
- **Custom Partitioner:** [`okx-stream-producer/pkg/producer/transactional_producer.go`](../okx-stream-producer/pkg/producer/transactional_producer.go) định tuyến BTC vào Partition 0, ETH vào Partition 1 để tránh Head-of-Line Blocking.
- **Transactional Producer (EOS):** Cấu hình `acks=all`, `Idempotent=true`, `MaxOpenRequests=1` đảm bảo Exactly-Once Semantics và không mất dữ liệu.
- **Dead Letter Queue (DLQ):** [`okx-stream-producer/pkg/producer/dlq.go`](../okx-stream-producer/pkg/producer/dlq.go) chuyển hướng các tin nhắn lỗi sang topic `platform.dlq`.
- **Enterprise Messaging:** Di chuyển và chuẩn hóa codec, retry policy, headers từ `BackendDataPlatform/internal/platform/messaging/kafka` sang [`okx-stream-producer/pkg/messaging/`](../okx-stream-producer/pkg/messaging/).

### 2.3. Nâng cấp Serving & Analytics API Layer (`okx-analytics-api`)
- **JWT RBAC & Redis Blacklist:** [`okx-analytics-api/middlewares/jwt_auth.go`](../okx-analytics-api/middlewares/jwt_auth.go) hỗ trợ phân quyền người dùng và thu hồi token tức thì qua Redis Blacklist.
- **Rate Limiting:** [`okx-analytics-api/middlewares/rate_limiter.go`](../okx-analytics-api/middlewares/rate_limiter.go) dùng Redis Sliding Window / Token Bucket ngăn chặn DDoS.
- **WebSocket Alert Broadcast Hub:** [`okx-analytics-api/api/rest/alert_ws.go`](../okx-analytics-api/api/rest/alert_ws.go) cung cấp endpoint `/api/v1/ws/alerts` để broadcast cảnh báo theo thời gian thực tới client.
- **Observability Middleware:** Tích hợp Zap Logger và Prometheus metrics đếm request, latency P99.

### 2.4. Core Platform Packages Dùng Chung (`shared/platform/`)
- [`shared/platform/config/config.go`](../shared/platform/config/config.go): Nạp và quản lý toàn bộ cấu hình hệ thống từ biến môi trường (OKX, VPS, Kafka, Redis, MySQL, InfluxDB, MinIO, JWT).
- [`shared/platform/observability/observability.go`](../shared/platform/observability/observability.go): Logger chuẩn JSON có cấu trúc + Prometheus Metrics Registry (HTTP requests, Kafka produced/consumed, Consumer lag, Exchange latency).
- [`shared/platform/fabric/lineage.go`](../shared/platform/fabric/lineage.go): Data Fabric Lineage Tracker theo dõi vết dữ liệu qua 6 Stages chuẩn.
- [`shared/platform/di/interfaces.go`](../shared/platform/di/interfaces.go): Định nghĩa các Interface trừu tượng (Logger, MessageProducer, CacheService, AnalyticsRepository) cho Dependency Injection.

### 2.5. Real-Time PySpark Processing Engine
- [`okx-batch-ingestion/pipeline/spark_jobs.py`](../okx-batch-ingestion/pipeline/spark_jobs.py):
  1. **VWAP Sliding Window:** Window 5 phút, slide 1 phút, Watermark 30s xử lý late data, ghi Delta Lake Silver.
  2. **Funding Rate Rolling Z-Score Anomaly:** Tính toán $Z = \frac{X - \mu}{\sigma}$, khi $|Z| \ge 2.5$ tự động phát sinh cảnh báo đẩy vào topic `okx.alerts.funding-anomaly`.

### 2.6. Quản trị Hạ tầng & Tài liệu Hướng dẫn
- [`.env.example`](../.env.example): Cung cấp mẫu cấu hình môi trường đầy đủ các thông số kết nối sàn OKX và VPS.
- [`infra/`](../infra/): Phân tách hạ tầng riêng biệt gồm K8s manifests, Ingress, Gateway, Load Balancer.
- [`grafana/dashboards/`](../grafana/dashboards/) & [`grafana/alerts/`](../grafana/alerts/): Dashboards và Prometheus alert rules.
- [`scripts/chaos_test.sh`](../scripts/chaos_test.sh): Kịch bản Chaos Testing tự động mô phỏng sập Broker Leader và kiểm tra 0 message loss.
- [`STAGE_DEBUG_GUIDE.md`](../STAGE_DEBUG_GUIDE.md): Hướng dẫn chi tiết quy trình debug độc lập cho 6 Stages trong hệ thống.
- [`DATAOPS_FINANCIAL_TRADING_HANDBOOK.md`](../DATAOPS_FINANCIAL_TRADING_HANDBOOK.md): Sổ tay lý thuyết và thực chiến toàn diện từ Zero-to-Hero giải thích cặn kẽ bản chất công nghệ và lý do viết từng đoạn code.

---

## 3. CẤU TRÚC THƯ MỤC DỰ ÁN SAU KHI HOÀN THIỆN

```
DataOps_Backend_Financial_Trading/
├── .env.example                                  ← Cấu hình đầy đủ OKX, VPS, Kafka, Redis, DB
├── docker-compose.yml                            ← HA KRaft 3 Brokers, MinIO, InfluxDB, Redis, MySQL, Grafana
├── kafka-connect-s3-sink.json                    ← Cấu hình Kafka Connect stream sang MinIO Parquet
├── STAGE_DEBUG_GUIDE.md                          ← Cẩm nang debug 6 stages cho Fresher
├── DATAOPS_FINANCIAL_TRADING_HANDBOOK.md         ← Sổ tay học thuật & thực chiến chi tiết
├── export_task/                                  ← Lưu trữ log và bài học phiên làm việc
│
├── okx-stream-producer/                          ← [T2] Go 1.22 Stream Ingestion
│   ├── app/                                      ← Server lifecycle & signal handling
│   ├── constants/                                ← Topics & error constants
│   ├── monitoring/                               ← Kafka consumer lag monitor
│   ├── pkg/
│   │   ├── configuration/                        ← Service config
│   │   ├── messaging/                            ← Kafka headers, retry, codec
│   │   ├── producer/                             ← Transactional EOS, Partitioner, DLQ
│   │   ├── schemas/                              ← Avro schema definitions (.avsc)
│   │   └── sources/
│   │       ├── okx/                              ← WebSocket producer & HMAC auth
│   │       └── vps/                              ← VPS poller & session management
│   └── utils/                                    ← Logger, env, time helpers
│
├── okx-batch-ingestion/                          ← [T1 & T3] Python Batch Ingestion & PySpark
│   ├── delta/                                    ← Pydantic schemas & Delta Bronze writer
│   ├── indicators/                               ← Phân tách: params.py, calculator.py, processor.py
│   ├── pipeline/
│   │   ├── ingestion.py                          ← Batch ingestion flow
│   │   └── spark_jobs.py                         ← PySpark VWAP & Z-score streaming jobs
│   └── sources/                                  ← OKX, VPS, CoinGecko REST clients
│
├── okx-analytics-api/                            ← [T4] Go Gin Serving & Analytics API
│   ├── api/rest/                                 ← Handlers (Crypto, VNStock, Auth, Alert WS Hub)
│   ├── middlewares/                              ← JWT RBAC, Redis Rate Limit, CORS, Tracing
│   ├── observability/                            ← Zap Logger, Prometheus metrics
│   ├── repository/                               ← GORM MySQL, InfluxDB flux, Delta/DuckDB
│   ├── services/                                 ← Business logic & DI container
│   └── queue/                                    ← Priority queue & in-memory structures
│
├── shared/                                       ← Platform Core Packages dùng chung
│   └── platform/
│       ├── config/                               ← Centralized configuration loader
│       ├── di/                                   ← Abstract interfaces cho DI & Wire
│       ├── fabric/                               ← Data Fabric Lineage Tracker
│       └── observability/                        ← Centralized Logging & Metrics
│
├── okx-protobuf/                                 ← Shared Protobuf contract definitions
│   ├── analytics/                                ← analytics.proto
│   └── vnstock/                                  ← vnstock.proto
│
├── infra/                                        ← Hạ tầng độc lập
│   ├── gateway/                                  ← API Gateway configurations
│   ├── load_balancer/                            ← Load balancer configs
│   └── k8s/                                      ← K8s Deployments, Services, Ingress, Secrets
│
└── grafana/                                      ← Monitoring & Dashboards
    ├── alerts/                                   ← Prometheus alert rules
    └── dashboards/                               ← Real-time VWAP & Market dashboards
```

---

## 4. BÀI HỌC KINH NGHIỆM & HƯỚNG DẪN DUY TRÌ (LESSONS LEARNED)

1. **Tuân thủ Bounded Context:**
   - Không chia sẻ database trực tiếp giữa các service. Giao tiếp giữa các service phải đi qua Kafka (Events/Streaming) hoặc gRPC/REST.
2. **Nguyên tắc Idempotency trong Data Lakehouse:**
   - Luôn partition dữ liệu Bronze/Silver theo `symbol/date` để khi re-run pipeline không sinh ra bản ghi trùng lặp.
3. **Quản trị Schema đồng bộ:**
   - Khi thay đổi cấu trúc dữ liệu, phải cập nhật file `.avsc` trong `okx-stream-producer/pkg/schemas/` và đăng ký lên Schema Registry trước khi sửa code xử lý ở Spark hay API.
4. **Debug theo Stage:**
   - Luôn tra cứu [`STAGE_DEBUG_GUIDE.md`](../STAGE_DEBUG_GUIDE.md) và log lineage để xác định chính xác lỗi phát sinh ở tầng nào (Ingestion, Kafka, Spark, MinIO hay API) trước khi sửa code.
