# PLAN: DataOps_Backend_Financial_Trading → Production-Grade Financial Trading Platform (okx-crypto-platform)

**Goal:** Nâng chuẩn production toàn diện cho real-time financial trading data platform — Kafka KRaft hardening, Spark Structured Streaming state management, Lakehouse storage compaction, API observability, Zero-Trust security, và Data Governance.

**Scope:** Áp dụng hệ thống nguyên lý phân tán thực chiến vào Go Ingestion/Producers, Spark Streaming jobs, Delta Lake storage, InfluxDB time-series, và Go REST API.

---

## BẢN ĐỒ ĐỐI CHIẾU KIẾN TRÚC PHÂN TÁN (THEORY-TO-PRACTICE MATRIX)

| Sub-Task | Tiêu đề Kỹ thuật | Trụ cột Kiến trúc / Lý thuyết Áp dụng | Rủi ro Thực chiến Giải quyết |
|---|---|---|---|
| **ST01** | Kafka KRaft Consensus & Quorum Hardening | **CAP (CP) + Raft Quorum + Fencing Tokens** | Ngăn chặn Split-Brain và Zombie Leader ghi đè dữ liệu sau Stop-The-World GC Pause |
| **ST02** | Static Membership & Rebalance Storm Defense | **Distributed Group Coordination + Failure Detection** | Triệt tiêu vòng lặp Rebalance vô tận (Stop-The-World) khi Consumer Pods restart |
| **ST03** | Dead Letter Queue (DLQ) & Poison Pill Isolation | **Circuit Breaker + Fault Isolation (Poison Pill)** | Chống treo nghẽn Partition vĩnh viễn (Head-of-Line Blocking) do tin nhắn hỏng |
| **ST04** | CDC Replication Slot & WAL Disk Safeguard | **WAL Durability + Resource Quota Management** | Chống tràn 100% đĩa cứng và PANIC Database khi Consumer offline |
| **ST05** | Streaming Watermarking & Persistent Checkpoint | **Stateful Stream Processing + Chandy-Lamport** | Giới hạn Unbounded State trong RAM và đảm bảo phục hồi trạng thái sau sự cố |
| **ST06** | Buffer Pool Defense & Redis Cache-Aside | **RDBMS Buffer Pool LRU + PACELC (EL - Latency)** | Ngăn chặn Analytical Query làm loãng (Evict) Hot Pages trong RAM Time-series DB |
| **ST07** | Zero-Trust Auth, Token Rotation & Rate Limiting | **Zero-Trust Security + Token Bucket Algorithm** | Chặn NAT Bypass Attack, bảo vệ Core API khỏi bị DDoS/Starvation |
| **ST08** | Distributed Tracing over Kafka Headers | **W3C Distributed Trace Context + Observability** | Xác định chính xác vị trí phát sinh Latency Bottleneck qua mạng phân tán |
| **ST09** | Chaos Engineering & Bounded Buffer Fallback | **Chaos Engineering + Graceful Degradation** | Đảm bảo hệ thống duy trì tính sẵn sàng (Availability) khi Kafka gặp sự cố |
| **ST10** | Protobuf Schema Registry CI Gates | **Schema Evolution & Backward Compatibility** | Ngăn chặn Schema Drift âm thầm làm gãy toàn bộ downstream pipelines |
| **ST11** | Lakehouse Compaction, VACUUM & CoW Defense | **Columnar Parquet + Copy-On-Write (CoW) + FinOps** | Giải quyết Small Files Problem và Write Amplification trên S3/MinIO |
| **ST12** | Spark RocksDB State Store & Hot-Key Skew | **LSM-Tree State Backend + Salting + AQE** | Chống OOMKilled và GC Pause do Hot Symbols (BTC/ETH) tập trung vào 1 Node |
| **ST13** | End-to-End Idempotency & WAP Sinks | **Idempotency Key + Write-Audit-Publish (WAP)** | Triệt tiêu Duplicate Data khi Replay/Restart luồng At-Least-Once |
| **ST14** | In-Stream Data Contracts & Zero-Trust Check | **Organizational Byzantine Fault + In-Stream Guard** | Chặn đứng dữ liệu rác, giá âm, lệch timestamp từ phía sàn Upstream |
| **ST15** | Ingestion Backpressure & Bounded Ring Buffer | **Push vs Pull Architecture + Load Shedding** | Bảo vệ Go Ingestion khỏi sập RAM khi có biến động giá đột biến (Flash Crash) |
| **ST16** | Connection Multiplexing & Jittered Backoff | **Connection Pooling + Thundering Herd Prevention** | Chống cạn kiệt Connection Sockets và lỗi HTTP 429 khi gọi API Upstream |

---

## CHI TIẾT CÁC SUB-TASKS THỰC HIỆN

---

### Sub-Task 01 — Kafka KRaft Production Hardening (Fencing Token + Quorum Config)

* **Lý thuyết kiến trúc áp dụng:**
  - **CAP (CP Model):** Khi xảy ra đứt gãy mạng ($P$), hệ thống giao dịch tài chính bắt buộc phải ưu tiên tính Nhất quán ($C$). Áp dụng công thức Quorum: $W + R > N$. Với cụm $N=3$ brokers, cấu hình $W=2$ (`min.insync.replicas=2`) kết hợp Producer `acks=all` đảm bảo dữ liệu ghi vào ít nhất 2 replicas trước khi phản hồi thành công.
  - **Distributed Consensus (KRaft):** Khắc phục nhược điểm của ZooKeeper bằng Raft Event-Log nội bộ.
  - **Zombie Leader & Fencing Tokens:** Khi Node Leader cũ bị GC Pause 15 giây rồi "tỉnh dậy", nó có thể tưởng mình vẫn là Leader và ghi đè dữ liệu. Ta sử dụng cơ chế Fencing Token (Token đơn điệu tăng) lưu tại Storage/Metadata DB để từ chối các write requests từ Zombie Leader mang Token cũ.

* **Expected Outcomes:**
  - `infra_dataops_finance_trading/docker-compose.yml`: Kafka 3 brokers với `KAFKA_MIN_INSYNC_REPLICAS=2`, `KAFKA_DEFAULT_REPLICATION_FACTOR=3`.
  - Go Producer (`dataops_stream_producer`): Cấu hình `RequiredAcks = WaitForAll`, `Idempotent = true`, `MaxOpenRequests = 1`.
  - Topics được khởi tạo qua script: `--replication-factor 3 --partitions 12`.
  - Cố định `CLUSTER_ID` cho KRaft metadata log.
  - Triển khai bảng `cluster_config` với `current_token BIGINT` để kiểm soát Fencing condition.

* **Todo List:**
  1. Kiểm tra cấu hình Kafka brokers hiện tại trong `infra_dataops_finance_trading/docker-compose.yml`.
  2. Bổ sung: `KAFKA_MIN_INSYNC_REPLICAS=2`, `KAFKA_DEFAULT_REPLICATION_FACTOR=3`, `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3`.
  3. Cố định `CLUSTER_ID` trong biến môi trường docker-compose.
  4. Cập nhật Sarama config trong `dataops_stream_producer/pkg/producer/producer.go`: `Producer.RequiredAcks = sarama.WaitForAll`, `Producer.Idempotent = true`.
  5. Tạo script `infra_dataops_finance_trading/scripts/create_topics.sh` khởi tạo topics (rep=3, part=12).
  6. Tạo migration bảng `cluster_config` và logic Fencing Token check trong `dataops_backend_api`.
  7. Viết tài liệu `docs/adr/ADR-001-kafka-kraft-production.md`.

---

### Sub-Task 02 — Kafka Consumer: Static Membership + Rebalance Storm Prevention

* **Lý thuyết kiến trúc áp dụng:**
  - **Rebalance Storm:** Khi một Consumer pod bị GC pause hoặc restart nhanh trong rolling deployment, Group Coordinator coi node đó đã chết và kích hoạt Stop-The-World Rebalance toàn cụm.
  - **Static Membership (Kafka 2.3+):** Gán định danh cố định `group.instance.id` cho từng consumer instance. Khi consumer restart trong khoảng `session.timeout.ms`, partition assignment được giữ nguyên, không gây gián đoạn các consumer khác.
  - **Tuning Timeouts:** Điều chỉnh `session.timeout.ms=45000` (chịu được GC pauses), `heartbeat.interval.ms=15000`, và `max.poll.interval.ms=300000` (cho phép Spark xử lý micro-batch nặng mà không bị kích ra khỏi group).

* **Expected Outcomes:**
  - Mọi consumer (Go & Python/Spark) đều khai báo `group.instance.id = {hostname}-{job_name}`.
  - Các tham số timeout được chuẩn hóa.
  - Xuất metric Consumer Lag sang Prometheus: `kafka_consumer_lag_offset{topic, partition, group}`.
  - Triển khai Kafka Lag Exporter trong `docker-compose.yml`.

* **Todo List:**
  1. Cập nhật Kafka consumer options trong `dataops_processing/config/spark_config.py`.
  2. Cập nhật `group.instance.id` và timeouts trong Go consumers.
  3. Bổ sung service `kafka-lag-exporter` vào `infra_dataops_finance_trading/docker-compose.yml`.
  4. Tạo Grafana dashboard theo dõi Consumer Lag và cảnh báo khi lag > 10,000 tin nhắn.
  5. Viết tài liệu `docs/adr/ADR-002-kafka-static-membership.md`.

---

### Sub-Task 03 — Dead Letter Queue (DLQ) + Poison Pill Pattern

* **Lý thuyết kiến trúc áp dụng:**
  - **Poison Pill:** Một tin nhắn hỏng (sai schema JSON/Protobuf hoặc chứa giá trị gây lỗi chia cho 0) khiến consumer crash. Vì Kafka xử lý tuần tự, khi consumer restart nó lại đọc trúng tin nhắn đó $\rightarrow$ Vòng lặp crash vô tận (Head-of-Line Blocking), nghẽn toàn bộ partition.
  - **DLQ Circuit Breaker:** Bắt lỗi ngoại lệ ngay trong vòng lặp `poll()`, bắn tin nhắn độc hại sang topic `{topic}.dlq` kèm error metadata và call stack, sau đó commit offset để tiếp tục xử lý các tin nhắn khỏe mạnh.

* **Expected Outcomes:**
  - Khởi tạo các topic DLQ: `binance.trades.dlq`, `tcbs.snapshot.dlq` với retention 7 ngày (`retention.ms=604800000`).
  - Go producer & Python streaming jobs tự động định tuyến bad messages sang DLQ.
  - Prometheus alert rule kích hoạt khi `kafka_dlq_messages_total > 0`.
  - Utility script `inspect_dlq.py` phục vụ việc truy vết và phân tích root cause.

* **Todo List:**
  1. Thêm DLQ topics vào script khởi tạo Kafka topics.
  2. Implement `DLQPublisher` trong `dataops_stream_producer/pkg/producer/`.
  3. Thêm try/except và DLQ routing trong `dataops_processing/core/base_job.py`.
  4. Cập nhật cấu hình DLQ trong `kafka-connect-s3-sink.json`.
  5. Tạo script `infra_dataops_finance_trading/scripts/inspect_dlq.py`.

---

### Sub-Task 04 — CDC Monitoring: PostgreSQL/MySQL Replication Slot & Binlog Protection

* **Lý thuyết kiến trúc áp dụng:**
  - **CDC Backpressure & Disk Exhaustion:** Khi CDC Connector (Debezium) giữ một Logical Replication Slot hoặc đọc Binlog, DB không được phép xóa các log này cho đến khi có xác nhận đã đọc. Nếu Kafka hoặc Debezium bị treo, log tích tụ hàng chục GB/giờ $\rightarrow$ Tràn 100% đĩa cứng $\rightarrow$ Database PANIC và sập toàn bộ hệ thống lõi.
  - **Resource Quota:** Giới hạn `max_slot_wal_keep_size` và cấu hình binlog retention an toàn. Đứng ở góc độ Business: Thà hy sinh pipeline dữ liệu còn hơn để sập Core OLTP Database.

* **Expected Outcomes:**
  - MySQL trong docker-compose có cấu hình retention: `expire_logs_days = 3`.
  - Giới hạn kích thước WAL cho PostgreSQL: `max_slot_wal_keep_size = 4GB`.
  - Script giám sát `check_replication_lag.py` và cảnh báo Prometheus khi lag > 1GB.

* **Todo List:**
  1. Cấu hình `infra_dataops_finance_trading/config/mysql.cnf` với ROW format và log expiration.
  2. Tạo script `infra_dataops_finance_trading/monitoring/check_replication_lag.py`.
  3. Thêm alert rule trong Prometheus: `replication_lag_bytes > 1GB`.
  4. Viết tài liệu `docs/adr/ADR-003-cdc-readiness.md`.

---

### Sub-Task 05 — Spark Structured Streaming Production (Watermark + Checkpoint)

* **Lý thuyết kiến trúc áp dụng:**
  - **State Management & Late-Arriving Data:** Trong giao dịch crypto/chứng khoán, nghẽn mạng 4G/ISP khiến sự kiện lúc 10:00 đến server lúc 10:05. Nếu không có Watermark, Spark phải giữ state vô hạn trong RAM $\rightarrow$ OOMKilled.
  - **Watermarking (Bounded Staleness):** Định nghĩa khoảng trễ tối đa cho phép (ví dụ: 10 giây). Khi watermark vượt qua timestamp của window, Spark chốt sổ và giải phóng RAM.
  - **Persistent Checkpointing:** Lưu trạng thái và offset xuống S3/MinIO theo chu kỳ $N$ giây để đảm bảo khôi phục chính xác (Fault Tolerance) theo nguyên lý Chandy-Lamport.

* **Expected Outcomes:**
  - Mọi streaming job (`vwap_job.py`, `order_flow_job.py`) đều khai báo `.withWatermark("event_time", "10 seconds")`.
  - Checkpoint location: `s3a://bronze/checkpoints/{job_name}`.
  - Output mode: `append`, trigger `processingTime="5 seconds"`.
  - Xuất metrics streaming sang Prometheus.

* **Todo List:**
  1. Cập nhật `dataops_processing/jobs/vwap_job.py` và `order_flow_job.py` với watermark và checkpoint.
  2. Cấu hình MinIO S3 credentials và bucket cho checkpointing.
  3. Xây dựng `StreamingMetricsListener` để đẩy metrics về Prometheus.
  4. Viết integration test `tests/integration/test_vwap_recovery.py` (kill và restart job).

---

### Sub-Task 06 — InfluxDB Buffer Pool Defense + Read Replica & Cache Pattern

* **Lý thuyết kiến trúc áp dụng:**
  - **Buffer Pool LRU Pollution:** Cơ sở dữ liệu Time-series/RDBMS quản lý Data Pages trong RAM (Buffer Pool) theo thuật toán LRU. Khi các truy vấn phân tích (Analytics Queries) quét khối lượng lớn dữ liệu lịch sử, chúng đẩy sạch (Evict) các trang dữ liệu "nóng" (Hot Pages) ra khỏi RAM $\rightarrow$ Latency của API real-time tăng vọt do phải đọc lại từ đĩa.
  - **PACELC (EL - Latency Optimization):** Trong điều kiện mạng bình thường ($E$), ta ưu tiên giảm Latency ($L$) cho dashboard bằng kiến trúc Cache-Aside qua Redis với TTL ngắn và pattern Stale-While-Revalidate.

* **Expected Outcomes:**
  - Lớp Redis Cache Layer bọc trước InfluxDB với TTL=30s cho VWAP queries.
  - Key deterministic: `vwap:{symbol}:{interval}:{timestamp_bucket}`.
  - Giám sát InfluxDB Cardinality (cảnh báo khi series count > 1M để chống tràn RAM).
  - Target Cache Hit Ratio > 80%.

* **Todo List:**
  1. Triển khai `CacheAwareInfluxRepository` trong `dataops_backend_api/repository/`.
  2. Bổ sung Redis cache key generator và TTL cấu hình per endpoint.
  3. Thêm Prometheus metrics đếm cache hit/miss: `influxdb_cache_hits_total`, `influxdb_cache_misses_total`.
  4. Tạo script định kỳ kiểm tra series cardinality của InfluxDB.

---

### Sub-Task 07 — Go REST API: Zero-Trust Security, Token Rotation & Rate Limiting

* **Lý thuyết kiến trúc áp dụng:**
  - **Zero-Trust Architecture:** Mọi request đều phải được xác thực và phân quyền nghiêm ngặt, không giả định an toàn dù nằm trong mạng nội bộ.
  - **Token Bucket Rate Limiting per API Key:** Rate limit theo IP dễ bị bypass hoặc chặn nhầm do NAT Gateway của văn phòng/tổ chức. Ta chuyển sang rate limit dựa trên API Key/User Token lưu trong Redis Sorted Set/Counter.
  - **JWT Expiry & Refresh Token Rotation:** JWT ngắn hạn (15 phút) kết hợp Refresh Token 1 lần (Single-use rotation) thu hồi ngay lập tức trong Redis để triệt tiêu nguy cơ chiếm đoạt phiên.

* **Expected Outcomes:**
  - JWT `exp=15min`, Refresh Token rotation lưu trong Redis (`SET refresh:{user_id} token EX 604800`).
  - Rate limit: 100 req/min per API Key kèm standard HTTP headers (`X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`).
  - Structured Logging với `user_id`, `duration_ms`, `trace_id`.
  - Scrape endpoint `/metrics` tích hợp sẵn trong Gin.

* **Todo List:**
  1. Cập nhật `dataops_backend_api/middlewares/auth.go` với JWT ngắn hạn và token rotation logic.
  2. Nâng cấp `dataops_backend_api/middlewares/ratelimit.go` sang API-key-based rate limit.
  3. Thêm headers rate limit vào Gin context response.
  4. Triển khai `RequestLoggingMiddleware` với structured JSON log.
  5. Đăng ký endpoint `/metrics` và cấu hình TLS termination qua Nginx trong docker-compose.

---

### Sub-Task 08 — OpenTelemetry Distributed Tracing: Go Services + Spark Processing

* **Lý thuyết kiến trúc áp dụng:**
  - **Distributed Trace Context (W3C Standard):** Trong hệ thống Event-driven phân tán, một giao dịch đi qua nhiều ranh giới: WebSocket $\rightarrow$ Go Producer $\rightarrow$ Kafka Topic $\rightarrow$ Spark Job $\rightarrow$ InfluxDB $\rightarrow$ Go API.
  - **Kafka Header Propagation:** Nhúng `traceparent` vào metadata header của tin nhắn Kafka để liên kết toàn bộ chuỗi span, giúp định vị chính xác thành phần gây độ trễ (P99 Latency).

* **Expected Outcomes:**
  - Go services (`dataops_stream_producer`, `dataops_backend_api`) khởi tạo OTel tracer và inject context vào Kafka headers.
  - Python Spark streaming trích xuất `traceparent` từ Kafka record headers để tạo child span.
  - OTel Collector nhận traces và đẩy về Jaeger backend hiển thị trực quan.

* **Todo List:**
  1. Cài đặt OpenTelemetry SDK trong Go và Python.
  2. Xây dựng module `telemetry/tracer.go` cho Go services.
  3. Inject W3C `traceparent` vào Kafka message header khi produce.
  4. Trích xuất trace context trong Spark streaming jobs và tạo span khi ghi vào InfluxDB/Delta Lake.
  5. Thêm `otel-collector` và `jaeger` vào `docker-compose.yml` kèm file cấu hình OTel.

---

### Sub-Task 09 — Chaos Engineering: Kafka Failure & Spark Recovery Scenarios

* **Lý thuyết kiến trúc áp dụng:**
  - **Resilience Engineering:** Chứng minh tính tin cậy của hệ thống dưới các điều kiện sự cố thực tế: Broker crash, đứt cáp mạng, OOMKilled executor.
  - **Circuit Breaker & Bounded Buffer:** Khi Kafka broker tạm thời không khả dụng, Producer không được crash mà kích hoạt Circuit Breaker (`gobreaker`) và lưu tạm vào Bounded In-Memory Buffer (tối đa 10,000 tin nhắn) để retry sau đó (Graceful Degradation).

* **Expected Outcomes:**
  - Bộ script chaos: `kill_kafka_broker.sh`, `oom_spark_executor.sh`, `partition_network.sh`.
  - Circuit Breaker và bounded memory buffer trong Go Producer.
  - Runbook tài liệu hóa quy trình phục hồi và đo lường SLA thực tế (Failover < 30s).

* **Todo List:**
  1. Tạo thư mục `scripts/chaos/` với các kịch bản crash broker và cô lập mạng.
  2. Tích hợp thư viện `gobreaker` và hàng đợi đệm có giới hạn vào `dataops_stream_producer/pkg/producer/`.
  3. Đo đạc thời gian failover của KRaft leader election khi tắt 1 broker.
  4. Soạn thảo runbook vận hành: `docs/runbooks/kafka_broker_failure.md`, `docs/runbooks/spark_job_recovery.md`.

---

### Sub-Task 10 — Schema Governance: Protobuf Schema Registry CI Gates

* **Lý thuyết kiến trúc áp dụng:**
  - **Schema Evolution & Backward Compatibility:** Trong kiến trúc Event-driven, thay đổi cấu trúc dữ liệu upstream (đổi kiểu dữ liệu cột, xóa field) sẽ làm sập toàn bộ các consumers hạ tầng downstream.
  - **Protobuf/Buf CI Gates:** Áp dụng nguyên tắc chỉ được phép append field mới (tag số nguyên tăng dần), không được đổi kiểu hoặc xóa field cũ. Bắt buộc kiểm tra tương thích ngược qua CI trước khi cho phép merge code.

* **Expected Outcomes:**
  - CI Pipeline trên GitHub Actions tự động chạy `buf lint` và `buf breaking --against main`.
  - Block PR ngay lập tức nếu phát hiện breaking changes trong thư mục `dataops_protobuf/`.
  - Docker multi-arch build tự động khi merge vào nhánh chính.

* **Todo List:**
  1. Tạo cấu hình `buf.yaml` và `buf.gen.yaml` trong `dataops_protobuf/`.
  2. Xây dựng workflow `.github/workflows/ci.yml` với các jobs: `proto-check`, `go-test`, `python-test`.
  3. Thêm bước kiểm tra `buf breaking --against .git#branch=main`.
  4. Cấu hình `.pre-commit-config.yaml` cho toàn bộ repository.

---

### Sub-Task 11 — Lakehouse Operations: Small Files Compaction, VACUUM & Write Amplification Defense

* **Lý thuyết kiến trúc áp dụng:**
  - **The Small Files Problem:** Streaming nạp liên tục tạo ra hàng vạn file Parquet 10KB–100KB trên S3/MinIO. Lệnh truy vấn phân tán (Spark/Trino) bị nghẽn mạng do phải gọi hàng triệu request HTTP GET/LIST đọc footer metadata thay vì đọc dữ liệu thật.
  - **Copy-On-Write (CoW) & Write Amplification:** Update 1 dòng dữ liệu trong file Parquet 256MB bắt buộc phải ghi lại toàn bộ file mới. Cần chạy Compaction định kỳ để gom nhóm file thành kích thước chuẩn 512MB và sắp xếp lại dữ liệu theo `Z-ORDER BY (symbol, event_time)`.
  - **Garbage Collection (VACUUM):** Dọn dẹp các snapshot cũ sau 7 ngày (`retention_hours = 168`) để giải phóng dung lượng đĩa mà không làm mồ côi (Orphaned pointers) các batch job đang chạy.

* **Expected Outcomes:**
  - Scheduled Job chạy Compaction định kỳ chuyển đổi các file nhỏ thành các khối file 256MB–512MB.
  - Áp dụng `OPTIMIZE` và `Z-ORDER` trên bảng Bronze/Silver Delta Lake.
  - Cấu hình `VACUUM` an toàn với retention 7 ngày.
  - Cấu hình Partitioning theo `date` và Clustering theo `symbol`.

* **Todo List:**
  1. Xây dựng script `dataops_processing/jobs/maintenance/lakehouse_compaction.py`.
  2. Khai báo logic `OPTIMIZE silver_trades ZORDER BY (symbol, event_time)`.
  3. Thiết lập lệnh `VACUUM` với ngưỡng retention an toàn (168 giờ).
  4. Tạo cronjob/Airflow DAG chạy task bảo trì vào khung giờ thấp điểm.
  5. Viết tài liệu hướng dẫn tối ưu lưu trữ: `docs/LAKEHOUSE_MAINTENANCE_GUIDE.md`.

---

### Sub-Task 12 — Spark Memory Optimization: RocksDB State Store & Hot-Key Skew Defense

* **Lý thuyết kiến trúc áp dụng:**
  - **Hot-Key Data Skewness:** Trong thị trường crypto, các cặp giao dịch lớn như `BTCUSDT`, `ETHUSDT` chiếm hơn 80% tổng volume. Khi GroupBy/Window theo `symbol`, cơ chế Hash Partitioning đẩy toàn bộ dữ liệu của BTC về một Worker Node duy nhất $\rightarrow$ Node đó bị tràn RAM, GC Pause liên tục và dính lỗi `OOMKilled`.
  - **Salting Key & Adaptive Query Execution (AQE):** Thêm random prefix/suffix vào partition key để chia nhỏ khối lượng tính toán (Two-Phase Aggregation: Local $\rightarrow$ Global), đồng thời bật Spark 3.x AQE để tự động phát hiện và chia nhỏ skew partitions trong runtime.
  - **RocksDB StateStore Provider:** Thay thế Java Heap StateStore mặc định bằng RocksDB (LSM-Tree) để xả State từ RAM xuống ổ cứng NVMe cục bộ, loại bỏ hoàn toàn nguy cơ JVM Heap OOM khi chạy window dài.

* **Expected Outcomes:**
  - Cấu hình Spark sử dụng `RocksDBStateStoreProvider`.
  - Bật Spark AQE và tinh chỉnh advisory partition size:
    ```properties
    spark.sql.adaptive.enabled = true
    spark.sql.adaptive.skewJoin.enabled = true
    spark.sql.adaptive.advisoryPartitionSizeInBytes = 134217728
    ```
  - Áp dụng kỹ thuật Salting Key trong pipeline tính toán tổng hợp VWAP và Order Flow.

* **Todo List:**
  1. Cập nhật `spark_config.py` kích hoạt RocksDB State Backend: `spark.sql.streaming.stateStore.providerClass = org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider`.
  2. Bổ sung các thông số AQE skew join vào cấu hình Spark production.
  3. Cải tiến logic tổng hợp trong `dataops_processing/jobs/vwap_job.py` với cơ chế Salting cho các Hot Symbols.
  4. Viết unit/integration test giả lập Skew Data để kiểm chứng hiệu năng bộ nhớ.

---

### Sub-Task 13 — End-to-End Idempotency & Write-Audit-Publish (WAP) Sinks

* **Lý thuyết kiến trúc áp dụng:**
  - **At-Least-Once Delivery Semantics:** Khi consumer/streaming job restart hoặc replay dữ liệu lịch sử (backfilling), tin nhắn sẽ bị gửi lại. Nếu không có cơ chế lọc trùng (Deduplication), các chỉ số tài chính, khối lượng và số dư tài khoản sẽ bị tính đúp.
  - **Idempotency Key & Deduplication Sink:** Sinh mã `Idempotency_Key = hash(symbol + trade_id + timestamp)` cho từng bản ghi.
  - **Write-Audit-Publish (WAP) Pattern:** Khi ghi vào Data Lake/Warehouse, dữ liệu được ghi vào bảng tạm (Staging/Draft) $\rightarrow$ Chạy kiểm tra chất lượng (Audit) $\rightarrow$ Tráo đổi con trỏ (Publish) sang bảng chính thức một cách nguyên tử (Atomic Swap), đảm bảo không bao giờ xuất hiện dữ liệu rác hoặc dữ liệu trùng lặp.

* **Expected Outcomes:**
  - Mọi bản tin trade và event đều mang trường `idempotency_key` duy nhất.
  - InfluxDB/TimescaleDB sink sử dụng UPSERT/Deduplication trên `(symbol, timestamp, trade_id)`.
  - Delta Lake writer áp dụng `MERGE INTO` hoặc pattern Write-Audit-Publish.
  - Bảng MySQL sử dụng `INSERT INTO processed_events ... ON DUPLICATE KEY UPDATE`.

* **Todo List:**
  1. Thêm trường `idempotency_key` vào schema Protobuf và Go ingestion payload.
  2. Triển khai bảng `processed_events` trong MySQL để lưu vết các giao dịch đã xử lý.
  3. Cập nhật Spark writer trong `dataops_processing` sử dụng Delta `MERGE` thay vì `append` thuần túy.
  4. Xây dựng module kiểm tra WAP trước khi commit partition sang vùng Gold.

---

### Sub-Task 14 — In-Stream Data Quality Contracts & Upstream Zero-Trust Guard

* **Lý thuyết kiến trúc áp dụng:**
  - **Organizational / Exchange Byzantine Faults:** Sàn giao dịch upstream (Binance/TCBS) hoặc các service upstream có thể bị lỗi logic, bắn ra dữ liệu sai lệch (giá âm, khối lượng bằng 0, timestamp lệch hàng giờ so với thực tế, hoặc bit TCP bị méo mó).
  - **Zero-Trust In-Stream Validation:** Đội Data Platform không tin tưởng tuyệt đối vào upstream. Ta đặt một lớp Data Contract Validator ngay tại Ingestion Layer như một chiếc Circuit Breaker để cô lập các bản ghi bất thường trước khi chúng làm sai lệch toàn bộ báo cáo phân tích.

* **Expected Outcomes:**
  - Bộ quy tắc kiểm tra Data Contract được thực thi in-stream:
    - $Price > 0$ và $Quantity > 0$.
    - $|Timestamp_{event} - Timestamp_{system}| \le 5\text{ seconds}$ (Clock Drift Guard).
    - $BidPrice \le AskPrice$ (Sanity check cho Orderbook).
  - Các bản ghi vi phạm bị đẩy ngay vào `trading.contract_violations.dlq` kèm mã lý do vi phạm.

* **Todo List:**
  1. Định nghĩa file hợp đồng dữ liệu `dataops_stream_producer/config/data_contracts.yaml`.
  2. Implement module `contracts/validator.go` trong `dataops_stream_producer`.
  3. Bổ sung kiểm tra Data Contract trong bước đầu tiên của Spark Streaming jobs.
  4. Đăng ký metric `data_contract_violations_total{reason, source}` xuất sang Prometheus.

---

### Sub-Task 15 — Ingestion Backpressure & Bounded Ring Buffer for WebSockets

* **Lý thuyết kiến trúc áp dụng:**
  - **Push vs. Pull Asymmetry:** WebSocket từ sàn Binance/TCBS hoạt động theo mô hình Push. Khi thị trường xảy ra biến động mạnh (Flash Crash), lưu lượng tin nhắn có thể tăng vọt gấp 50 lần ($R_{produce} \gg R_{consume}$).
  - **Cascading Failure & Load Shedding:** Nếu Go service sử dụng Unbounded Channel, bộ nhớ RAM sẽ tăng đột ngột dẫn đến OOM Crash. Ta phải áp dụng Bounded Ring Buffer kết hợp chiến lược Coalescing (với Orderbook Snapshot: chỉ giữ lại snapshot mới nhất, loại bỏ các snapshot cũ chưa kịp gửi) và Load Shedding có kiểm soát.

* **Expected Outcomes:**
  - Kênh nhận WebSocket trong Go sử dụng Bounded Channel/Ring Buffer với kích thước cố định (ví dụ: 10,000 slots).
  - Triển khai cơ chế Coalescing Buffer cho L2 Orderbook updates.
  - Xuất metric giám sát `ws_buffer_occupancy_ratio` và `ws_dropped_messages_total`.
  - Kích hoạt cảnh báo khi buffer đạt mức tải trên 80%.

* **Todo List:**
  1. Tái cấu trúc WebSocket consumer trong `dataops_stream_producer/pkg/sources/` sử dụng Bounded Channel.
  2. Triển khai thuật toán Coalescing Buffer cho Orderbook snapshots.
  3. Thêm Prometheus gauge theo dõi dung lượng bộ đệm và rate tin nhắn bị drop.
  4. Viết benchmark test kiểm tra hiệu năng dưới tải giả lập 100,000 msgs/s.

---

### Sub-Task 16 — Database Connection Multiplexing, LRU Defense & Jittered Retry

* **Lý thuyết kiến trúc áp dụng:**
  - **Connection Starvation:** Mỗi kết nối TCP tới MySQL/PostgreSQL khởi tạo một process/thread riêng ngốn 5–10MB RAM. Nếu nhiều service pods đồng thời mở pool lớn, DB sẽ cạn kiệt bộ nhớ và từ chối kết nối.
  - **Thundering Herd & Exponential Backoff with Jitter:** Khi API của đối tác (TCBS/Binance) trả về mã lỗi HTTP 429 Too Many Requests hoặc 503, việc retry đồng loạt tại cùng một thời điểm sẽ tạo thành đợt tấn công từ chối dịch vụ (DDoS) ngược lại hệ thống. Ta bắt buộc phải áp dụng Full Jitter Exponential Backoff.

* **Expected Outcomes:**
  - Cấu hình Connection Pool chuẩn mực trong Go `sql.DB`:
    ```go
    db.SetMaxOpenConns(50)
    db.SetMaxIdleConns(25)
    db.SetConnMaxLifetime(5 * time.Minute)
    db.SetConnMaxIdleTime(1 * time.Minute)
    ```
  - HTTP Client gọi external APIs được bọc trong thuật toán Exponential Backoff kết hợp Full Jitter.
  - Giới hạn Timeout nghiêm ngặt cho toàn bộ outbound network calls.

* **Todo List:**
  1. Cập nhật cấu hình Connection Pool trong `dataops_backend_api/configuration/database.go`.
  2. Xây dựng HTTP Client utility với Exponential Backoff + Jitter trong `dataops_batch_api/pkg/client/`.
  3. Tích hợp Circuit Breaker cho mọi outbound REST calls sang TCBS/Binance.
  4. Viết unit test mô phỏng kịch bản 429 retry với Jitter.

---

## THỨ TỰ PHỤ THUỘC TRIỂN KHAI (DEPENDENCY GRAPH)

```
[ST01: KRaft Quorum] ──► [ST02: Static Membership] ──► [ST03: DLQ & Poison Pill]
        │                                                        │
        ├──► [ST10: Schema Registry CI]                          ▼
        │                                             [ST04: CDC Replication Slot]
        ▼
[ST14: Data Contracts] ──► [ST15: Ring Buffer Ingestion]
        │
        ▼
[ST05: Spark Watermark] ──► [ST12: RocksDB & Data Skew] ──► [ST13: Idempotent WAP Sinks]
        │                                                             │
        ▼                                                             ▼
[ST08: OTel Tracing] ────────────────────────────────────────► [ST11: Lakehouse Compaction]
        │
        ▼
[ST06: Influx Cache] ──► [ST07: Zero-Trust API] ──► [ST16: DB Pool & Jitter]
        │
        ▼
[ST09: Chaos Engineering] (Thực hiện cuối cùng để xác thực toàn diện)
```

---

## BẢNG THEO DÕI TRẠNG THÁI TIẾN ĐỘ (EXECUTION TRACKER)

- [ ] **ST01** — Kafka KRaft Production Hardening (Fencing Token + Quorum Config)
- [ ] **ST02** — Kafka Consumer: Static Membership + Rebalance Storm Prevention
- [ ] **ST03** — Dead Letter Queue (DLQ) + Poison Pill Pattern
- [ ] **ST04** — CDC Monitoring: PostgreSQL/MySQL Replication Slot & Binlog Protection
- [ ] **ST05** — Spark Structured Streaming Production (Watermark + Checkpoint)
- [ ] **ST06** — InfluxDB Buffer Pool Defense + Read Replica & Cache Pattern
- [ ] **ST07** — Go REST API: Zero-Trust Security, Token Rotation & Rate Limiting
- [ ] **ST08** — OpenTelemetry Distributed Tracing: Go Services + Spark Processing
- [ ] **ST09** — Chaos Engineering: Kafka Failure & Spark Recovery Scenarios
- [ ] **ST10** — Schema Governance: Protobuf Schema Registry CI Gates
- [ ] **ST11** — Lakehouse Operations: Small Files Compaction, VACUUM & Write Amplification Defense
- [ ] **ST12** — Spark Memory Optimization: RocksDB State Store & Hot-Key Skew Defense
- [ ] **ST13** — End-to-End Idempotency & Write-Audit-Publish (WAP) Sinks
- [ ] **ST14** — In-Stream Data Quality Contracts & Upstream Zero-Trust Guard
- [ ] **ST15** — Ingestion Backpressure & Bounded Ring Buffer for WebSockets
- [ ] **ST16** — Database Connection Multiplexing, LRU Defense & Jittered Retry
