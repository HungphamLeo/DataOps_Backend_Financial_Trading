# docker-compose additions — TCBS + Binance endpoint config
#
# File này ghi lại các thay đổi cần thêm vào infra_dataops_finance_trading/docker-compose.yml
# (file docker-compose.yml bị gitignore — xem infra_dataops_finance_trading/ trực tiếp để edit)
#
# Áp dụng bằng cách merge vào docker-compose.yml hiện có.

# ─── 1. Kafka Init — Thêm TCBS topics ────────────────────────────────────────
#
# Trong service `kafka-init` (hoặc `init-kafka`) thêm:
#
# kafka-topics.sh --create --if-not-exists \
#   --bootstrap-server kafka:9092 \
#   --replication-factor 1 \
#   --partitions 3 \
#   --topic tcbs.derivative.contracts
#
# kafka-topics.sh --create --if-not-exists \
#   --bootstrap-server kafka:9092 \
#   --replication-factor 1 \
#   --partitions 1 \
#   --topic tcbs.derivative.contracts.dlq
#
# kafka-topics.sh --create --if-not-exists \
#   --bootstrap-server kafka:9092 \
#   --replication-factor 1 \
#   --partitions 1 \
#   --topic tcbs.alerts.unusual-volume

# ─── 2. Environment variables — dataops_stream_producer ───────────────────────
#
# Thêm vào service `dataops-stream-producer` hoặc file .env:
#
# TCBS data sources (stream producer)
# TCBS_BASE_URL=https://openapi.tcbs.com.vn
# TCBS_WSS_URL=wss://openapi.tcbs.com.vn/api/v1/derivatives/contracts
# TCBS_API_KEY=<your_tcbs_api_key>
# TCBS_WSS_SYMBOLS=VN30F2411,VN30F2412    # Empty = disable WSS consumer
# TCBS_INDEXES=HOSE,VN30,HNX              # Index baskets to poll
# TCBS_REST_SYMBOLS=VCB,VHM,TCB,VIC,HPG   # Specific symbols for BSA/intraday

# ─── 3. Environment variables — dataops_batch_api ─────────────────────────────
#
# Thêm vào service `dataops-batch-api` hoặc file .env:
#
# Binance
# BINANCE_BASE_URL=https://api.binance.com/api/v3
# BINANCE_FUTURES_BASE_URL=https://fapi.binance.com/fapi/v1
# BINANCE_API_KEY=<your_binance_api_key>
# BINANCE_SECRET_KEY=<your_binance_secret_key>
#
# TCBS
# TCBS_BASE_URL=https://openapi.tcbs.com.vn
# TCBS_API_KEY=<your_tcbs_api_key>
# TCBS_ACCOUNT_NO=<your_tcbs_account_number>      # For account proxy endpoints
# TCBS_DERIV_ACCOUNT_NO=<your_tcbs_deriv_account> # For derivative endpoints
#
# MinIO / Delta Lake
# MINIO_ENDPOINT=http://minio:9010
# MINIO_ACCESS_KEY=minioadmin
# MINIO_SECRET_KEY=minioadmin
# MINIO_BUCKET=binance-datalake
#
# Kafka
# KAFKA_BROKERS=kafka:9092

# ─── 4. Full topic list (current state) ──────────────────────────────────────
#
# Binance topics
# - binance.trade.ticks            (12 partitions, 7d retention)
# - binance.orderbook.snapshot     (6 partitions, 1d retention)
# - binance.liquidation.events     (3 partitions, 30d retention)
# - binance.funding.rates          (3 partitions, 90d retention)
# - binance.ohlcv.batch            (3 partitions, 7d retention)
# - binance.alerts.funding-anomaly (1 partition, 7d retention)
# - binance.alerts.liquidation     (1 partition, 7d retention)
# - binance.trade.ticks.dlq
# - binance.orderbook.snapshot.dlq
# - binance.liquidation.events.dlq
#
# TCBS topics
# - tcbs.derivative.contracts      (3 partitions, 3d retention)  ← NEW
# - tcbs.alerts.unusual-volume     (1 partition, 7d retention)   ← NEW
# - tcbs.derivative.contracts.dlq                                ← NEW
#
# Platform
# - vps.market.snapshot            (6 partitions, 3d retention)
# - platform.dlq                   (3 partitions, 30d retention)

# ─── 5. Port reference (từ devops-infra-engineer.agent.md) ────────────────────
#
# MySQL:       3306
# Backend API: 8000 / 8080
# MinIO API:   9010
# MinIO UI:    9011
# Spark Master: 7087
# Spark UI:    8095
# Redis:       6379
# InfluxDB:    8086
# Kafka:       9092 (internal) / 29092 (external)
# Grafana:     3000
# Schema Registry: 8081

# ─── 6. Loki / Promtail — file-based logs của Python services (TASK-INF1) ─────
#
# `loki` + `promtail` đã có trong docker-compose.yml (scrape Docker stdout).
# promtail-config.yaml nay có thêm job `dataops-python-logfiles` đọc
# log_storage/*.log (JSON) của dataops_batch_api và dataops_processing.
# Cần mount thêm 2 volume (read-only) vào service `promtail` trong docker-compose.yml:
#
#   promtail:
#     volumes:
#       - /var/lib/docker/containers:/var/lib/docker/containers:ro
#       - /var/run/docker.sock:/var/run/docker.sock:ro
#       - ./promtail-config.yaml:/etc/promtail/config.yaml:ro
#       - ../dataops_batch_api/log/log_storage:/var/log/dataops/batch_api:ro     # NEW
#       - ../dataops_processing/log/log_storage:/var/log/dataops/processing:ro   # NEW
#
# Query mẫu (Grafana Explore → Loki):
#   {job="dataops-batch-api-file", level="ERROR"}
#   {job="dataops-processing-file"} |= "order-flow"
#
# Lưu ý: nếu services chạy trong container và ghi log vào volume riêng, đổi nguồn mount
# tương ứng; debug log không bị drop ở job file-based (chỉ job Docker stdout có drop DEBUG).
