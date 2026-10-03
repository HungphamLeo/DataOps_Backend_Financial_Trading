
# HƯỚNG DẪN INFRA DOCKER-COMPOSE, TEST SERVICES VÀ CÀI ĐẶT GOLANG TRÊN UBUNTU

## PHẦN 1: CÀI ĐẶT GOLANG TRÊN UBUNTU

### Cách 1: Cài đặt từ Official Tarball (Khuyên dùng - Chuẩn nhất cho Developer)
---
---
```bash
# 1. Cập nhật hệ thống và cài đặt công cụ cần thiết
sudo apt update && sudo apt upgrade -y
sudo apt install -y curl wget tar git

# 2. Tải bản Go 1.23.6 (hoặc phiên bản mong muốn)
GO_VERSION="1.27.0"
wget https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz

# 3. Xoá bản cũ và giải nén vào /usr/local
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go${GO_VERSION}.linux-amd64.tar.gz
rm go${GO_VERSION}.linux-amd64.tar.gz

# 4. Cấu hình biến môi trường
echo 'export PATH=$PATH:/home/hungpham/go/bin' >> ~/.bashrc
echo 'export GOPATH=$HOME/go' >> ~/.bashrc
echo 'export PATH=$PATH:$GOPATH/bin' >> ~/.bashrc
source ~/.bashrc

# 5. Kiểm tra phiên bản
go version
```
---
### Cách 2: Cài đặt nhanh qua Snap
---
---
```bash
sudo snap install --classic go
go version
```
---
## PHẦN 2: THỨ TỰ BUILD & KHỞI CHẠY INFRA DOCKER-COMPOSE

### Ma trận Port (Đã chuẩn hóa, tránh xung đột)
- **MySQL:** `3306:3306`
- **Redis:** `6379:6379`
- **InfluxDB 2.7:** `8086:8086`
- **MinIO:** `9010:9000` (S3 API), `9011:9001` (Console)
- **Kafka Brokers:** `9092`, `9093`, `9094`
- **Schema Registry:** `8081:8081`
- **Kafka UI:** `8082:8080`
- **Spark Master:** `7087:7077` (RPC), `8095:8080` (Web UI)
- **Grafana:** `3001:3000`
- **dataops-backend-api:** `8080:8080`

### Bước 1: Di chuyển vào thư mục hạ tầng
---
---
```bash
cd infra_dataops_finance_trading
```
---
### Bước 2: Khởi chạy Layer 1 & Layer 2 (Brokers, Storage, Registry)
---
---
```bash
# Khởi động cụm Kafka, DB, Redis, InfluxDB, MinIO
docker compose up -d kafka1 kafka2 kafka3 redis mysql influxdb minio

# Khởi động tiếp Schema Registry, Kafka UI, Spark, Grafana
docker compose up -d schema-registry kafka-ui spark-master spark-worker grafana

# Kiểm tra trạng thái container (đảm bảo healthy/running)
docker compose ps
```
---
### Bước 3: Build và khởi chạy Layer 3 (Application Services)
---
---
```bash
# Build và chạy đồng loạt 3 services
docker compose up -d --build dataops-stream-producer dataops-backend-api dataops-batch-api
```
---
## PHẦN 3: CÁC LỆNH TEST CHI TIẾT TỪNG SERVICE

### 1. Test Storage, Message Broker & Registry
---
---
```bash
# Kiểm tra danh sách Kafka topics qua CLI
docker compose exec kafka1 kafka-topics --bootstrap-server localhost:9092 --list

# Kiểm tra Redis
docker compose exec redis redis-cli ping

# Kiểm tra MySQL
docker compose exec mysql mysql -u root -p123456 -e "SHOW DATABASES; USE binance_analytics; SHOW TABLES;"

# Kiểm tra Web UI:
# - Kafka UI: http://localhost:8082
# - MinIO Console: http://localhost:9011 (minioadmin / minioadmin)
# - Spark Master UI: http://localhost:8095
# - Grafana: http://localhost:3001
```
---
### 2. Test `dataops-stream-producer` (Real-time Stream Ingestion)
---
---
```bash
# Xem log kết nối WebSocket Binance / Poller TCBS
docker compose logs -f --tail=100 dataops-stream-producer

# Kiểm tra tin nhắn thực tế đổ về Kafka topic
docker compose exec kafka1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic binance.trade.ticks \
  --from-beginning \
  --max-messages 5
```
---
### 3. Test `dataops_backend_api` (REST API & Metrics)
---
---
```bash
# Xem log khởi động Backend API
docker compose logs -f --tail=100 dataops-backend-api

# Kiểm tra Healthcheck
curl -i http://localhost:8080/health

# Kiểm tra Prometheus Metrics
curl -i http://localhost:8080/metrics

# Kiểm tra API Crypto Trade
curl -i http://localhost:8080/api/v1/crypto/trades/BTCUSDT
```
---
### 4. Test `dataops_batch_api` (PySpark & Delta Lake Pipeline)
---
---
```bash
# Xem log Batch API container
docker compose logs -f --tail=100 dataops-batch-api

# Exec vào container chạy thử bronze flow
docker compose exec dataops-batch-api python -m cli.flows.serving.bronze_flow
```
---
## PHẦN 4: LỆNH DỌN DẸP & RESTART

---
```bash
# Xem log service cụ thể
docker compose logs -f <service_name>

# Rebuild 1 service sau khi sửa code
docker compose up -d --build <service_name>

# Dừng toàn bộ hệ thống
docker compose down

# Dừng và xoá sạch Volumes
docker compose down -v
```
---
