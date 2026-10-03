# ==============================================================================
# HƯỚNG DẪN KIẾN TRÚC VÀ DEBUG TỪNG GIAI ĐOẠN (STAGE-BY-STAGE DEBUG GUIDE)
# Dành cho Fresher & DataOps Engineer
# ==============================================================================

Tài liệu này giải thích chi tiết luồng dữ liệu (Data Fabric Pipeline) 6 giai đoạn, cách ly lỗi độc lập và quy trình debug từ Source đến API.

---

## 1. Bản đồ luồng dữ liệu (Data Fabric Architecture)

```
[OKX WS / VPS Poller] 
        │ (Stage 1: Ingestion & Auth)
        ▼
[okx-stream-producer] ──(Transactional acks=all)──► [Kafka KRaft 3 Brokers]
                                                            │ (Stage 2: Buffer)
        ┌───────────────────────────────────────────────────┼───────────────────────────────┐
        ▼                                                   ▼                               ▼
[okx-spark-streaming]                             [Kafka Connect S3 Sink]            [platform.dlq]
  - VWAP Sliding Window (5m/1m, Watermark 30s)      - Stream to Parquet MinIO          (Error Ticks)
  - Z-score Funding Anomaly (Z >= 2.5)                      │ (Stage 4: Bronze)
        │ (Stage 3: Processed)                              ▼
        ├─────────────────────────────┐             [MinIO Delta Lake Bronze]
        ▼                             ▼
[Delta Lake Silver]           [InfluxDB 2.x]
        │                             │
        └──────────────┬──────────────┘
                       │ (Stage 5 & 6: Serving & Observability)
                       ▼
             [okx-analytics-api]
               ├── REST API (/api/v1/crypto/*, /api/v1/vn-stocks/*)
               └── WebSocket Hub (/api/v1/ws/alerts)
```

---

## 2. Quy trình Debug và Kiểm tra theo 6 Stages

### Stage 1: Ingestion & Exchange Authentication (`okx-stream-producer`)
* **Mục tiêu:** Kiểm tra WebSocket OKX hoặc VPS Poller có nhận được tick thực tế và ký HMAC đúng không.
* **Cách debug:**
  1. Kiểm tra biến môi trường sàn trong `.env`:
     ```bash
     OKX_API_KEY=...
     OKX_SECRET_KEY=...
     OKX_PASSPHRASE=...
     ```
  2. Xem log Zap structured của `okx-stream-producer`:
     ```bash
     # Tìm log lineage Stage 1:
     grep "STAGE_1_INGESTION" app.log
     ```
  3. Lỗi thường gặp: Sàn trả về `401 Unauthorized` hoặc `Timestamp invalid` $\rightarrow$ kiểm tra NTP time sync của máy tính hoặc Passphrase.

---

### Stage 2: Message Buffer & Partitioner (`Kafka KRaft`)
* **Mục tiêu:** Đảm bảo tick BTC vào Partition 0, ETH vào Partition 1 và tin nhắn lỗi bị đẩy về `platform.dlq`.
* **Cách debug:**
  1. Mở Kafka UI trên trình duyệt tại: `http://localhost:8080` (hoặc `http://localhost:8085`).
  2. Kiểm tra topic `okx.trade.ticks` xem message count có tăng đều không.
  3. Kiểm tra topic `platform.dlq`: Nếu có message trong này, đọc header `failure-reason` để biết nguyên nhân (lỗi parse Avro/JSON hay timeout).

---

### Stage 3: Real-time Processing Engine (`PySpark Structured Streaming`)
* **Mục tiêu:** Kiểm tra PySpark có nhận stream từ Kafka và tính toán VWAP, Z-score chính xác không.
* **Cách debug:**
  1. Chạy spark job cục bộ:
     ```bash
     cd okx-batch-ingestion
     python pipeline/spark_jobs.py
     ```
  2. Kiểm tra log Watermark: Nếu dữ liệu đến trễ quá 30 giây (`Watermark 30s`), Spark sẽ bỏ qua.
  3. Kiểm tra Kafka topic `okx.alerts.funding-anomaly` xem có alert phát sinh khi funding rate tăng vọt.

---

### Stage 4 & 5: Delta Lakehouse Storage (`MinIO`) & InfluxDB
* **Mục tiêu:** Kiểm tra dữ liệu được ghi vào Data Lake (Bronze/Silver) theo chuẩn ACID và InfluxDB metrics.
* **Cách debug:**
  1. Truy cập MinIO Console: `http://localhost:9001` (User: `minioadmin` / Pass: `minioadmin`).
  2. Kiểm tra bucket `okx-delta/bronze/ohlcv/` và `okx-delta/silver/vwap/`.
  3. Mở InfluxDB UI: `http://localhost:8086`, vào Data Explorer và gõ câu lệnh Flux:
     ```flux
     from(bucket: "market_analytics")
       |> range(start: -1h)
       |> filter(fn: (r) => r._measurement == "vwap_metrics")
     ```

---

### Stage 6: Analytics API & WebSocket Alerts (`okx-analytics-api`)
* **Mục tiêu:** Client nhận dữ liệu qua REST và WebSocket nhanh chóng, bảo mật qua JWT.
* **Cách test:**
  1. Login lấy JWT Token:
     ```bash
     curl -X POST http://localhost:8080/api/v1/login \
          -H "Content-Type: application/json" \
          -d '{"username":"admin","password":"password123"}'
     ```
  2. Gọi API VWAP có đính kèm Bearer Token:
     ```bash
     curl -X GET "http://localhost:8080/api/v1/crypto/vwap?symbol=BTC-USDT" \
          -H "Authorization: Bearer <TOKEN_O_BUOC_1>"
     ```
  3. Kết nối WebSocket nhận cảnh báo thời gian thực:
     `ws://localhost:8080/api/v1/ws/alerts`

---

## 3. Lệnh Chaos Testing (Kiểm tra chịu tải & mất kết nối Broker)

Chạy file script `scripts/chaos_test.sh` để giả lập Broker Leader bị sập mà hệ thống vẫn không bị mất tin nhắn (`0 message loss`).
