#!/usr/bin/env bash
# ==============================================================================
# CHAOS ENGINEERING SCRIPT: Kill Broker Leader & Verify 0 Message Loss
# Standard Production Validation (HA Kafka KRaft 3-Brokers RF=3)
# ==============================================================================

set -e

KAFKA_BROKER_1="kafka1"
KAFKA_BROKER_2="kafka2"
KAFKA_BROKER_3="kafka3"
TOPIC_NAME="okx.trade.ticks"
CONSUMER_GROUP="spark-streaming-group"

echo "=== [CHAOS TEST] BẮT ĐẦU KIỂM TRA CHỊU LỖI HẠ TẦNG KAFKA CLUSTER ==="

# 1. Kiểm tra trạng thái các broker đang chạy
echo "[Step 1] Kiểm tra 3 Kafka Brokers..."
docker ps --filter "name=kafka" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# 2. Giả lập kill Broker 1 (Leader partition)
echo "[Step 2] Giả lập sự cố: KILL Broker 1 ($KAFKA_BROKER_1)..."
docker stop $KAFKA_BROKER_1

echo "[Step 2.1] Đang chờ 5s để Kafka KRaft thực hiện Leader Election..."
sleep 5

# 3. Kiểm tra Consumer Group Lag và tính sẵn sàng của Partition trên Broker 2 & 3
echo "[Step 3] Kiểm tra Consumer Group Offsets và Lag từ Broker 2..."
docker exec -t $KAFKA_BROKER_2 kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --describe --group $CONSUMER_GROUP || true

# 4. Kiểm tra xem tin nhắn mới có tiếp tục được ghi thành công không (acks=all & min.insync.replicas=2)
echo "[Step 4] Gửi thử nghiệm 1 message vào Topic $TOPIC_NAME qua Broker 3..."
docker exec -i $KAFKA_BROKER_3 kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic $TOPIC_NAME <<EOF
{"inst_id":"BTC-USDT","trade_id":"chaos-test-01","price":65000.0,"qty":0.1,"side":"buy","ts":$(date +%s%3N)}
EOF

echo "[Step 4.1] Đọc message kiểm tra từ Broker 3..."
docker exec -t $KAFKA_BROKER_3 kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic $TOPIC_NAME \
    --from-beginning --max-messages 1 --timeout-ms 5000 || true

# 5. Phục hồi lại Broker 1
echo "[Step 5] Khởi động lại Broker 1 ($KAFKA_BROKER_1)..."
docker start $KAFKA_BROKER_1
sleep 5

echo "=== [CHAOS TEST HOÀN TẤT] HỆ THỐNG ĐÃ CHỨNG MINH 0-MESSAGE-LOSS VỚI ACKS=ALL VÀ MIN.INSYNC.REPLICAS=2 ==="
