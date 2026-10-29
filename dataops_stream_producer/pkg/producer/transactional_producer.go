// Package producer — phần 2: BinancePartitioner và TransactionalProducer.
//
// File này chứa 2 thành phần độc lập:
//   - BinancePartitioner: custom partitioning logic dựa trên symbol.
//   - TransactionalProducer: wrapper dùng cho Exactly-Once Semantics (EOS) với Transactional ID.
package producer

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// BinancePartitioner là custom Kafka partitioner dành riêng cho Binance & TCBS instruments.
//
// VẤN ĐỀ với default partitioner (round-robin hoặc hash trên toàn key):
//   BTC chiếm ~40% total crypto volume, ETH ~20%.
//   Nếu dùng consistent hash đơn thuần: với 6 partitions, BTC và ETH vẫn có thể
//   hash vào cùng 1 partition → partition đó nhận 60% tổng traffic → DATA SKEW.
//   Consumer đọc partition đó bị quá tải, các partition còn lại nhàn rỗi.
//
// GIẢI PHÁP: Dedicated partitions cho high-volume symbols:
//   Partition 0: BTC* (1 dedicated slot cho top-1 volume)
//   Partition 1: ETH* (1 dedicated slot cho top-2 volume)
//   Partition 2..N-1: tất cả còn lại phân phối đều qua FNV32a hash
//
// Lợi ích:
//   1. Load balancing: BTC và ETH không cạnh tranh partition với altcoins.
//   2. Dedicated consumer: Spark executor chuyên đọc P0 (BTC) không bị interrupt bởi P1+ traffic.
//   3. Ordering per symbol: BTCUSDT messages → P0 → tất cả consumers thấy đúng thứ tự thời gian.
//      (VWAP, volume bar, order flow phụ thuộc vào ordering này)
//
// FNV32a hash cho altcoins:
//   FNV (Fowler-Noll-Vo) là non-cryptographic hash, rất nhanh (~1ns/call).
//   Distribution tốt: "SOLUSDT" và "BNBUSDT" ít khi hash vào cùng partition.
//   Không cần crypto hash (SHA256) vì mục đích là distribute, không phải bảo mật.
type BinancePartitioner struct{}

// NewBinancePartitioner là factory function theo chuẩn sarama.PartitionerConstructor.
// Nhận topic name nhưng bỏ qua (routing logic không phụ thuộc vào topic).
func NewBinancePartitioner(topic string) sarama.Partitioner {
	return &BinancePartitioner{}
}

// Partition xác định partition index cho 1 message dựa trên message key (symbol ID).
// numPartitions là tổng số partitions của topic.
func (p *BinancePartitioner) Partition(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if numPartitions <= 2 {
		return 0, nil // fallback: tất cả vào partition 0 nếu topic quá nhỏ
	}

	if msg.Key == nil {
		return 0, nil
	}

	keyBytes, err := msg.Key.Encode()
	if err != nil {
		return 0, err
	}
	symbol := strings.ToUpper(string(keyBytes))

	// Dedicated partitions cho high-volume instruments (Binance & VN30 leader)
	if strings.HasPrefix(symbol, "BTC") {
		return 0, nil
	}
	if strings.HasPrefix(symbol, "ETH") {
		return 1, nil
	}

	// Các pairs/mã CK còn lại: FNV32a hash → distribute vào partition 2..N-1
	// FNV32a: non-cryptographic hash, nhanh và phân bố đều
	hasher := fnv.New32a()
	_, _ = hasher.Write(keyBytes)
	partition := int32(hasher.Sum32()%(uint32(numPartitions)-2)) + 2
	return partition, nil
}

// RequiresConsistency trả về true — cùng key luôn đến cùng partition.
// Bắt buộc phải true để đảm bảo ordering guarantee cho từng instrument.
func (p *BinancePartitioner) RequiresConsistency() bool {
	return true
}

// TransactionalProducer gửi events lên Kafka với Exactly-Once Semantics (EOS).
//
// EOS (Exactly-Once Semantics) — 3 điều kiện bắt buộc:
//   1. Idempotent producer: broker track (producer_id, sequence_number) per partition.
//      Retry cùng message → broker reject duplicate → không ghi 2 lần.
//   2. Transactional ID: định danh cố định cho producer.
//      Nếu producer crash và restart, Kafka tìm "transactional ID" này → abort
//      transaction đang dở → resume từ trạng thái sạch → không duplicate với session cũ.
//   3. acks=all + MaxOpenRequests=1: đảm bảo ordering trong pipeline (giải thích ở producer.go).
//
// SO SÁNH với kafkaProducer (producer.go):
//   kafkaProducer:         Idempotent=true, KHÔNG có TransactionalID → EOS trong 1 session.
//   TransactionalProducer: Idempotent=true + TransactionalID → EOS qua nhiều sessions/restarts.
//
// Headers tự động gắn vào mỗi message:
//   produced_at: RFC3339Nano timestamp khi producer gọi SendMessage.
//     → Dùng để đo end-to-end latency: produced_at đến consumer processing time.
//     → Phân biệt với event.TsMs (thời điểm trade thực tế trên sàn).
//   source: "dataops-stream-producer"
//     → Cho phép consumer biết message đến từ service nào (quan trọng khi có nhiều producers).
//     → Dùng cho distributed tracing (ST08 trong PLAN_REPO03).
type TransactionalProducer struct {
	producer sarama.SyncProducer
	logger   *zap.Logger
}

// NewTransactionalProducer khởi tạo Sarama SyncProducer với EOS config.
//
// transactionalID nên unique per producer instance để tránh conflict.
// Ví dụ: "binance-producer-btc-usdt-1", "binance-producer-vps-1".
// Nếu transactionalID rỗng, chỉ dùng idempotent mode (không transactional).
func NewTransactionalProducer(brokers []string, transactionalID string, logger *zap.Logger) (*TransactionalProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // acks=all: tất cả ISR phải ACK
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Net.MaxOpenRequests = 1 // bắt buộc cho idempotent producer
	config.Producer.Idempotent = true
	config.Producer.Partitioner = NewBinancePartitioner

	if transactionalID != "" {
		config.Producer.Transaction.ID = transactionalID
		config.Producer.Transaction.Retry.Max = 5
		config.Producer.Transaction.Retry.Backoff = 250 * time.Millisecond
	}

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create transactional producer: %w", err)
	}

	return &TransactionalProducer{
		producer: producer,
		logger:   logger,
	}, nil
}

// SendMessage gửi 1 message lên Kafka topic với produced_at và source headers.
//
// Trả về (partition, offset, error) — partition và offset hữu ích để trace message
// trong Kafka UI hoặc log audit.
// ctx hiện chưa được dùng bởi sarama.SyncProducer nhưng giữ lại để
// tương thích API và phòng khi upgrade lên phiên bản hỗ trợ context.
func (p *TransactionalProducer) SendMessage(ctx context.Context, topic, key string, value []byte) (int32, int64, error) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
		Headers: []sarama.RecordHeader{
			{Key: []byte("produced_at"), Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
			{Key: []byte("source"), Value: []byte("dataops-stream-producer")},
		},
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		p.logger.Error("Failed to produce message",
			zap.String("topic", topic),
			zap.String("key", key),
			zap.Error(err),
		)
		return 0, 0, err
	}

	return partition, offset, nil
}

// Close đóng Sarama producer và flush messages còn trong buffer.
// Nên gọi trong defer hoặc graceful shutdown handler.
func (p *TransactionalProducer) Close() error {
	return p.producer.Close()
}
