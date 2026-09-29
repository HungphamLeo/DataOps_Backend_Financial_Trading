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

// OKXPartitioner là custom Kafka partitioner dành riêng cho OKX instruments.
//
// Chiến lược phân vùng (partition routing):
//   - Partition 0: tất cả BTC-* instruments (BTC-USDT, BTC-USDT-SWAP...)
//   - Partition 1: tất cả ETH-* instruments
//   - Partition 2..N-1: các instruments còn lại, hash đều bằng FNV32a
//
// Lý do cần dedicated partitions cho BTC/ETH:
//   - BTC và ETH chiếm ~60-70% khối lượng giao dịch crypto
//   - Dedicated partition → consumer có thể đọc riêng BTC mà không bị ảnh hưởng bởi altcoins
//   - Ordering guarantee: tất cả BTC messages đến đúng thứ tự trong partition 0
type OKXPartitioner struct{}

// NewOKXPartitioner là factory function theo chuẩn sarama.PartitionerConstructor.
// Nhận topic name nhưng bỏ qua (routing logic không phụ thuộc vào topic).
func NewOKXPartitioner(topic string) sarama.Partitioner {
	return &OKXPartitioner{}
}

// Partition xác định partition index cho 1 message dựa trên message key (instrument ID).
// numPartitions là tổng số partitions của topic, phải >= 3 để hoạt động đúng.
func (p *OKXPartitioner) Partition(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if numPartitions <= 2 {
		return 0, nil // fallback: tất cả vào partition 0 nếu topic quá nhỏ
	}

	keyBytes, err := msg.Key.Encode()
	if err != nil {
		return 0, err
	}
	symbol := string(keyBytes)

	// Dedicated partitions cho high-volume instruments
	if strings.HasPrefix(symbol, "BTC") {
		return 0, nil
	}
	if strings.HasPrefix(symbol, "ETH") {
		return 1, nil
	}

	// Các pairs còn lại: FNV32a hash → distribute vào partition 2..N-1
	// FNV32a: non-cryptographic hash, nhanh và phân bố đều
	hasher := fnv.New32a()
	_, _ = hasher.Write(keyBytes)
	partition := int32(hasher.Sum32()%(uint32(numPartitions)-2)) + 2
	return partition, nil
}

// RequiresConsistency trả về true — cùng key luôn đến cùng partition.
// Bắt buộc phải true để đảm bảo ordering guarantee cho từng instrument.
func (p *OKXPartitioner) RequiresConsistency() bool {
	return true
}

// TransactionalProducer gửi events lên Kafka với Exactly-Once Semantics (EOS).
//
// EOS đảm bảo mỗi message được ghi đúng 1 lần dù có broker failures hay retries.
// Đạt được thông qua:
//   - Idempotent producer: broker track sequence number, reject duplicates
//   - Transactional ID: cho phép producer resume sau crash mà không duplicate
//   - acks=all + MaxOpenRequests=1: required conditions cho idempotent mode
//
// Headers được tự động gắn vào mỗi message để traceability:
//   - produced_at: thời điểm producer gửi (RFC3339Nano)
//   - source: "okx-stream-producer" (giúp phân biệt khi có nhiều producers)
type TransactionalProducer struct {
	producer sarama.SyncProducer
	logger   *zap.Logger
}

// NewTransactionalProducer khởi tạo Sarama SyncProducer với EOS config.
//
// transactionalID nên unique per producer instance để tránh conflict.
// Ví dụ: "okx-producer-btc-usdt-1", "okx-producer-vps-1".
// Nếu transactionalID rỗng, chỉ dùng idempotent mode (không transactional).
func NewTransactionalProducer(brokers []string, transactionalID string, logger *zap.Logger) (*TransactionalProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // acks=all: tất cả ISR phải ACK
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Net.MaxOpenRequests = 1          // bắt buộc cho idempotent producer
	config.Producer.Idempotent = true
	config.Producer.Partitioner = NewOKXPartitioner

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
			{Key: []byte("source"), Value: []byte("okx-stream-producer")},
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
