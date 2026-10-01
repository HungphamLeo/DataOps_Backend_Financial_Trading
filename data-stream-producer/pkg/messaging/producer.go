// Package kafka cung cấp abstraction layer cho Kafka producer trong pkg/messaging.
//
// Đây là lightweight producer dùng segmentio/kafka-go với cấu hình đơn giản
// (RequireOne ACK, synchronous). Phù hợp cho internal use trong messaging layer.
// Khi cần exactly-once semantics, dùng TransactionalProducer trong pkg/producer thay thế.
package kafka

import (
	"context"
	"io"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer định nghĩa interface để publish message lên Kafka.
// Abstraction này cho phép swap implementation (mock khi test, real khi prod).
type Producer interface {
	// Publish gửi 1 message lên topic với key và value cho trước.
	// key được dùng để route message vào đúng partition (consistent hashing).
	Publish(ctx context.Context, topic string, key []byte, value []byte) error
	// Close đóng underlying Kafka writer và flush buffered messages.
	Close() error
}

// producer là concrete implementation của Producer dùng kafka.Writer.
type producer struct {
	writer *kafka.Writer
}

// NewProducer tạo Kafka producer mới kết nối đến danh sách brokers.
//
// Cấu hình:
//   - LeastBytes balancer: route message đến broker có ít bytes nhất (tối ưu throughput)
//   - RequireOne ACK: leader ACK là đủ (balance giữa durability và latency)
//   - Async=false: synchronous write, block cho đến khi broker ACK
//   - BatchTimeout=10ms: gộp messages trong 10ms để giảm round-trips
func NewProducer(brokers []string) (Producer, error) {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
		BatchTimeout: 10 * time.Millisecond,
	}
	return &producer{writer: w}, nil
}

// Publish gửi message lên Kafka topic. Blocking call — trả về sau khi broker ACK.
// ctx có thể dùng để cancel write nếu timeout hoặc shutdown.
func (p *producer) Publish(ctx context.Context, topic string, key []byte, value []byte) error {
	return p.writer.WriteMessages(ctx,
		kafka.Message{
			Topic: topic,
			Key:   key,
			Value: value,
		})
}

// Close đóng Kafka writer. Trả về io.EOF nếu writer chưa được khởi tạo.
// Nên được gọi trong defer hoặc graceful shutdown để flush buffered messages.
func (p *producer) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return io.EOF
}
