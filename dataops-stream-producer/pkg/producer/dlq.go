// Package producer chứa các Kafka producer implementations cho dataops-stream-producer.
package producer

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// DLQProducer gửi các message thất bại vào Dead Letter Queue topic.
//
// Dead Letter Queue (DLQ) là pattern xử lý lỗi trong messaging systems:
//   - Khi message không serialize được hoặc broker reject → ghi vào DLQ thay vì drop
//   - DLQ message chứa original payload + metadata lỗi (original topic, reason, timestamp)
//   - Operator sau đó xử lý DLQ: inspect lỗi, replay message, hoặc discard
//   - Đảm bảo zero data loss ngay cả khi có lỗi tạm thời
//
// DLQProducer dùng RequireAll ACK để đảm bảo DLQ message không mất — quan trọng
// vì đây là safety net cuối cùng trước khi data bị mất.
type DLQProducer struct {
	writer *kafka.Writer
	logger *zap.Logger
}

// NewDLQProducer tạo producer ghi vào dlqTopic trên brokers cho trước.
// RequireAll ACK được dùng để đảm bảo DLQ message được ghi vào tất cả replicas.
// Async=false để đảm bảo ghi xong trước khi hàm trả về.
func NewDLQProducer(brokers []string, dlqTopic string, logger *zap.Logger) *DLQProducer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        dlqTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, // đảm bảo DLQ không mất data
		Async:        false,            // synchronous để caller biết kết quả
	}
	return &DLQProducer{writer: writer, logger: logger}
}

// SendToDLQ gửi message thất bại vào DLQ với đầy đủ metadata để debug.
//
// Headers được gắn vào message để giúp operator hiểu context của lỗi:
//   - original-topic: topic gốc mà message nên được gửi đến
//   - failure-reason: error message từ nguyên nhân gốc (serialization, broker reject...)
//   - failed-at: thời điểm lỗi xảy ra (RFC3339 UTC)
//
// Nếu ghi DLQ cũng thất bại, log ERROR nhưng vẫn trả về error gốc (không wrap)
// để caller biết toàn bộ chuỗi thất bại.
func (d *DLQProducer) SendToDLQ(ctx context.Context, originalTopic, key string, payload []byte, failureErr error) error {
	msg := kafka.Message{
		Key:   []byte(key),
		Value: payload,
		Headers: []kafka.Header{
			{Key: "original-topic", Value: []byte(originalTopic)},
			{Key: "failure-reason", Value: []byte(failureErr.Error())},
			{Key: "failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	}

	err := d.writer.WriteMessages(ctx, msg)
	if err != nil {
		d.logger.Error("Critical: Failed to write message to DLQ",
			zap.String("original_topic", originalTopic),
			zap.Error(err),
		)
		return err
	}

	d.logger.Warn("Message routed to Dead Letter Queue (DLQ)",
		zap.String("original_topic", originalTopic),
		zap.String("key", key),
		zap.String("reason", failureErr.Error()),
	)
	return nil
}

// Close đóng Kafka writer của DLQ producer. Nên gọi khi service shutdown.
func (d *DLQProducer) Close() error {
	return d.writer.Close()
}
