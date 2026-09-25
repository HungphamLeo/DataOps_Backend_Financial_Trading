package producer

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// DLQProducer quản lý Dead Letter Queue cho các message bị lỗi định dạng hoặc broker reject
type DLQProducer struct {
	writer *kafka.Writer
	logger *zap.Logger
}

func NewDLQProducer(brokers []string, dlqTopic string, logger *zap.Logger) *DLQProducer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        dlqTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
	return &DLQProducer{writer: writer, logger: logger}
}

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

func (d *DLQProducer) Close() error {
	return d.writer.Close()
}
