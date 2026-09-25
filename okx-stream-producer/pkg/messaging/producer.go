package kafka

import (
	"context"
	"io"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer interface {
	Publish(ctx context.Context, topic string, key []byte, value []byte) error
	Close() error
}

type producer struct {
	writer *kafka.Writer
}

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

func (p *producer) Publish(ctx context.Context, topic string, key []byte, value []byte) error {
	return p.writer.WriteMessages(ctx,
		kafka.Message{
			Topic: topic,
			Key:   key,
			Value: value,
		})
}

func (p *producer) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return io.EOF
}
