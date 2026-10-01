// Package kafka cung cấp low-level writer với key-hash balancer bảo toàn thứ tự partition.
package kafka

import (
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// GetWriter tạo Kafka writer với Hash balancer (bảo toàn ordering theo message key) và acks=all
func GetWriter(kafkaURL string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(kafkaURL),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
}
