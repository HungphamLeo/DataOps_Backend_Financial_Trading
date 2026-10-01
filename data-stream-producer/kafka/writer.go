// REUSE 100%: trading-bot-finance-service/kafka/writer.go
// Kafka writer với LeastBytes balancer — phân phối đều message vào partitions
package kafka

import (
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// GetWriter tạo Kafka writer với LeastBytes balancer
// REUSE: trading-bot-finance-service/kafka/writer.go
func GetWriter(kafkaURL string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(kafkaURL),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}
}
