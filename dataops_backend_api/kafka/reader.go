// REUSE: ~90% adapted from trading-bot-api/kafka/reader.go
// LEARN: Kafka Consumer trong Go với segmentio/kafka-go
//
// Consumer Group: nhiều instances của cùng 1 service có thể chia sẻ partitions
// VD: 3 instances analytics-api → mỗi instance đọc từ subset partitions khác nhau

package kafka

import (
	kafka "github.com/segmentio/kafka-go"
)

// GetReader tạo Kafka consumer reader
// LEARN: kafka.ReaderConfig các fields quan trọng:
//   - Brokers: list các broker address (dùng nhiều để fault-tolerant)
//   - GroupID: consumer group ID — Kafka track offset cho mỗi group riêng biệt
//   - GroupTopics: đọc từ nhiều topics (mỗi topic là 1 stream data riêng)
//   - MaxBytes: giới hạn size mỗi fetch request (10MB = safe default)
func GetReader(kafkaBrokers []string, groupID string, topics []string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     kafkaBrokers,
		GroupID:     groupID,
		GroupTopics: topics,
		MaxBytes:    10e6, // 10MB
	})
}

// GetSingleTopicReader tạo reader cho 1 topic cụ thể (không dùng consumer group)
// LEARN: Không có GroupID = không track offset = dọc từ đầu mỗi khi restart
// Dùng cho: manual inspection, backfill, one-time processing
func GetSingleTopicReader(kafkaBrokers []string, topic string, partition int) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:   kafkaBrokers,
		Topic:     topic,
		Partition: partition,
		MaxBytes:  10e6,
	})
}
