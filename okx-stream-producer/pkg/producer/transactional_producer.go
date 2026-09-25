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

// OKXPartitioner định tuyến chuyên dụng: Partition 0 cho BTC, Partition 1 cho ETH, còn lại hash đều
type OKXPartitioner struct{}

func NewOKXPartitioner(topic string) sarama.Partitioner {
	return &OKXPartitioner{}
}

func (p *OKXPartitioner) Partition(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if numPartitions <= 2 {
		return 0, nil
	}

	keyBytes, err := msg.Key.Encode()
	if err != nil {
		return 0, err
	}
	symbol := string(keyBytes)

	// Dedicated high-volume partitions
	if strings.HasPrefix(symbol, "BTC") {
		return 0, nil
	}
	if strings.HasPrefix(symbol, "ETH") {
		return 1, nil
	}

	// Other pairs routed to partition 2..N-1
	hasher := fnv.New32a()
	_, _ = hasher.Write(keyBytes)
	partition := int32(hasher.Sum32()%(uint32(numPartitions)-2)) + 2
	return partition, nil
}

func (p *OKXPartitioner) RequiresConsistency() bool {
	return true
}

// TransactionalProducer quản lý gửi event lên Kafka với Exactly-Once Semantics (EOS)
type TransactionalProducer struct {
	producer sarama.SyncProducer
	logger   *zap.Logger
}

func NewTransactionalProducer(brokers []string, transactionalID string, logger *zap.Logger) (*TransactionalProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // acks=all
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Net.MaxOpenRequests = 1 // Bắt buộc cho idempotent/transactional
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

// SendMessage gửi tin nhắn với transactional context
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

func (p *TransactionalProducer) Close() error {
	return p.producer.Close()
}
