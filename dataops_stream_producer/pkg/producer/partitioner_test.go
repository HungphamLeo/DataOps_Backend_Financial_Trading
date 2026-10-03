package producer

import (
	"testing"

	"github.com/IBM/sarama"
)

func TestBinancePartitioner(t *testing.T) {
	partitioner := NewBinancePartitioner("binance.trade.ticks")

	tests := []struct {
		name          string
		key           string
		numPartitions int32
		wantPartition int32
	}{
		{
			name:          "BTCUSDT to partition 0",
			key:           "BTCUSDT",
			numPartitions: 5,
			wantPartition: 0,
		},
		{
			name:          "ETHUSDT to partition 1",
			key:           "ETHUSDT",
			numPartitions: 5,
			wantPartition: 1,
		},
		{
			name:          "Small partition count fallback",
			key:           "BTCUSDT",
			numPartitions: 2,
			wantPartition: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &sarama.ProducerMessage{
				Topic: "binance.trade.ticks",
				Key:   sarama.StringEncoder(tt.key),
			}
			p, err := partitioner.Partition(msg, tt.numPartitions)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if p != tt.wantPartition {
				t.Errorf("got partition %d, want %d", p, tt.wantPartition)
			}
		})
	}
}
