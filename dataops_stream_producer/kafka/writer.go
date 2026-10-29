// Package kafka — low-level Kafka writer wrapper dùng segmentio/kafka-go.
//
// ═══════════════════════════════════════════════════════════════════════════════
// ST08 — DISTRIBUTED TRACING: Kafka Header Propagation (PLAN_REPO03)
// ═══════════════════════════════════════════════════════════════════════════════
//
// Distributed Trace Context (W3C Standard):
//   Một trade event đi qua nhiều ranh giới service:
//   Binance WebSocket → Go Producer → Kafka → Spark Streaming → InfluxDB → Go API.
//   Không có tracing: khi latency tăng đột biến lúc 3h sáng, không biết component nào gây ra.
//
//   W3C Trace Context: header "traceparent" = "{version}-{trace_id}-{span_id}-{flags}"
//   Ví dụ: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
//     → trace_id: theo dõi toàn bộ chain từ WebSocket đến API response.
//     → span_id: theo dõi một bước cụ thể (e.g. "ghi vào Kafka").
//
//   Kafka Header Propagation (ST08 TODO):
//   Khi Producer gửi message, inject traceparent vào Kafka message headers:
//     msg.Headers = append(msg.Headers, kafka.Header{
//         Key: "traceparent", Value: []byte(span.SpanContext().TraceID().String()),
//     })
//   Spark Streaming job đọc header này và tạo child span khi ghi vào InfluxDB.
//   → Entire chain visible trong Jaeger: từ WebSocket connect đến InfluxDB write.
//
//   TransactionalProducer (transactional_producer.go) đã inject "produced_at" và "source"
//   headers — đây là tiền thân của distributed tracing. ST08 extends thêm "traceparent".
//
// Key-Hash Balancer (ordering guarantee):
//   Kafka Hash{} partitioner: hash(message.Key) % numPartitions → deterministic.
//   Cùng symbol key (e.g. "BTCUSDT") → luôn cùng partition → ordering preserved.
//   Round-robin thay thế: throughput cao hơn nhưng mất ordering → VWAP tính sai.
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
