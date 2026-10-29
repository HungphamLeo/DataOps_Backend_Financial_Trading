// Package producer — DLQ (Dead Letter Queue) Producer standalone dùng kafka-go.
//
// ═══════════════════════════════════════════════════════════════════════════════
// ST03 — DEAD LETTER QUEUE & POISON PILL ISOLATION (PLAN_REPO03)
// ═══════════════════════════════════════════════════════════════════════════════
//
// VẤN ĐỀ — Poison Pill & Head-of-Line Blocking:
//   Kafka xử lý messages theo thứ tự tuần tự trong mỗi partition.
//   Nếu message số N bị hỏng (JSON thiếu dấu đóng, giá âm, timestamp 1970...):
//     1. Consumer parse → throw exception → crash.
//     2. Consumer restart → đọc lại offset N → crash lại.
//     3. Vòng lặp crash vô tận — Head-of-Line Blocking.
//     4. Tất cả messages sau N (dù valid) bị block mãi mãi.
//
//   Ví dụ thực tế: Binance gửi "price=-1" do firmware bug ở exchange.
//   Không có DLQ: 10,000 trades/giây → lag tăng → VWAP sai → trading signal lệch.
//
// GIẢI PHÁP — DLQ Circuit Breaker:
//   try { process(msg) }
//   catch(Exception) { dlq.send(msg + error_metadata); commitOffset(msg) }
//   → Consumer KHÔNG crash. Offset N được commit. Consumer tiếp tục xử lý N+1.
//   → Message độc hại bị cô lập sang DLQ topic để operator điều tra sau.
//
// ST09 — CHAOS ENGINEERING CONNECTION:
//   Circuit Breaker (gobreaker) bổ sung thêm một lớp bảo vệ:
//   Nếu DLQ write cũng liên tục thất bại (Kafka cluster sập) →
//   Circuit Breaker "mở mạch" → ngừng gửi request → consumer tạm dùng in-memory buffer.
//   Sau timeout → "nửa mở" → thử lại → nếu thành công → đóng mạch.
//   Bounded Ring Buffer: chứa tối đa 10,000 messages trong khi circuit open.
//   Tránh OOM khi cluster sập lâu (xem ST09 TODO trong PLAN_REPO03).
//
// DLQ TOPIC DESIGN:
//   partitions=1: xử lý tuần tự, dễ debug và audit (không cần parallel throughput).
//   retention=7 ngày: đủ thời gian on-call team điều tra root cause.
//   Tên convention: {original_topic}.dlq (e.g. binance.trade.ticks.dlq).
//
// QUYẾT ĐỊNH THIẾT KẾ — Tại sao kafka-go thay vì IBM/sarama ở đây?
//   DLQ producer không cần Idempotent mode hay custom partitioner.
//   kafka-go nhẹ hơn cho use case write-only, không cần sarama's full feature set.
package producer

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// DLQProducer gửi các message thất bại vào Dead Letter Queue topic.
//
// Đây là safety net cuối cùng của pipeline — nếu main producer fail.
// RequireAll ACK đảm bảo DLQ message không bị mất dù broker failover.
// Async=false: blocking call → caller biết ngay kết quả ghi DLQ thành công chưa.
type DLQProducer struct {
	writer *kafka.Writer
	logger *zap.Logger
}

// NewDLQProducer tạo producer ghi vào dlqTopic trên brokers cho trước.
//
// RequireAll ACK: DLQ là "ổ đĩa cứu hộ" cuối cùng — nếu DLQ cũng mất data thì
// không còn cơ hội điều tra nguyên nhân. WaitForAll đảm bảo 2/3 replicas đã có.
// Async=false: blocking write → caller biết chính xác DLQ write thành công hay không.
// Nếu DLQ write cũng fail → log CRITICAL → trigger alert → human intervention.
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
// Headers được gắn vào message — operator đọc DLQ KHÔNG cần decode payload:
//   original-topic: topic gốc (e.g. "binance.trade.ticks") — biết data thuộc pipeline nào.
//   failure-reason: error text từ nguyên nhân gốc — "json: cannot unmarshal..." v.v.
//   failed-at:      RFC3339 UTC timestamp — biết khi nào sự cố xảy ra.
//
// Luồng debug sau DLQ incident:
//   1. kafka-console-consumer --topic binance.trade.ticks.dlq --from-beginning
//   2. Đọc header failure-reason → xác định loại lỗi (schema? network? logic?)
//   3. Nếu là transient error → replay message về original topic.
//   4. Nếu là Poison Pill thật → xóa và điều tra upstream (Binance API bug?).
//
// KHÔNG wrap originalErr khi DLQ write cũng fail:
//   Caller đang ở trong catch block — nó cần biết error gốc để log đúng metric.
//   Wrap thêm DLQ error sẽ làm mất context của error gốc trong stack trace.
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
