// Package models — ST13 processed-event idempotency ledger.
package models

import "time"

// ProcessedEvent lưu identity đã được xử lý để tại-least-once replay không
// ghi trùng cùng trade hoặc cùng downstream event.
//
// ST13:
//
//	IdempotencyKey là primary key duy nhất và bắt buộc ổn định qua replay,
//	không được sinh từ Kafka offset/batch_id (những giá trị có thể đổi).
//
// Mỗi nguồn event có thể dùng identity riêng, nhưng key phải được sinh với
// namespace nguồn và các immutable upstream fields.
type ProcessedEvent struct {
	IdempotencyKey string    `gorm:"size:64;primaryKey" json:"idempotency_key"`
	Source         string    `gorm:"size:32;not null;index" json:"source"`
	Symbol         string    `gorm:"size:32;not null;index:idx_processed_event_symbol_time" json:"symbol"`
	TradeID        string    `gorm:"size:128;not null;index:idx_processed_event_trade" json:"trade_id"`
	EventTsMs      int64     `gorm:"not null;index:idx_processed_event_symbol_time" json:"event_ts_ms"`
	Status         string    `gorm:"size:16;not null;default:processed" json:"status"`
	ProcessedAt    time.Time `gorm:"autoCreateTime" json:"processed_at"`
}

// TableName fixes the ST13 table name independently of GORM pluralization.
func (ProcessedEvent) TableName() string {
	return "processed_events"
}
