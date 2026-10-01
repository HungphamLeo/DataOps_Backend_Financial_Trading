// Package di định nghĩa các shared interfaces dùng cho Dependency Injection
// trên toàn bộ platform (okx-analytics-api, dataops-stream-producer).
//
// Mục đích: tạo contract chung để các service có thể swap implementation
// mà không cần sửa caller code (Dependency Inversion Principle — SOLID D).
//
// Cách dùng:
//   - Trong constructor, nhận interface thay vì concrete type
//   - Trong tests, inject mock implementation
//   - Production: inject real implementation qua NewServices() / NewServer()
package di

import (
	"context"

	"go.uber.org/zap"
)

// Logger là interface chung cho structured logging, wrapping zap.Logger.
// Named() cho phép tạo child logger với namespace (e.g. logger.Named("kafka")).
type Logger interface {
	Info(msg string, fields ...zap.Field)
	Warn(msg string, fields ...zap.Field)
	Error(msg string, fields ...zap.Field)
	Debug(msg string, fields ...zap.Field)
	// Named tạo child logger với tên prefix — giúp trace log theo component.
	Named(name string) *zap.Logger
}

// MessageProducer là interface để publish message lên Kafka.
// Implementations: TransactionalProducer (exactly-once), producer (at-least-once).
type MessageProducer interface {
	// SendMessage gửi message lên topic và trả về (partition, offset, error).
	SendMessage(ctx context.Context, topic, key string, value []byte) (int32, int64, error)
	// Close flush buffer và đóng connection đến broker.
	Close() error
}

// CacheService là interface cho distributed cache (Redis).
// expiration tính bằng seconds (int64 để tương thích với Redis TTL).
type CacheService interface {
	// Get lấy value theo key. Trả về ("", nil) nếu key không tồn tại.
	Get(ctx context.Context, key string) (string, error)
	// Set ghi key-value với TTL (seconds). expiration=0 = không expire.
	Set(ctx context.Context, key string, value interface{}, expiration int64) error
	// Delete xóa key khỏi cache.
	Delete(ctx context.Context, key string) error
}

// AnalyticsRepository là interface để query time-series data từ InfluxDB.
// startUnix và endUnix là Unix timestamps tính bằng giây (int64).
type AnalyticsRepository interface {
	// QueryOHLCV trả về OHLCV candles cho symbol và interval trong khoảng thời gian.
	QueryOHLCV(ctx context.Context, symbol, interval string, startUnix, endUnix int64) ([]map[string]interface{}, error)
	// QueryVWAP trả về VWAP data points cho symbol và interval trong khoảng thời gian.
	QueryVWAP(ctx context.Context, symbol, interval string, startUnix, endUnix int64) ([]map[string]interface{}, error)
}
