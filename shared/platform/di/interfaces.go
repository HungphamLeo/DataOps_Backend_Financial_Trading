package di

import (
	"context"

	"go.uber.org/zap"
)

// Logger Interface
type Logger interface {
	Info(msg string, fields ...zap.Field)
	Warn(msg string, fields ...zap.Field)
	Error(msg string, fields ...zap.Field)
	Debug(msg string, fields ...zap.Field)
	Named(name string) *zap.Logger
}

// MessageProducer Interface
type MessageProducer interface {
	SendMessage(ctx context.Context, topic, key string, value []byte) (int32, int64, error)
	Close() error
}

// CacheService Interface
type CacheService interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value interface{}, expiration int64) error
	Delete(ctx context.Context, key string) error
}

// AnalyticsRepository Interface
type AnalyticsRepository interface {
	QueryOHLCV(ctx context.Context, symbol, interval string, startUnix, endUnix int64) ([]map[string]interface{}, error)
	QueryVWAP(ctx context.Context, symbol, interval string, startUnix, endUnix int64) ([]map[string]interface{}, error)
}
