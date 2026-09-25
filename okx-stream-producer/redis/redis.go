// REUSE 100%: trading-bot-api/redis/redis.go
// Redis service interface + implementation — dùng cho session cache và market data cache
package redis

import (
	"context"
	"encoding/json"
	"log"
	"okx-stream-producer/pkg/configuration"
	"time"

	"github.com/go-redis/redis/v8"
)

// Service interface — tách biệt contract khỏi implementation
// Pattern: mọi service chỉ depend vào interface này, không depend vào RedisStore cụ thể
type Service interface {
	Set(key string, value interface{}, ttl time.Duration) error
	Get(key string) string
	Del(key string) error
	Ping() bool
	Close() error
	Publish(channel string, payload []byte) error
}

type RedisStore struct {
	Client *redis.Client
}

// NewServer tạo Redis client — REUSE: trading-bot-api/redis/redis.go
func NewServer(cfg *configuration.Config, db int) Service {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Address,
		Password: cfg.Redis.Password,
		DB:       db,
	})
	return &RedisStore{Client: client}
}

func (r *RedisStore) Close() error { return r.Client.Close() }

func (r *RedisStore) Ping() bool {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := r.Client.Ping(ctx).Result()
	if err != nil {
		log.Printf("[Redis] Ping error: %s", err)
		return false
	}
	return true
}

func (r *RedisStore) Set(key string, value interface{}, ttl time.Duration) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// String thì lưu thẳng, không marshal JSON thêm
	if strVal, ok := value.(string); ok {
		return r.Client.Set(ctx, key, strVal, ttl).Err()
	}
	b, _ := json.Marshal(value)
	return r.Client.Set(ctx, key, b, ttl).Err()
}

func (r *RedisStore) Get(key string) string {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	val, err := r.Client.Get(ctx, key).Result()
	if err == redis.Nil {
		return ""
	}
	return val
}

func (r *RedisStore) Del(key string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return r.Client.Del(ctx, key).Err()
}

func (r *RedisStore) Publish(channel string, payload []byte) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return r.Client.Publish(ctx, channel, payload).Err()
}
