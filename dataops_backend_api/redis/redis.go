// REUSE: ~95% copied from trading-bot-api/redis/redis.go
// LEARN: Interface + struct pattern — dependency injection trong Go
//
// Tại sao dùng Interface cho Redis?
//   - Dễ mock khi test: tạo MockRedis implement Service
//   - Dễ swap implementation: có thể thay Redis bằng Memcached mà không sửa code khác
//   - Dependency Inversion Principle (SOLID): code phụ thuộc vào abstraction, không phải concrete type

package redis

import (
	"context"
	"encoding/json"
	"log"
	"okx-analytics-api/configuration"
	"time"

	"github.com/go-redis/redis/v8"
)

// Service định nghĩa contract — mọi caller dùng interface này, không phải *RedisStore trực tiếp
// LEARN: Đây là Repository Pattern: ẩn chi tiết implementation đằng sau interface
type Service interface {
	Set(key string, value interface{}, time time.Duration) error
	Get(key string) string
	Scan(key string) []string
	Del(key string) error
	Dels(key []string) error
	Ping() bool
	Close() error

	// Pub/Sub: dùng cho real-time alert broadcasting
	// LEARN: Redis Pub/Sub = lightweight message broker, không cần Kafka cho low-volume alerts
	Publish(channel string, payload []byte) error
	Subscribe(channel string) *redis.PubSub
}

// RedisStore là concrete implementation của Service
// LEARN: Struct chứa *redis.Client (pointer) vì Client có internal connection pool
type RedisStore struct {
	Client *redis.Client
}

// NewServer khởi tạo Redis connection
// LEARN: Nhận *configuration.Config thay vì raw string — Dependency Injection
func NewServer(cfg *configuration.Config, db int) Service {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Address,
		Password: cfg.Redis.Password,
		DB:       db, // Redis có 16 databases (0-15), mỗi service dùng 1 DB riêng
	})

	return &RedisStore{
		Client: client,
	}
}

func (r *RedisStore) Close() error {
	return r.Client.Close()
}

// Ping kiểm tra kết nối Redis — dùng trong health check endpoint
func (r *RedisStore) Ping() bool {
	// LEARN: context.WithCancel — có thể cancel operation bất kỳ lúc nào
	// defer cancel() đảm bảo context được cleanup, tránh goroutine leak
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := r.Client.Ping(ctx).Result()
	if err != nil {
		log.Printf("[Redis] Ping error: %s", err)
		return false
	}
	return true
}

// Set lưu value vào Redis với TTL (time-to-live)
// LEARN: type assertion (value.(string)) — kiểm tra kiểu của interface{} lúc runtime
func (r *RedisStore) Set(key string, value interface{}, ttl time.Duration) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// String trực tiếp — không encode JSON (tránh double-quote trong Redis)
	if strVal, ok := value.(string); ok {
		return r.Client.Set(ctx, key, strVal, ttl).Err()
	}

	// Các type khác (struct, map...) → serialize thành JSON bytes
	jsonBytes, _ := json.Marshal(value)
	return r.Client.Set(ctx, key, jsonBytes, ttl).Err()
}

func (r *RedisStore) Get(key string) string {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	val, err := r.Client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "" // key không tồn tại = empty string (không phải error)
	}
	return val
}

// Scan tìm tất cả keys khớp với pattern — dùng để tìm keys theo prefix
// LEARN: SCAN tốt hơn KEYS trong production vì không block Redis
// VD: Scan("OKX:BTC") → ["OKX:BTC-USDT:last", "OKX:BTC-USDT:ts"]
func (r *RedisStore) Scan(key string) []string {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pattern := "*" + key + "*"
	val, cursor, err := r.Client.Scan(ctx, 0, pattern, 0).Result()
	if err == redis.Nil {
		return nil
	}

	// cursor > 0 = còn nhiều trang kết quả (Redis trả về từng batch)
	for cursor > 0 {
		val2, cursor2, err := r.Client.Scan(ctx, cursor, pattern, 0).Result()
		if err != nil {
			break
		}
		val = append(val, val2...)
		cursor = cursor2
	}
	return val
}

func (r *RedisStore) Del(key string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return r.Client.Del(ctx, key).Err()
}

// Dels xóa nhiều keys cùng lúc dùng Pipeline — hiệu quả hơn gọi Del() nhiều lần
// LEARN: Redis Pipeline = gộp nhiều commands vào 1 round-trip TCP (giảm latency)
func (r *RedisStore) Dels(keys []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pipe := r.Client.Pipeline()
	for _, key := range keys {
		pipe.Del(ctx, key)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Subscribe đăng ký nhận messages từ Redis channel
// LEARN: Redis Pub/Sub — producer gọi Publish, consumer gọi Subscribe
// Dùng cho: real-time price alerts, bot status updates
func (r *RedisStore) Subscribe(channel string) *redis.PubSub {
	// NOTE: Không dùng WithCancel ở đây — PubSub cần context sống lâu
	return r.Client.Subscribe(context.Background(), channel)
}

// Publish gửi message lên Redis channel
func (r *RedisStore) Publish(channel string, payload []byte) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return r.Client.Publish(ctx, channel, payload).Err()
}
