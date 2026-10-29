// Package redis — Redis client wrapper cho dataops-backend-api.
//
// ═══════════════════════════════════════════════════════════════════════════════
// ST06 — CACHE-ASIDE PATTERN & PACELC (PLAN_REPO03)
// ═══════════════════════════════════════════════════════════════════════════════
//
// Redis trong kiến trúc này đóng 3 vai trò:
//
//   [1] Cache-Aside Layer trước InfluxDB (ST06):
//       PACELC - EL (Else + Latency): khi mạng bình thường, ưu tiên Latency thấp.
//       Redis cache VWAP query results với TTL 30s → API latency < 1ms vs 20ms InfluxDB.
//       Stale-While-Revalidate: client nhận data cũ ngay, background refresh cache mới.
//       Key pattern: "vwap:{symbol}:{interval}:{ts_bucket}" (deterministic → high hit ratio).
//
//   [2] Real-time Ticker Snapshot (ST15):
//       binance_{symbol}_ticker TTL 10s — snapshot mới nhất, không tích lũy state.
//       tcbs_{symbol}_ticker    TTL 120s — TCBS poll mỗi 1 phút giờ giao dịch.
//       KHÔNG dùng Redis để aggregate (buyVol, sellVol) — đó là việc của Spark (ST12).
//
//   [3] Session & Token Blacklist (ST07):
//       JWT refresh token lưu với key "refresh:{user_id}" TTL 7 ngày.
//       Token revocation blacklist "blacklist:token:{token}" — kiểm tra trước mỗi request.
//       Rate limit counter "ratelimit:{ip}:{epoch_window}" — fixed window per IP.
//
// EVICTION POLICY PRODUCTION:
//   Cấu hình Redis: maxmemory-policy allkeys-lru
//   Khi RAM đầy → tự động xóa key ít dùng nhất (LRU) — không crash.
//   Hệ quả: cache miss tăng tạm thời, InfluxDB chịu thêm load — chấp nhận được.
//   Không dùng noeviction trong production (Redis crash khi full → toàn bộ API down).
//
// CONNECTION POOL:
//   go-redis tự quản lý connection pool. Mặc định: PoolSize = 10 connections.
//   Nếu API có 100 goroutines đồng thời → pool đủ dùng (goroutine chờ connection available).
//   Monitor: redis_connected_clients > PoolSize * 0.8 → tăng PoolSize hoặc scale Redis.
//
// Interface pattern — tại sao dùng interface:
//   Dễ mock khi test: tạo MockRedis implement Service (không cần Redis thật).
//   Dễ swap implementation: có thể thay Redis bằng Valkey/DragonflyDB mà không sửa caller.
//   Dependency Inversion Principle (SOLID-D): code phụ thuộc vào abstraction.

package redis

import (
	"context"
	"encoding/json"
	"log"
	"dataops-backend-api/configuration"
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
