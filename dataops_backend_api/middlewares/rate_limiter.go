// Package middlewares — Gin HTTP middlewares cho dataops-backend-api.
//
// ═══════════════════════════════════════════════════════════════════════════════
// ST07 — ZERO-TRUST AUTH, TOKEN ROTATION & RATE LIMITING (PLAN_REPO03)
// ═══════════════════════════════════════════════════════════════════════════════
//
// Zero-Trust Architecture: KHÔNG giả định request nào là an toàn dù từ internal network.
//   Mọi request phải được: (1) authenticate, (2) authorize, (3) rate-limited.
//   "Never trust, always verify" — ngay cả service-to-service calls.
//
// Rate Limiting Strategy — Tại sao IP-based là không đủ:
//   Vấn đề: Văn phòng lớn dùng NAT Gateway → 1000 nhân viên có chung 1 IP.
//   IP-based limit → chặn nhầm toàn văn phòng khi 1 người abuse.
//   Giải pháp tốt hơn: API Key/User Token-based rate limit (ST07 TODO).
//
// Thuật toán Fixed Window Counter:
//   Mỗi "window" = khoảng thời gian cố định (e.g. 1 phút).
//   window_id = Unix_epoch_seconds / window_size_seconds.
//   Redis key: "ratelimit:{clientIP}:{window_id}" → INCR và Expire.
//   Trade-off vs Sliding Window:
//     Fixed window: O(1) memory, nhưng burst gấp đôi tại boundary
//     (N requests cuối window + N requests đầu window tiếp theo).
//     Sliding window log: chính xác hơn nhưng O(requests) memory.
//     Token Bucket: smooth limiting, memory-efficient — ideal cho production (TODO).
//
// Fail Open (vs Fail Closed):
//   Khi Redis unavailable → middleware cho phép request qua (fail open).
//   Lý do: Redis tạm thời chết không nên kéo theo toàn bộ API down.
//   Đánh đổi: trong window Redis outage, rate limit bị bypass.
//   Production: alert khi Redis down + có fallback in-memory counter.
//
// HTTP Headers chuẩn (TODO ST07):
//   X-RateLimit-Limit:     100      — max requests/window
//   X-RateLimit-Remaining: 47       — còn lại
//   X-RateLimit-Reset:     1700000  — epoch khi window reset
//   Giúp client biết khi nào nên retry thay vì hammer server.
package middlewares

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

// RateLimiterMiddleware giới hạn tốc độ truy cập theo client IP dùng Redis fixed-window counter.
//
// Thuật toán: fixed window — mỗi window (tính theo Unix epoch / window size) có một counter
// riêng trong Redis. Counter tăng mỗi request; nếu vượt limit → 429 Too Many Requests.
// Key format: "ratelimit:{clientIP}:{epochWindow}" — tự expire sau window duration.
//
// Trade-off so với sliding window:
//   - Fixed window: đơn giản hơn, có thể bị burst gấp đôi tại boundary.
//   - Sliding window log: chính xác hơn nhưng tốn memory O(requests).
//
// Nếu Redis không available, fail open (cho phép request qua) để tránh downtime cascade.
type RateLimiterMiddleware struct {
	redisClient *redis.Client
	limit       int64         // số request tối đa trong một window
	window      time.Duration // độ dài của mỗi window
}

// NewRateLimiterMiddleware tạo RateLimiterMiddleware với Redis client, ngưỡng limit và độ dài window.
// redisClient không được nil trong production — nếu nil, Limit() sẽ fail open toàn bộ.
func NewRateLimiterMiddleware(redisClient *redis.Client, limit int64, window time.Duration) *RateLimiterMiddleware {
	return &RateLimiterMiddleware{
		redisClient: redisClient,
		limit:       limit,
		window:      window,
	}
}

// Limit trả về gin.HandlerFunc kiểm tra rate limit theo client IP.
// Fail open nếu redisClient là nil hoặc gặp lỗi Redis để tránh block toàn bộ traffic.
func (r *RateLimiterMiddleware) Limit() gin.HandlerFunc {
	return func(c *gin.Context) {
		if r.redisClient == nil {
			c.Next()
			return
		}

		// ST07: prefer API key / user token over IP
        apiKey := c.GetHeader("X-API-Key")
        if apiKey == "" {
            apiKey = c.GetHeader("Authorization")
            if strings.HasPrefix(apiKey, "Bearer ") {
                apiKey = strings.TrimPrefix(apiKey, "Bearer ")
            }
        }
        if apiKey == "" {
            c.Next()
            return
        }

        windowID := time.Now().Unix() / int64(r.window.Seconds())
        key := fmt.Sprintf("ratelimit:%s:%d", apiKey, windowID)

        ctx := c.Request.Context()
        count, err := r.redisClient.Incr(ctx, key).Result()
        if err != nil {
            c.Next()
            return
        }
        if count == 1 {
            _ = r.redisClient.Expire(ctx, key, r.window).Err()
        }

        limit := int64(100)
        remaining := limit - count
        if remaining < 0 {
            remaining = 0
        }

        reset := (time.Now().Unix() / int64(r.window.Seconds()) + 1) * int64(r.window.Seconds())

        c.Header("X-RateLimit-Limit", strconv.FormatInt(limit, 10))
        c.Header("X-RateLimit-Remaining", strconv.FormatInt(remaining, 10))
        c.Header("X-RateLimit-Reset", strconv.FormatInt(reset, 10))

        if count > limit {
            c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
                "error":       "rate limit exceeded",
                "retry_after": r.window.Seconds(),
            })
            return
        }

        c.Next()
    }
}
