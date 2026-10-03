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

		clientIP := c.ClientIP()
		key := fmt.Sprintf("ratelimit:%s:%d", clientIP, time.Now().Unix()/(int64(r.window.Seconds())))

		ctx := c.Request.Context()
		count, err := r.redisClient.Incr(ctx, key).Result()
		if err != nil {
			c.Next() // fail open on redis error
			return
		}

		if count == 1 {
			r.redisClient.Expire(ctx, key, r.window)
		}

		if count > r.limit {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":       "Too Many Requests",
				"retry_after": r.window.Seconds(),
			})
			return
		}

		c.Next()
	}
}
