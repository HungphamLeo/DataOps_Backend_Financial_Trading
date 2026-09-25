package middlewares

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

// RateLimiterMiddleware giới hạn tốc độ truy cập theo client IP / User ID dùng Redis sliding log / fixed window
type RateLimiterMiddleware struct {
	redisClient *redis.Client
	limit       int64
	window      time.Duration
}

func NewRateLimiterMiddleware(redisClient *redis.Client, limit int64, window time.Duration) *RateLimiterMiddleware {
	return &RateLimiterMiddleware{
		redisClient: redisClient,
		limit:       limit,
		window:      window,
	}
}

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
