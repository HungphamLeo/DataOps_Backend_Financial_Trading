// REUSE: ~100% từ trading-bot-api/middlewares/auth.go
// LEARN: Middleware trong Gin — function chạy TRƯỚC handler chính
//
// Middleware chain: Request → [CORS] → [Auth] → [RateLimit] → Handler → Response
//
// Gin middleware pattern:
//   func MyMiddleware() gin.HandlerFunc {
//       return func(c *gin.Context) {
//           // Code TRƯỚC handler
//           c.Next()
//           // Code SAU handler (response đã được ghi)
//       }
//   }
//
// c.Set/c.Get: lưu/lấy data trong request context (goroutine-safe)
// c.Abort(): dừng chuỗi middleware, không gọi tiếp

package middlewares

import (
	"encoding/json"
	"log"
	"net/http"
	"okx-analytics-api/constants"
	"okx-analytics-api/models"
	redisService "okx-analytics-api/redis"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// Các key để lưu user data trong Gin context
// LEARN: Hằng số cho context keys — tránh typo khi dùng c.Set/c.Get
const (
	CtxKeyUser  = "user"
	CtxKeyToken = "token"
)

// JwtAuthMiddleware là middleware full JWT authentication
// REUSE: 95% từ trading-bot-api/middlewares/auth.go JwtAuthMiddleware
// LEARN: Closure — function trả về function (higher-order function)
//        Nhận cfg + redis lúc khởi tạo, trả về gin.HandlerFunc để Gin gọi mỗi request
func JwtAuthMiddleware(jwtSecret string, redis redisService.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Bước 1: Lấy token từ Authorization header
		// LEARN: "Bearer <token>" là standard HTTP Bearer Token format (RFC 6750)
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "authorization header required"})
			c.Abort()
			return
		}

		// Bước 2: Parse Bearer token
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid authorization format"})
			c.Abort()
			return
		}
		tokenStr := parts[1]

		// Bước 3: Parse và validate JWT signature
		// LEARN: jwt.ParseWithClaims verify:
		//        1. Signature (HMAC-SHA256 với secret)
		//        2. Expiry (exp claim)
		//        3. Không thể tamper claims mà không biết secret
		claims := &JWTClaims{}
		token, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
			// Verify signing method — tránh "none" algorithm attack
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, jwt.ErrSignatureInvalid
			}
			return []byte(jwtSecret), nil
		})

		if err != nil || !token.Valid {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			c.Abort()
			return
		}

		// Bước 4: Kiểm tra Redis session — user có bị force logout không?
		// LEARN: Stateful JWT — thêm Redis check để có thể revoke token ngay lập tức
		//        Stateless JWT: token valid đến khi hết hạn, không thể revoke trước đó
		sessionKey := "session:" + claims.UserID
		sessionData := redis.Get(sessionKey)
		if sessionData == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "session expired"})
			c.Abort()
			return
		}

		// Bước 5: Deserialize user từ Redis
		user := &models.UserSession{}
		if err := json.Unmarshal([]byte(sessionData), user); err != nil {
			log.Printf("[Auth] Failed to deserialize session for user %s: %v", claims.UserID, err)
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid session"})
			c.Abort()
			return
		}

		// Bước 6: Kiểm tra status user
		if user.Status == constants.USER_STATUS_BLOCKED {
			c.JSON(http.StatusForbidden, gin.H{"error": "account blocked"})
			c.Abort()
			return
		}
		if user.Status == constants.USER_STATUS_INACTIVE {
			c.JSON(http.StatusForbidden, gin.H{"error": "account inactive"})
			c.Abort()
			return
		}

		// Bước 7: Lưu user vào context cho handlers downstream
		// LEARN: c.Set — lưu arbitrary data vào request context
		//        c.MustGet/c.Get — lấy lại trong handler
		c.Set(CtxKeyUser, user)
		c.Set(CtxKeyToken, tokenStr)
		c.Next() // tiếp tục chuỗi middleware / handler
	}
}

// AdminMiddleware chỉ cho phép admin users
// REUSE: 100% từ trading-bot-api/middlewares/auth.go Admin()
// LEARN: Middleware composition — AdminMiddleware phải đặt SAU JwtAuthMiddleware
//        vì nó cần user đã được set bởi JwtAuthMiddleware
func AdminMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		user := GetUserFromContext(c)
		if user == nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
			c.Abort()
			return
		}
		if user.RoleID != constants.ROLE_ADMIN {
			c.JSON(http.StatusForbidden, gin.H{"error": "admin access required"})
			c.Abort()
			return
		}
		c.Next()
	}
}

// TrialMiddleware giới hạn chức năng cho trial users
// REUSE: Từ trading-bot-api/middlewares/auth.go Trial()
// LEARN: Feature flag middleware — có thể mở rộng để kiểm tra subscription tier
func TrialMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		user := GetUserFromContext(c)
		if user == nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
			c.Abort()
			return
		}
		// Trial users có thể truy cập, nhưng mark trong context để handler giới hạn data
		// VD: trial user chỉ xem 24h data thay vì toàn bộ history
		c.Set("is_trial", user.Status == constants.USER_STATUS_TRIAL)
		c.Next()
	}
}

// RateLimitMiddleware giới hạn số requests per minute per user
// LEARN: Simple in-memory rate limiter dùng Redis — không scale across multiple instances
//        Production: dùng redis sliding window algorithm
func RateLimitMiddleware(redis redisService.Service, maxReqPerMin int) gin.HandlerFunc {
	return func(c *gin.Context) {
		user := GetUserFromContext(c)
		if user == nil {
			c.Next()
			return
		}

		// Redis key: "ratelimit:{userID}:{minute}"
		// LEARN: Minute-based key tự động expire sau 60s mà không cần TTL management
		minute := time.Now().Format("200601021504") // YYYYMMDDHHmm
		key := "ratelimit:" + user.UserID + ":" + minute

		// Increment counter, set TTL 2 phút để cleanup
		current := redis.Get(key)
		count := 0
		if current != "" {
			json.Unmarshal([]byte(current), &count)
		}
		count++

		if count > maxReqPerMin {
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":       "rate limit exceeded",
				"retry_after": "60",
			})
			c.Abort()
			return
		}

		redis.Set(key, count, 2*time.Minute)
		c.Next()
	}
}

// GetUserFromContext lấy user từ Gin context (set bởi JwtAuthMiddleware)
// REUSE: Từ trading-bot-api/middlewares/auth.go GetUser()
// LEARN: Gin context.Get trả về (interface{}, bool) — luôn check ok trước
func GetUserFromContext(c *gin.Context) *models.UserSession {
	val, exists := c.Get(CtxKeyUser)
	if !exists {
		return nil
	}
	user, ok := val.(*models.UserSession)
	if !ok {
		return nil
	}
	return user
}

// JWTClaims là struct chứa claims trong JWT token
// LEARN: jwt.RegisteredClaims embed — thêm standard claims (exp, iat, iss...)
//        Custom claims: UserID, RoleID — thêm vào token payload
type JWTClaims struct {
	jwt.RegisteredClaims
	UserID string `json:"userId"`
	RoleID uint   `json:"roleId"`
	Email  string `json:"email"`
}

// GenerateToken tạo JWT token mới (dùng trong authentication service)
// LEARN: jwt.NewWithClaims + token.SignedString = tạo và sign token
func GenerateToken(jwtSecret string, userID string, roleID uint, email string, ttl time.Duration) (string, error) {
	claims := JWTClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(ttl)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Issuer:    "okx-analytics-api",
		},
		UserID: userID,
		RoleID: roleID,
		Email:  email,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(jwtSecret))
}
