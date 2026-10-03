// Package middlewares cung cấp các Gin middleware dùng chung trong dataops-backend-api:
//   - jwt_auth.go   : JWT authentication + Redis blacklist (struct-based, dùng go-redis/v9)
//   - auth_tiers.go : JWT + Redis session, role và status check (function-based)
//   - rate_limiter.go: IP-based fixed-window rate limiting qua Redis
//   - security.go   : CORS headers cho production và development
//
// Hai file jwt_auth.go và auth_tiers.go phục vụ cùng mục đích nhưng theo hai
// interface khác nhau. jwt_auth.go dùng AuthMiddleware struct (dễ inject mock trong test),
// auth_tiers.go dùng closure function (nhẹ hơn, phù hợp route-level middleware).
package middlewares

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v4"
	"github.com/redis/go-redis/v9"
)

// AuthMiddleware giữ dependencies cần thiết để xác thực JWT và kiểm tra blacklist.
// Dùng NewAuthMiddleware để khởi tạo — không tạo trực tiếp struct.
type AuthMiddleware struct {
	secretKey   string
	redisClient *redis.Client // nil-safe: nếu nil, bỏ qua blacklist check
}

// UserClaims là JWT payload claims cho dataops-backend-api.
// Dùng jwt/v4 RegisteredClaims (exp, iat, iss...) + custom fields.
type UserClaims struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	Role     string `json:"role"` // "USER" | "ADMIN" | "SUPER_ADMIN"
	jwt.RegisteredClaims
}

// NewAuthMiddleware tạo AuthMiddleware mới với JWT secret và Redis client.
// redisClient có thể là nil — khi đó blacklist check bị bỏ qua (dùng cho dev/test).
func NewAuthMiddleware(secretKey string, redisClient *redis.Client) *AuthMiddleware {
	return &AuthMiddleware{
		secretKey:   secretKey,
		redisClient: redisClient,
	}
}

// RequireAuth kiểm tra tính hợp lệ của JWT Access Token
func (m *AuthMiddleware) RequireAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Authorization header required"})
			return
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid Authorization header format"})
			return
		}

		tokenString := parts[1]
		claims := &UserClaims{}

		token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}
			return []byte(m.secretKey), nil
		})

		if err != nil || !token.Valid {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid or expired token"})
			return
		}

		// Kiểm tra revocation / blacklist trên Redis
		if m.redisClient != nil {
			blacklistKey := fmt.Sprintf("blacklist:token:%s", tokenString)
			if val, _ := m.redisClient.Get(c.Request.Context(), blacklistKey).Result(); val != "" {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Token has been revoked"})
				return
			}
		}

		c.Set("user_id", claims.UserID)
		c.Set("username", claims.Username)
		c.Set("role", claims.Role)
		c.Next()
	}
}

// RequireRole kiểm tra quyền truy cập theo vai trò (RBAC)
func (m *AuthMiddleware) RequireRole(allowedRoles ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
		roleVal, exists := c.Get("role")
		if !exists {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "Role context not found"})
			return
		}

		userRole := roleVal.(string)
		for _, role := range allowedRoles {
			if userRole == role || userRole == "SUPER_ADMIN" {
				c.Next()
				return
			}
		}

		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "Forbidden: Insufficient privileges"})
	}
}
