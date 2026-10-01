// REUSE 100%: market-maker-api/middlewares/security.go
// CORS và security headers — quan trọng cho production
package middlewares

import (
	"dataops-backend-api/configuration"

	"github.com/gin-gonic/gin"
)

// Secure áp dụng CORS headers cho production.
// Origin được giới hạn theo cfg.Web.URL — chỉ frontend đã cấu hình mới được phép.
// Preflight OPTIONS request trả về 204 No Content ngay lập tức để giảm latency.
func Secure(cfg *configuration.Config) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		ctx.Writer.Header().Set("Access-Control-Allow-Origin", cfg.Web.URL)
		ctx.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		ctx.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		ctx.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")

		if ctx.Request.Method == "OPTIONS" {
			ctx.AbortWithStatus(204)
			return
		}
		ctx.Next()
	}
}

// SecureNoRestrict cho phép tất cả origins (Access-Control-Allow-Origin: *).
// Chỉ dùng cho môi trường development/local — KHÔNG dùng trong production
// vì sẽ mở API cho mọi domain, kể cả các trang độc hại.
func SecureNoRestrict(_ *configuration.Config) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		ctx.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		ctx.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		ctx.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")

		if ctx.Request.Method == "OPTIONS" {
			ctx.AbortWithStatus(204)
			return
		}
		ctx.Next()
	}
}
