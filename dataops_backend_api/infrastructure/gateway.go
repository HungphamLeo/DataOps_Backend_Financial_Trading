// LEARN: API Gateway — single entry point cho tất cả microservice routes
//
// API Gateway pattern (từ "Building Microservices" O'Reilly Ch.13):
//   Client → [API Gateway] → [Auth] → [Rate Limit] → [Service A/B/C]
//
// Trách nhiệm của Gateway:
//   1. Route aggregation: /api/v1/* → đúng service
//   2. Authentication: verify JWT trước khi forward
//   3. Rate limiting: protect downstream services
//   4. Request/Response transformation: normalize formats
//   5. Health check exposure: /health endpoint
//   6. Observability: log latency, status codes
//
// Trong project này, Gateway là Gin router trong okx-analytics-api
// (không phải separate service vì scale đơn giản)

package infrastructure

import (
	"fmt"
	"net/http"
	"okx-analytics-api/logging"
	"time"

	"github.com/gin-gonic/gin"
)

// GatewayConfig cấu hình cho API Gateway
type GatewayConfig struct {
	Port             int // HTTP listen port
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	ShutdownTimeout  time.Duration
	TrustedProxies   []string // Nginx, load balancer IPs
	EnableCORS       bool
	EnableRequestLog bool
}

// DefaultGatewayConfig trả về config mặc định phù hợp production
func DefaultGatewayConfig() GatewayConfig {
	return GatewayConfig{
		Port:             8080,
		ReadTimeout:      30 * time.Second,
		WriteTimeout:     30 * time.Second,
		ShutdownTimeout:  10 * time.Second,
		TrustedProxies:   []string{"127.0.0.1"},
		EnableCORS:       true,
		EnableRequestLog: true,
	}
}

// RequestLogger middleware ghi lại mọi HTTP request (method, path, latency, status).
// Dùng logging.Logger (JSON structured) thay vì log.Printf để đồng nhất với toàn service.
//
// LEARN: Gin middleware với time tracking — đo performance của từng endpoint
func RequestLogger(logger *logging.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		// Next() — thực thi handler
		c.Next()

		// Sau handler: ghi log với kết quả
		latency := time.Since(start)
		status := c.Writer.Status()

		logger.Info("http.request", "incoming request", map[string]interface{}{
			"method":  c.Request.Method,
			"path":    path,
			"status":  status,
			"latency": latency.String(),
		})
	}
}

// RecoveryMiddleware recover từ panic và trả về 500 thay vì crash.
// Dùng logging.Logger để ghi panic context với structured fields.
//
// LEARN: recover() — built-in Go, chỉ hoạt động trong deferred function
//
//	Không để panic propagate → service crash và down time
func RecoveryMiddleware(logger *logging.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				logger.Error("gateway.panic", "panic recovered", map[string]interface{}{
					"error": err,
					"path":  c.Request.URL.Path,
				})
				c.JSON(http.StatusInternalServerError, gin.H{
					"error": "internal server error",
				})
				c.Abort()
			}
		}()
		c.Next()
	}
}

// HealthCheckHandler trả về service health status.
//
// LEARN: Health check endpoint là MUST HAVE cho:
//  1. Kubernetes liveness/readiness probes
//  2. Load balancer health check
//  3. Monitoring dashboards
func HealthCheckHandler(version string) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":    "ok",
			"version":   version,
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	}
}

// SetupHTTPServer tạo *http.Server với proper timeouts.
//
// LEARN: Luôn set timeout cho HTTP server trong production:
//
//	ReadTimeout: tối đa bao lâu để đọc toàn bộ request
//	WriteTimeout: tối đa bao lâu để ghi response
//	Không set → slowloris attack có thể làm exhausted goroutines
func SetupHTTPServer(router *gin.Engine, cfg GatewayConfig) *http.Server {
	return &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      router,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}
}
