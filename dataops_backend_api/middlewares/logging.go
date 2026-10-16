// Package middlewares — HTTP request/response structured logging middleware.
//
// Log mỗi request với: method, path, status, latency, trace_id.
// Tích hợp với logging.Logger để output JSON → Loki có thể scrape.
package middlewares

import (
	"time"

	"dataops-backend-api/logging"

	"github.com/gin-gonic/gin"
)

// RequestLogger middleware ghi structured log cho mỗi HTTP request.
// Phải đăng ký SAU TraceID() để trace_id đã có trong context.
//
//	router.Use(middlewares.TraceID())
//	router.Use(middlewares.RequestLogger(logger))
func RequestLogger(logger *logging.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()
		traceID := c.GetString(TraceIDKey)

		fields := map[string]interface{}{
			"method":     c.Request.Method,
			"path":       c.Request.URL.Path,
			"status":     status,
			"latency_ms": latency.Milliseconds(),
			"client_ip":  c.ClientIP(),
		}

		logFn := logger.WithTraceID(traceID)
		if status >= 500 {
			logFn.Error("http.request", c.Request.URL.Path, fields)
		} else if status >= 400 {
			logFn.Warn("http.request", c.Request.URL.Path, fields)
		} else {
			logFn.Info("http.request", c.Request.URL.Path, fields)
		}
	}
}
