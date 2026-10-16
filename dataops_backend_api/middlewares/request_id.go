// Package middlewares — TraceID middleware.
//
// Mỗi request HTTP vào đều cần 1 trace_id để trace end-to-end qua tất cả logs:
//   stream_producer → kafka → processing → backend_api
//
// Logic:
//   1. Đọc header X-Trace-ID từ request (client hoặc upstream gateway có thể inject).
//   2. Nếu không có → tự sinh UUID v4.
//   3. Set vào gin.Context key "trace_id" → handler dùng c.GetString("trace_id").
//   4. Ghi lại vào response header X-Trace-ID → client có thể correlate.
package middlewares

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const TraceIDKey = "trace_id"
const TraceIDHeader = "X-Trace-ID"

// TraceID middleware extract hoặc tạo mới trace_id cho mỗi request.
// Phải đăng ký trước JWT middleware để tất cả handlers đều có trace_id.
func TraceID() gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := c.GetHeader(TraceIDHeader)
		if traceID == "" {
			traceID = uuid.New().String()
		}
		c.Set(TraceIDKey, traceID)
		c.Header(TraceIDHeader, traceID)
		c.Next()
	}
}
