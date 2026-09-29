// Package logging cung cấp structured JSON logger nội bộ cho okx-analytics-api.
//
// Vai trò:
//   File này là logger INTERNAL của service — không phải duplicate của shared/platform/observability.
//   shared/platform/observability dùng go.uber.org/zap (chưa trong go.mod của service này).
//   Khi go.mod được cập nhật để include zap, các caller chỉ cần thay NewLogger() call —
//   không cần sửa business logic ở bất kỳ chỗ nào khác.
//
// Tại sao cần structured logging?
//   - Plain log.Printf("error: %s") → khó parse, không có context
//   - Structured: {"level":"ERROR","service":"vps-poller","symbol":"VNM","msg":"timeout","ts":1234}
//   - Có thể query: tìm tất cả ERROR trong service X → log aggregation (ELK, Grafana Loki)
//
// REUSE: Extends market-maker-background/utils/logger.go
//
// Log levels:
//   DEBUG: development details (tắt ở production)
//   INFO:  normal operations
//   WARN:  unexpected but recoverable
//   ERROR: requires investigation
//   FATAL: unrecoverable, app exits

package logging

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"time"
)

// Level là log severity level
type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
	FATAL
)

func (l Level) String() string {
	return [...]string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}[l]
}

// LogEntry là 1 log record với structured fields
// LEARN: JSON struct tags → log entries có thể được parse bởi log aggregators
type LogEntry struct {
	Timestamp string                 `json:"ts"`
	Level     string                 `json:"level"`
	Service   string                 `json:"service"`
	Action    string                 `json:"action,omitempty"`
	Message   string                 `json:"msg"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
	Caller    string                 `json:"caller,omitempty"` // file:line
}

// Logger là structured logger cho 1 service
type Logger struct {
	service   string
	minLevel  Level
	writers   []io.Writer // có thể write ra nhiều nơi (file + stdout)
	stdLogger *log.Logger
}

// Config cấu hình logger
type Config struct {
	Service  string // tên service: "okx-stream-producer", "okx-analytics-api"
	Level    Level  // minimum level để log
	LogFile  string // path đến log file, "" = stdout only
	JSONMode bool   // true = JSON format, false = human-readable
}

// NewLogger tạo logger mới theo config
// REUSE: Extends market-maker-background/utils/logger.go GetLogger()
func NewLogger(cfg Config) (*Logger, func(), error) {
	writers := []io.Writer{os.Stdout}
	var closeFn func()

	// Thêm file writer nếu cấu hình
	if cfg.LogFile != "" {
		file, err := os.OpenFile(cfg.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			return nil, nil, fmt.Errorf("open log file %s: %w", cfg.LogFile, err)
		}
		writers = append(writers, file)
		closeFn = func() { file.Close() }
	} else {
		closeFn = func() {}
	}

	// LEARN: io.MultiWriter = fan-out, ghi vào nhiều writers cùng lúc
	multiWriter := io.MultiWriter(writers...)

	return &Logger{
		service:   cfg.Service,
		minLevel:  cfg.Level,
		writers:   writers,
		stdLogger: log.New(multiWriter, "", 0), // 0 = không prefix (chúng ta tự format)
	}, closeFn, nil
}

// log ghi 1 entry với level, action, message và optional fields
// LEARN: Variadic map parameter — caller có thể truyền 0 hoặc 1 map fields
func (l *Logger) log(level Level, action, message string, fields map[string]interface{}) {
	if level < l.minLevel {
		return // skip nếu level thấp hơn cấu hình
	}

	// Lấy caller info (file:line) cho debug
	_, file, line, _ := runtime.Caller(2)
	// Chỉ lấy phần cuối của path (tránh path dài)
	parts := strings.Split(file, "/")
	caller := fmt.Sprintf("%s:%d", parts[len(parts)-1], line)

	entry := LogEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Level:     level.String(),
		Service:   l.service,
		Action:    action,
		Message:   message,
		Fields:    fields,
		Caller:    caller,
	}

	// Format JSON
	jsonBytes, err := json.Marshal(entry)
	if err != nil {
		// Fallback: nếu JSON marshal fail → log text đơn giản
		l.stdLogger.Printf("[%s] %s %s: %s", level, l.service, action, message)
		return
	}

	l.stdLogger.Println(string(jsonBytes))

	// FATAL: log xong rồi exit
	if level == FATAL {
		os.Exit(1)
	}
}

// Public methods — helpers để gọi dễ hơn

func (l *Logger) Debug(msg string, fields ...map[string]interface{}) {
	l.log(DEBUG, "", msg, mergeFields(fields))
}

func (l *Logger) Info(action, msg string, fields ...map[string]interface{}) {
	l.log(INFO, action, msg, mergeFields(fields))
}

func (l *Logger) Warn(action, msg string, fields ...map[string]interface{}) {
	l.log(WARN, action, msg, mergeFields(fields))
}

func (l *Logger) Error(action, msg string, fields ...map[string]interface{}) {
	l.log(ERROR, action, msg, mergeFields(fields))
}

func (l *Logger) Fatal(action, msg string, fields ...map[string]interface{}) {
	l.log(FATAL, action, msg, mergeFields(fields))
}

// WithFields tạo logger mới với default fields (không làm ô nhiễm logger gốc)
// LEARN: Functional options pattern — trả về mới thay vì modify in-place
func (l *Logger) WithFields(defaultFields map[string]interface{}) *FieldLogger {
	return &FieldLogger{logger: l, defaults: defaultFields}
}

// FieldLogger wrapper với default fields — dùng khi mọi log cùng context
// VD: logger.WithFields({"symbol":"BTC-USDT"}).Info(...)
type FieldLogger struct {
	logger   *Logger
	defaults map[string]interface{}
}

func (f *FieldLogger) Info(action, msg string, extra ...map[string]interface{}) {
	merged := mergeFields([]map[string]interface{}{f.defaults})
	for k, v := range mergeFields(extra) {
		merged[k] = v
	}
	f.logger.log(INFO, action, msg, merged)
}

func (f *FieldLogger) Error(action, msg string, extra ...map[string]interface{}) {
	merged := mergeFields([]map[string]interface{}{f.defaults})
	for k, v := range mergeFields(extra) {
		merged[k] = v
	}
	f.logger.log(ERROR, action, msg, merged)
}

func (f *FieldLogger) Warn(action, msg string, extra ...map[string]interface{}) {
	merged := mergeFields([]map[string]interface{}{f.defaults})
	for k, v := range mergeFields(extra) {
		merged[k] = v
	}
	f.logger.log(WARN, action, msg, merged)
}

// mergeFields kết hợp nhiều field maps thành 1
// LEARN: Variadic slice flatten — []map → merge tất cả
func mergeFields(fieldSlices []map[string]interface{}) map[string]interface{} {
	merged := make(map[string]interface{})
	for _, fields := range fieldSlices {
		for k, v := range fields {
			merged[k] = v
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}
