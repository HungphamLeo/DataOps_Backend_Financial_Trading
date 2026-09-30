// REUSE: 100% copied from market-maker-background/utils/logger.go
// LEARN: Structured logging trong Go
//
// Dùng log.Logger chuẩn thư viện — đủ dùng cho production nhỏ
// Nâng cấp lên zap/zerolog khi cần structured JSON logging

package utils

import (
	"log"
	"os"
)

// GetLogger tạo logger ghi vào file (append mode)
// LEARN: os.OpenFile với flags:
//   - O_CREATE: tạo file nếu chưa có
//   - O_APPEND: ghi tiếp vào cuối file (không xóa data cũ)
//   - O_WRONLY: chỉ ghi, không đọc
func GetLogger(logFilePath string) (*log.Logger, *os.File, error) {
	file, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return nil, nil, err
	}

	// log.LstdFlags = in date + time ở đầu mỗi dòng log
	logger := log.New(file, "", log.LstdFlags)
	return logger, file, nil
}

// NewStdoutLogger tạo logger ghi ra stdout (terminal) — dùng trong Docker
// LEARN: os.Stdout là *os.File, tương thích với log.New
func NewStdoutLogger(prefix string) *log.Logger {
	return log.New(os.Stdout, prefix+" ", log.LstdFlags|log.Lshortfile)
}
