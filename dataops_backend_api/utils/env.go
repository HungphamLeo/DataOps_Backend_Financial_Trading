// REUSE: 100% copied from market-maker-background/utils/env.go
// LEARN: Environment variables trong Go — 12-Factor App methodology
//
// Tất cả config đều đến từ environment variables (không hardcode)
// Giúp deploy cùng 1 binary lên nhiều môi trường (dev/staging/prod)

package utils

import (
	"os"
	"strconv"
)

// GetEnv đọc env var, trả về defaultValue nếu không tồn tại
// LEARN: os.Getenv trả về "" nếu không tìm thấy — không panic, không error
func GetEnv(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// GetEnvInt đọc env var dạng số nguyên
// LEARN: strconv.Atoi chuyển string → int, trả về (int, error)
func GetEnvInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	intVal, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return intVal
}

// GetEnvBool đọc env var dạng boolean ("true"/"false"/"1"/"0")
func GetEnvBool(key string, defaultValue bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	boolVal, err := strconv.ParseBool(value)
	if err != nil {
		return defaultValue
	}
	return boolVal
}

// MustGetEnv đọc env var bắt buộc — panic nếu không tồn tại
// LEARN: Dùng khi app không thể chạy nếu thiếu config (DB password, API key...)
func MustGetEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		panic("required environment variable not set: " + key)
	}
	return value
}
