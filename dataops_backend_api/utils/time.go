// REUSE: 100% copied from market-maker-background/utils/time.go
// LEARN: Utility package pattern — small pure functions, no state, no side effects
//
// Tất cả utils đều stateless = dễ test, dễ reuse, không cần mock

package utils

import "time"

// GetTimeNow trả về thời gian hiện tại theo định dạng chuẩn log
func GetTimeNow() string {
	return time.Now().Format("2006-01-02 15:04:05")
}

// GetCurrentTimeMs trả về Unix timestamp tính bằng milliseconds
// LEARN: time.Now().UnixMilli() là Go 1.17+, UnixNano()/1e6 cho Go cũ hơn
func GetCurrentTimeMs() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// GetCurrentTimeSec trả về Unix timestamp tính bằng giây
func GetCurrentTimeSec() int64 {
	return time.Now().Unix()
}

// FormatTimeRFC3339 format time sang chuẩn RFC3339 — dùng trong JSON API responses
// LEARN: time.RFC3339 = "2006-01-02T15:04:05Z07:00" — Go dùng reference time cụ thể
func FormatTimeRFC3339(t time.Time) string {
	return t.Format(time.RFC3339)
}

// ParseTimeRFC3339 parse chuỗi thời gian RFC3339 từ API requests
func ParseTimeRFC3339(s string) (time.Time, error) {
	return time.Parse(time.RFC3339, s)
}

// IsVNTradingHours kiểm tra xem có phải giờ giao dịch VPS không
// HOSE/HNX/UPCOM: 9:00-11:30 và 13:00-14:45 ICT, Thứ 2-6
// LEARN: time.LoadLocation — timezone handling đúng chuẩn (tránh hardcode UTC+7)
func IsVNTradingHours() bool {
	loc, err := time.LoadLocation("Asia/Ho_Chi_Minh")
	if err != nil {
		return false
	}

	now := time.Now().In(loc)

	// Chỉ giao dịch Thứ 2-6 (Monday=1 .. Friday=5)
	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return false
	}

	h, m, _ := now.Clock()
	minuteOfDay := h*60 + m

	// Phiên sáng: 9:00 (540) - 11:30 (690)
	morningOpen := 9*60 + 0
	morningClose := 11*60 + 30

	// Phiên chiều: 13:00 (780) - 14:45 (885)
	afternoonOpen := 13*60 + 0
	afternoonClose := 14*60 + 45

	return (minuteOfDay >= morningOpen && minuteOfDay < morningClose) ||
		(minuteOfDay >= afternoonOpen && minuteOfDay < afternoonClose)
}
