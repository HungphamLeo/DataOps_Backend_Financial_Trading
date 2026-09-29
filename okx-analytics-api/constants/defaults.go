// Package constants chứa tất cả giá trị cố định dùng chung trong okx-analytics-api.
//
// File này tập trung các DEFAULT VALUES và TIME RANGE LIMITS để tránh hardcode
// rải rác trong handlers. Khi cần thay đổi ngưỡng hoặc default, chỉ sửa tại đây.
//
// Phân loại:
//   - DefaultTimeRange*  : khoảng thời gian mặc định khi client không truyền from/to
//   - MaxTimeRange*      : giới hạn tối đa cho phép per request (bảo vệ InfluxDB)
//   - DefaultInterval*   : interval mặc định theo loại dữ liệu
//   - Default*Limit      : giới hạn số bản ghi trả về
//   - WS*                : cấu hình WebSocket heartbeat
package constants

import "time"

// ─── Default Time Ranges ──────────────────────────────────────────────────────
// Khoảng thời gian mặc định khi client không truyền query param from/to.

const (
	// DefaultVWAPLookback là khoảng nhìn lại mặc định cho VWAP query.
	// 24 giờ phù hợp với intraday trading analysis.
	DefaultVWAPLookback = -24 * time.Hour

	// DefaultOHLCVLookback là khoảng mặc định cho OHLCV crypto (7 ngày = 1 tuần giao dịch).
	DefaultOHLCVLookback = -7 * 24 * time.Hour

	// DefaultFundingLookback là khoảng mặc định cho funding rate (30 ngày = 1 chu kỳ tháng).
	DefaultFundingLookback = -30 * 24 * time.Hour

	// DefaultLiquidationLookback là khoảng mặc định cho liquidation events (7 ngày).
	DefaultLiquidationLookback = -7 * 24 * time.Hour

	// DefaultVNStockOHLCVLookback là khoảng mặc định cho VN stock OHLCV (90 ngày = ~1 quý).
	DefaultVNStockOHLCVLookback = -90 * 24 * time.Hour
)

// ─── Max Time Ranges ──────────────────────────────────────────────────────────
// Giới hạn tối đa per request để tránh query InfluxDB quá lớn gây OOM.

const (
	// MaxVWAPLookback giới hạn VWAP query tối đa 7 ngày.
	MaxVWAPLookback = 7 * 24 * time.Hour

	// MaxOHLCVLookback giới hạn OHLCV crypto query tối đa 1 năm.
	MaxOHLCVLookback = 365 * 24 * time.Hour

	// MaxFundingLookback giới hạn funding rate query tối đa 90 ngày.
	MaxFundingLookback = 90 * 24 * time.Hour

	// MaxLiquidationLookback giới hạn liquidation query tối đa 30 ngày.
	MaxLiquidationLookback = 30 * 24 * time.Hour

	// MaxVNStockOHLCVLookback giới hạn VN stock query tối đa 1 năm.
	MaxVNStockOHLCVLookback = 365 * 24 * time.Hour
)

// ─── Default Intervals ────────────────────────────────────────────────────────
// Timeframe mặc định khi client không truyền interval param.

const (
	// DefaultCryptoVWAPInterval là interval mặc định cho VWAP.
	// 5m phù hợp với short-term momentum analysis.
	DefaultCryptoVWAPInterval = TF_5M

	// DefaultCryptoOHLCVInterval là interval mặc định cho crypto OHLCV.
	DefaultCryptoOHLCVInterval = TF_1H

	// DefaultVNStockInterval là interval mặc định cho VN stock OHLCV.
	// 1d vì VPS chỉ cung cấp daily data qua REST.
	DefaultVNStockInterval = TF_1D
)

// ─── Pagination Limits ────────────────────────────────────────────────────────

const (
	// DefaultPageSize là số bản ghi mặc định mỗi trang.
	DefaultPageSize = 20

	// MaxPageSize là số bản ghi tối đa mỗi trang để tránh response quá lớn.
	MaxPageSize = 100

	// DefaultTopMoversLimit là số lượng top movers mặc định.
	DefaultTopMoversLimit = 10

	// MaxTopMoversLimit là giới hạn tối đa cho top movers query.
	MaxTopMoversLimit = 50

	// DefaultPnLDays là số ngày P&L mặc định.
	DefaultPnLDays = 30

	// MaxPnLDays là giới hạn tối đa ngày P&L có thể query.
	MaxPnLDays = 365
)

// ─── HTTP Server ──────────────────────────────────────────────────────────────

const (
	// DefaultHTTPPort là cổng mặc định mà REST API lắng nghe.
	// Đặt tại đây để server.go không hardcode — dễ thay khi deploy multi-port.
	DefaultHTTPPort = 8080
)

// ─── WebSocket Config ─────────────────────────────────────────────────────────

const (
	// WSReadLimit giới hạn kích thước message đọc vào (bytes).
	// Chỉ cần đọc control frames (pong), không cần nhận data lớn.
	WSReadLimit = 512

	// WSPingDeadline là thời gian tối đa chờ pong frame từ client.
	// Sau khoảng này mà không nhận pong → coi như client đã ngắt kết nối.
	WSPingDeadline = 60 * time.Second

	// WSBroadcastBufferSize là số lượng messages tối đa trong broadcast channel.
	// Nếu buffer đầy, BroadcastAlert() sẽ block cho đến khi Run() tiêu thụ bớt.
	WSBroadcastBufferSize = 1024
)

// ─── Default Exchanges ────────────────────────────────────────────────────────

const (
	// DefaultCryptoExchange là sàn crypto mặc định khi không truyền tham số.
	DefaultCryptoExchange = "OKX"

	// DefaultVNExchange là sàn chứng khoán VN mặc định.
	DefaultVNExchange = "HOSE"

	// DefaultCurrency là đơn vị tiền tệ mặc định cho accounting.
	DefaultCurrency = "USDT"
)
