// AUTH_ROLE và các constants dùng chung
// REUSE: trading-bot-api/constants/
package constants

// AUTH_ROLE — quyền truy cập
type AUTH_ROLE int

const (
	USER        AUTH_ROLE = 1
	ADMIN       AUTH_ROLE = 2
	SUPER_ADMIN AUTH_ROLE = 3
)

// ASSET_TYPE — loại tài sản
type ASSET_TYPE uint8

const (
	CRYPTO   ASSET_TYPE = 1
	VN_STOCK ASSET_TYPE = 2
)

// EXCHANGE — sàn giao dịch
type EXCHANGE string

const (
	OKX_FUTURE  EXCHANGE = "okx_future"
	OKX_SPOT    EXCHANGE = "okx_spot"
	VPS_STOCK   EXCHANGE = "vps_stock"
	VPS_FUTURE  EXCHANGE = "vps_future"
)

// Timeframe — khung thời gian analytics
const (
	TF_1M  = "1m"
	TF_5M  = "5m"
	TF_15M = "15m"
	TF_1H  = "1h"
	TF_4H  = "4h"
	TF_1D  = "1d"
)
