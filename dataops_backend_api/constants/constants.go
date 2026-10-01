// Package constants chứa tất cả hằng số dùng chung trong dataops-backend-api.
//
// Phân loại:
//   - AUTH_ROLE      : role-based access control (RBAC)
//   - USER_STATUS_*  : trạng thái tài khoản người dùng
//   - ROLE_*         : alias uint cho role ID (dùng với JWTClaims.RoleID)
//   - ASSET_TYPE     : phân loại tài sản (crypto, stock)
//   - EXCHANGE       : định danh sàn giao dịch
//   - TF_*           : timeframe/interval chuẩn
//
// REUSE: trading-bot-api/constants/
package constants

// AUTH_ROLE biểu diễn mức quyền truy cập của người dùng trong hệ thống.
// Dùng để kiểm tra phân quyền trong middleware và service layer.
type AUTH_ROLE int

const (
	USER        AUTH_ROLE = 1
	ADMIN       AUTH_ROLE = 2
	SUPER_ADMIN AUTH_ROLE = 3
)

// ROLE_* là alias uint để map với JWTClaims.RoleID và UserSession.RoleID.
// Phải giữ giá trị đồng bộ với AUTH_ROLE ở trên.
const (
	ROLE_USER        uint = 1 // người dùng thông thường
	ROLE_ADMIN       uint = 2 // quản trị viên — truy cập đầy đủ
	ROLE_SUPER_ADMIN uint = 3 // super admin — toàn quyền hệ thống
)

// USER_STATUS_* là trạng thái tài khoản người dùng lưu trong Redis session.
// JwtAuthMiddleware kiểm tra các giá trị này để cho phép hoặc từ chối request.
const (
	USER_STATUS_ACTIVE   = "active"   // tài khoản đang hoạt động bình thường
	USER_STATUS_INACTIVE = "inactive" // tài khoản chưa kích hoạt hoặc bị vô hiệu hóa
	USER_STATUS_BLOCKED  = "blocked"  // tài khoản bị khoá — từ chối 403 Forbidden
	USER_STATUS_TRIAL    = "trial"    // tài khoản dùng thử — giới hạn dữ liệu trả về
)

// ASSET_TYPE phân loại loại tài sản giao dịch trong hệ thống.
type ASSET_TYPE uint8

const (
	CRYPTO   ASSET_TYPE = 1 // tài sản tiền điện tử (Binance)
	VN_STOCK ASSET_TYPE = 2 // cổ phiếu thị trường chứng khoán Việt Nam (VPS/HOSE)
)

// EXCHANGE là định danh chuỗi cho từng sàn giao dịch tích hợp.
// Dùng làm tag trong InfluxDB và filter param trong query.
type EXCHANGE string

const (
	BINANCE_FUTURE EXCHANGE = "binance_future" // Binance Perpetual Futures (USD-M)
	BINANCE_SPOT   EXCHANGE = "binance_spot"   // Binance Spot market
	VPS_STOCK      EXCHANGE = "vps_stock"      // VPS chứng khoán cơ sở (HOSE)
	VPS_FUTURE     EXCHANGE = "vps_future"     // VPS phái sinh (VN30F)
)

// TF_* là các timeframe/interval chuẩn dùng trong OHLCV, VWAP, và analytics queries.
// Giá trị phải khớp với interval field trong InfluxDB measurement.
const (
	TF_1M  = "1m"  // 1 phút
	TF_5M  = "5m"  // 5 phút
	TF_15M = "15m" // 15 phút
	TF_1H  = "1h"  // 1 giờ
	TF_4H  = "4h"  // 4 giờ
	TF_1D  = "1d"  // 1 ngày
)
