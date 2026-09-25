// REUSE: 100% từ trading-bot-finance-service/pkg/sources/vps/constants.go
// LEARN: Constants trong Go — typed constants tốt hơn string/int literals
//        VPS_CMD là custom string type → compiler bắt lỗi nếu dùng sai type

package vpsclient

// VPS API endpoints
const (
	// VPS_URL_LOGIN: endpoint xác thực, dùng domain partner riêng
	VPS_URL_LOGIN = "https://soapi-partner.vps.com.vn/Api/Proxy"
	// VPS_URL: endpoint chính cho market data và trading commands
	VPS_URL = "https://smartone.vps.com.vn/Api/Proxy"
	// VPS_URL_DATA_FEED: endpoint lấy market data realtime (không cần auth)
	VPS_URL_DATA_FEED = "https://bgapidatafeed.vps.com.vn"
)

// VPS_CMD định nghĩa các command codes của VPS API
// LEARN: Custom type trên string — type VPS_CMD string
//        Lợi ích: không thể truyền string thường vào nơi cần VPS_CMD
type VPS_CMD string

const (
	VPS_LOGIN         VPS_CMD = "Web.sCheckLogin"   // Đăng nhập, lấy session
	VPS_GET_STOCK_DATA VPS_CMD = "getStockData"      // Lấy realtime market data
	VPS_GET_STOCK_LIST VPS_CMD = "getAllStock"        // Lấy danh sách cổ phiếu
	VPS_NEW_ORDER     VPS_CMD = "Web.newOrder"       // Đặt lệnh mua/bán
	VPS_CANCEL_ORDER  VPS_CMD = "Web.cancelOrder"    // Hủy lệnh
	VPS_LIST_ORDER    VPS_CMD = "ListOrder"          // Tra cứu lệnh
)

// VPS_STATUS_RC là response code từ VPS API
// LEARN: iota — tự động đếm từ giá trị start (đây là iota - 2 → bắt đầu từ -2)
type VPS_STATUS_RC int8

const (
	VPS_RC_TIMEOUT  VPS_STATUS_RC = iota - 2 // = -2: request timeout
	VPS_RC_FAILED_1                           // = -1: lỗi loại 1
	VPS_RC_FAILED_2                           // =  0: lỗi loại 2 (NOTE: 0 là failed!)
	VPS_RC_SUCCESS                            // =  1: thành công
)

// VPS_STATUS_CODE — trạng thái lệnh chứng khoán
// LEARN: String constants — dùng cho VPS order status codes
type VPS_STATUS_CODE string

const (
	VPS_CODE_PENDING               VPS_STATUS_CODE = "0" // Chờ xử lý
	VPS_CODE_PENDING_TO_MATCH      VPS_STATUS_CODE = "1" // Đang chờ khớp
	VPS_CODE_PENDING_TO_CANCEL     VPS_STATUS_CODE = "2" // Đang chờ hủy
	VPS_CODE_PENDING_TO_EDIT       VPS_STATUS_CODE = "3" // Đang chờ sửa
	VPS_CODE_CANCELLED             VPS_STATUS_CODE = "4" // Đã hủy
	VPS_CODE_EDIT                  VPS_STATUS_CODE = "5" // Đã sửa
	VPS_CODE_PARTIALLY_MATCHED     VPS_STATUS_CODE = "6" // Khớp một phần
	VPS_CODE_MATCHED               VPS_STATUS_CODE = "7" // Đã khớp hoàn toàn
	VPS_CODE_PARTIAL_CANCELLED     VPS_STATUS_CODE = "8" // Khớp một phần, phần còn lại hủy
	VPS_CODE_PARTIAL_CANCEL_REMAIN VPS_STATUS_CODE = "9" // Hủy một phần
)

// VN Exchange identifiers
const (
	ExchangeHOSE  = "HOSE"  // Sàn HCMC — cổ phiếu vốn hóa lớn (VNM, VIC, HPG...)
	ExchangeHNX   = "HNX"   // Sàn Hà Nội — cổ phiếu vốn hóa trung bình
	ExchangeUPCOM = "UPCOM" // Sàn giao dịch công ty chưa niêm yết
)

// VPS Session config
const (
	// VPSSessionTTL: session VPS expires sau 30 phút (VPS quy định)
	// Refresh 5 phút trước khi hết để đảm bảo không bị gián đoạn
	VPSSessionTTL     = 30 // minutes
	VPSSessionRefresh = 25 // minutes — refresh before expiry
)
