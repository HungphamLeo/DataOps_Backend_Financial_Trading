// constants.go — TCBS iFlash Open API typed constants
//
// Single source of truth cho tất cả enums, endpoint paths, codes của TCBS.
// Reference: openapi-v1.0.0.json — TCBS iFlash Open API v1.0.0
// Base URL:  https://openapi.tcbs.com.vn
// Contact:   openapi@tcbs.com.vn
//
// QUAN TRỌNG — Không được hardcode bất kỳ giá trị nào từ file này vào các
// file khác. Luôn import và dùng constant từ package này.
//
// Pattern sử dụng:
//
//	import tcbsconst "dataops-stream-producer/pkg/sources/tcbs/constants"
//	url := tcbsconst.BaseURL + tcbsconst.EndpointTickerCommons
//	code := tcbsconst.IndexCodeHOSE
package constants

// ─── Base URL ─────────────────────────────────────────────────────────────────

const (
	// BaseURL là base URL duy nhất của TCBS iFlash Open API
	BaseURL = "https://openapi.tcbs.com.vn"
)

// ─── API Endpoints (đầy đủ theo openapi-v1.0.0.json) ─────────────────────────

// TCBSEndpoint là custom type để compiler bắt lỗi khi truyền sai string literal
type TCBSEndpoint string

const (
	// Auth (Gaia service)
	EndpointToken TCBSEndpoint = "/gaia/v1/oauth2/openapi/token"

	// Market Data — Tartarus service
	EndpointTickerCommons    TCBSEndpoint = "/tartarus/v1/tickerCommons"
	EndpointTickerSnaps      TCBSEndpoint = "/tartarus/v1/tickerSnaps"
	EndpointPutThroughSnaps  TCBSEndpoint = "/tartarus/v1/putThroughSnaps"
	EndpointDerivatives      TCBSEndpoint = "/tartarus/v1/derivatives"
	EndpointDerivContracts   TCBSEndpoint = "/api/v1/derivatives/contracts"
	EndpointSecurities       TCBSEndpoint = "/ananke/v1/securities"

	// Intraday Analytics — Nyx service
	// Dùng fmt.Sprintf(string(EndpointIntradayBSA), ticker) để build URL
	EndpointIntradayBSA     TCBSEndpoint = "/nyx/v1/intraday/%s/bsa"
	EndpointIntradayBSAExt  TCBSEndpoint = "/nyx/v1/intraday/%s/bsa-ext"
	EndpointIntradayBSAMonth TCBSEndpoint = "/nyx/v1/intraday/%s/bsa-month"
	EndpointIntradayHistory  TCBSEndpoint = "/nyx/v1/intraday/%s/his/paging"

	// Account (Aion service)
	EndpointAccountCash        TCBSEndpoint = "/aion/v1/accounts/%s/cashInvestments"
	EndpointAccountOrders      TCBSEndpoint = "/aion/v1/accounts/%s/orders"
	EndpointAccountOrderDetail TCBSEndpoint = "/aion/v1/accounts/%s/orders/%s"
	EndpointAccountMatchDetail TCBSEndpoint = "/aion/v1/accounts/%s/matching-details"
	EndpointAccountPPSE        TCBSEndpoint = "/aion/v1/accounts/%s/ppse"
	EndpointAccountStockAsset  TCBSEndpoint = "/aion/v1/accounts/%s/se"
	EndpointCustomerAccounts   TCBSEndpoint = "/aion/v1/customers/%s/accounts"

	// Order Management (Akhlys service)
	EndpointOrderPlace  TCBSEndpoint = "/akhlys/v1/accounts/%s/orders"
	EndpointOrderUpdate TCBSEndpoint = "/akhlys/v1/accounts/%s/orders/%s"
	EndpointOrderCancel TCBSEndpoint = "/akhlys/v1/accounts/%s/cancel-orders"

	// Derivative Trading (Khronos service)
	EndpointDerivOrderPlace          TCBSEndpoint = "/khronos/v1/order/place"
	EndpointDerivOrderChange         TCBSEndpoint = "/khronos/v1/order/change"
	EndpointDerivOrderCancel         TCBSEndpoint = "/khronos/v1/order/cancel"
	EndpointDerivOrderInDay          TCBSEndpoint = "/khronos/v1/order/in-day"
	EndpointDerivConditionPlace      TCBSEndpoint = "/khronos/v1/order/condition/place"
	EndpointDerivConditionChange     TCBSEndpoint = "/khronos/v2/order/condition/change"
	EndpointDerivConditionCancel     TCBSEndpoint = "/khronos/v1/order/condition/cancel"
	EndpointDerivConditionDetail     TCBSEndpoint = "/khronos/v1/order/condition/detail"
	EndpointDerivAccountStatus       TCBSEndpoint = "/khronos/v1/account/status"
	EndpointDerivPortfolioStatus     TCBSEndpoint = "/khronos/v1/account/portfolio/status"
	EndpointDerivPortfolioClose      TCBSEndpoint = "/khronos/v1/account/portfolio/position/close"
	EndpointDerivCashDeposit         TCBSEndpoint = "/khronos/v1/cash/deposit/update"
	EndpointDerivCashWithdraw        TCBSEndpoint = "/khronos/v1/cash/withdraw/update"

	// Margin & Risk (Hydros / Erebos / Khaos)
	EndpointAccountRisk       TCBSEndpoint = "/hydros/v1/account/%s/risk"
	EndpointPricingPolicy     TCBSEndpoint = "/hydros/v1/account/%s/pricing-policy"
	EndpointMarginInfo        TCBSEndpoint = "/erebos/v2/digital/margin-info"
	EndpointCashStatements    TCBSEndpoint = "/erebos/v2/digital/trans-hist-cashStatements"
	EndpointLoanList          TCBSEndpoint = "/khaos/v1/loan/%s"
	EndpointMarginAddonDetail TCBSEndpoint = "/campaign-management/v1/margin/subscription/%s/addons/detail"

	// Customer (Eros service)
	EndpointSubAccountProfile TCBSEndpoint = "/eros/v2/get-profile/by-username/%s"

	// Stock Transfer (Physis service)
	EndpointStockTransfer TCBSEndpoint = "/physis/v1/stock/transfer"
)

// ─── Index / Basket Codes ─────────────────────────────────────────────────────
// Dùng cho query param ?index= trong EndpointTickerCommons và EndpointTickerSnaps

// TCBSIndexCode là numeric code cho index basket
type TCBSIndexCode int

const (
	IndexCodeHOSE        TCBSIndexCode = 1
	IndexCodeVN30        TCBSIndexCode = 2
	IndexCodeHNX         TCBSIndexCode = 3
	IndexCodeHNX30       TCBSIndexCode = 4
	IndexCodeUPCOM       TCBSIndexCode = 5
	IndexCodeMidcap      TCBSIndexCode = 10
	IndexCodeVN100       TCBSIndexCode = 11
	IndexCodeVNAllShare  TCBSIndexCode = 12
	IndexCodeVNSmallCap  TCBSIndexCode = 13
	IndexCodeVNXAllShare TCBSIndexCode = 14
	IndexCodeVN50        TCBSIndexCode = 15
	IndexCodeVNSI        TCBSIndexCode = 16
)

// IndexNameToCode map tên index → numeric code.
// Dùng khi cần convert từ config string sang API param.
var IndexNameToCode = map[string]TCBSIndexCode{
	"HOSE":         IndexCodeHOSE,
	"VN30":         IndexCodeVN30,
	"HNX":          IndexCodeHNX,
	"HNX30":        IndexCodeHNX30,
	"UPCOM":        IndexCodeUPCOM,
	"MIDCAP":       IndexCodeMidcap,
	"VN100":        IndexCodeVN100,
	"VN_ALL_SHARE": IndexCodeVNAllShare,
	"VN_SMALL_CAP": IndexCodeVNSmallCap,
	"VNX_ALL_SHARE": IndexCodeVNXAllShare,
	"VN50":         IndexCodeVN50,
	"VNSI":         IndexCodeVNSI,
}

// ─── Price Types ─────────────────────────────────────────────────────────────
// Dùng cho field priceType trong /akhlys/v1/accounts/{accountNo}/orders

// TCBSPriceType là loại lệnh cổ phiếu cơ sở
type TCBSPriceType string

const (
	PriceTypeLO  TCBSPriceType = "LO"  // Limit Order — lệnh giới hạn
	PriceTypeATO TCBSPriceType = "ATO" // At The Open — khớp tại giá mở cửa
	PriceTypeATC TCBSPriceType = "ATC" // At The Close — khớp tại giá đóng cửa
	PriceTypePLO TCBSPriceType = "PLO" // Peg Limit Order
	PriceTypeMP  TCBSPriceType = "MP"  // Market Price (HNX)
	PriceTypeMTL TCBSPriceType = "MTL" // Match To Limit (HOSE)
	PriceTypeMOK TCBSPriceType = "MOK" // Match Or Kill (HOSE)
	PriceTypeMAK TCBSPriceType = "MAK" // Match And Kill (HOSE)
)

// ─── Order Direction ─────────────────────────────────────────────────────────
// Dùng cho field execType trong /akhlys/v1/accounts/{accountNo}/orders

// TCBSExecType là chiều lệnh (mua/bán)
type TCBSExecType string

const (
	ExecTypeNB TCBSExecType = "NB" // Normal Buy — mua thông thường
	ExecTypeNS TCBSExecType = "NS" // Normal Sell — bán thông thường
)

// ─── Derivative Order Side ────────────────────────────────────────────────────
// Dùng cho field side trong /khronos/v1/order/place

// TCBSDerivSide là chiều lệnh phái sinh
type TCBSDerivSide string

const (
	DerivSideBuy  TCBSDerivSide = "B" // Buy — mua phái sinh
	DerivSideSell TCBSDerivSide = "S" // Sell — bán phái sinh
)

// ─── BSA Investor Types ───────────────────────────────────────────────────────
// Dùng cho query param ?type= trong EndpointIntradayBSA

// TCBSInvestorType là loại nhà đầu tư cho BSA analysis
type TCBSInvestorType string

const (
	InvestorTypeSheep TCBSInvestorType = "sheep" // Nhỏ lẻ (small retail)
	InvestorTypeWolf  TCBSInvestorType = "wolf"  // Tổ chức vừa (medium institutional)
	InvestorTypeShark TCBSInvestorType = "shark" // Tổ chức lớn (large institutional)
	InvestorTypeAll   TCBSInvestorType = "all"   // Tất cả (default)
)

// ─── Session States ───────────────────────────────────────────────────────────

// TCBSSessionState là trạng thái phiên giao dịch
type TCBSSessionState string

const (
	SessionStateATO        TCBSSessionState = "ATO"        // At The Open
	SessionStateATC        TCBSSessionState = "ATC"        // At The Close
	SessionStateContinuous TCBSSessionState = "CONTINUOUS" // Khớp lệnh liên tục
	SessionStateClosed     TCBSSessionState = "CLOSED"     // Đã đóng cửa
	SessionStateHalt       TCBSSessionState = "HALT"       // Tạm ngưng
	SessionStateUnknown    TCBSSessionState = "UNKNOWN"    // Không xác định
)

// ─── Result Codes ─────────────────────────────────────────────────────────────

const (
	// ResultCodeStockOrderSuccess — error field = "0" cho lệnh cổ phiếu cơ sở (/akhlys)
	ResultCodeStockOrderSuccess = "0"
	// ResultCodeDerivOrderSuccess — rc field = "1" cho lệnh phái sinh (/khronos)
	ResultCodeDerivOrderSuccess = "1"
)

// ─── Exchange Names ───────────────────────────────────────────────────────────

// TCBSExchange là tên sàn giao dịch
type TCBSExchange string

const (
	ExchangeHOSE  TCBSExchange = "HOSE"  // Sàn HCMC
	ExchangeHNX   TCBSExchange = "HNX"   // Sàn Hà Nội
	ExchangeUPCOM TCBSExchange = "UPCOM" // Sàn UPCoM
	ExchangeDeriv TCBSExchange = "DERIV" // Thị trường Phái sinh
)

// ─── HTTP config ──────────────────────────────────────────────────────────────

const (
	// DefaultTimeout request timeout (giây)
	DefaultTimeout = 15

	// DefaultRetryMax số lần retry tối đa
	DefaultRetryMax = 3

	// TokenRefreshThreshold — refresh token khi còn < 1h (giây)
	TokenRefreshThreshold = 3600

	// IntraHistoryMaxSize — số lệnh tối đa mỗi page (theo spec)
	IntraHistoryMaxSize = 100

	// SymbolsBatchSize — số symbols tối đa mỗi request tickerCommons (conservative)
	SymbolsBatchSize = 50
)

// ─── Kafka Topics (xem constants/topics.go) ────────────────────────────────
// Topics cho TCBS streaming không được định nghĩa ở đây —
// dùng constants package gốc: dataops-stream-producer/constants
