// tcbs.go — TCBS iFlash Open API typed constants cho dataops-backend-api.
//
// Single source of truth cho tất cả TCBS-specific enums, exchange codes,
// index basket codes, BSA investor types, và session states.
//
// Reference: openapi-v1.0.0.json — TCBS iFlash Open API v1.0.0
// Base URL:  https://openapi.tcbs.com.vn
//
// Tất cả handlers, services, repository đều PHẢI import từ package này.
// Không được hardcode bất kỳ giá trị nào từ file này vào code khác.
package constants

// ─── TCBS Exchange / Basket Names ────────────────────────────────────────────

// TCBSExchange là định danh chuỗi cho các sàn giao dịch VN được TCBS hỗ trợ.
type TCBSExchange string

const (
	// TCBSExchangeHOSE là Sàn Giao dịch Chứng khoán TP.HCM
	TCBSExchangeHOSE  TCBSExchange = "HOSE"
	// TCBSExchangeHNX là Sàn Giao dịch Chứng khoán Hà Nội
	TCBSExchangeHNX   TCBSExchange = "HNX"
	// TCBSExchangeUPCOM là thị trường UPCoM
	TCBSExchangeUPCOM TCBSExchange = "UPCOM"
	// TCBSExchangeDeriv là thị trường Phái sinh (VSD)
	TCBSExchangeDeriv TCBSExchange = "DERIV"
)

// ─── TCBS Index Basket Codes ──────────────────────────────────────────────────
// Dùng cho query param ?index= trong /tartarus/v1/tickerCommons và /tartarus/v1/tickerSnaps

// TCBSIndexCode là numeric code cho index basket của TCBS.
// Phải khớp với openapi-v1.0.0.json description của param ?index=
type TCBSIndexCode int

const (
	TCBSIndexHOSE        TCBSIndexCode = 1
	TCBSIndexVN30        TCBSIndexCode = 2
	TCBSIndexHNX         TCBSIndexCode = 3
	TCBSIndexHNX30       TCBSIndexCode = 4
	TCBSIndexUPCOM       TCBSIndexCode = 5
	TCBSIndexMidcap      TCBSIndexCode = 10
	TCBSIndexVN100       TCBSIndexCode = 11
	TCBSIndexVNAllShare  TCBSIndexCode = 12
	TCBSIndexVNSmallCap  TCBSIndexCode = 13
	TCBSIndexVNXAllShare TCBSIndexCode = 14
	TCBSIndexVN50        TCBSIndexCode = 15
	TCBSIndexVNSI        TCBSIndexCode = 16
)

// TCBSIndexNameToCode map tên basket → numeric code.
// Dùng khi parse query param "index=HOSE" → int để gọi TCBS API.
var TCBSIndexNameToCode = map[string]TCBSIndexCode{
	"HOSE":          TCBSIndexHOSE,
	"VN30":          TCBSIndexVN30,
	"HNX":           TCBSIndexHNX,
	"HNX30":         TCBSIndexHNX30,
	"UPCOM":         TCBSIndexUPCOM,
	"MIDCAP":        TCBSIndexMidcap,
	"VN100":         TCBSIndexVN100,
	"VN_ALL_SHARE":  TCBSIndexVNAllShare,
	"VN_SMALL_CAP":  TCBSIndexVNSmallCap,
	"VNX_ALL_SHARE": TCBSIndexVNXAllShare,
	"VN50":          TCBSIndexVN50,
	"VNSI":          TCBSIndexVNSI,
}

// IsValidTCBSIndex kiểm tra index name có hợp lệ không.
func IsValidTCBSIndex(indexName string) bool {
	_, ok := TCBSIndexNameToCode[indexName]
	return ok
}

// ─── TCBS BSA Investor Types ──────────────────────────────────────────────────
// Dùng cho query param ?type= trong /nyx/v1/intraday/{ticker}/bsa

// TCBSInvestorType là loại nhà đầu tư cho BSA (Buy/Sell Analysis).
type TCBSInvestorType string

const (
	// TCBSInvestorSheep là nhà đầu tư nhỏ lẻ (small retail)
	TCBSInvestorSheep TCBSInvestorType = "sheep"
	// TCBSInvestorWolf là tổ chức vừa (medium institutional)
	TCBSInvestorWolf  TCBSInvestorType = "wolf"
	// TCBSInvestorShark là tổ chức lớn (large institutional)
	TCBSInvestorShark TCBSInvestorType = "shark"
	// TCBSInvestorAll là tất cả loại nhà đầu tư (default)
	TCBSInvestorAll   TCBSInvestorType = "all"
)

// validTCBSInvestorTypes là tập hợp các investor type hợp lệ.
var validTCBSInvestorTypes = map[TCBSInvestorType]bool{
	TCBSInvestorSheep: true,
	TCBSInvestorWolf:  true,
	TCBSInvestorShark: true,
	TCBSInvestorAll:   true,
}

// IsValidTCBSInvestorType kiểm tra investor type có hợp lệ không.
func IsValidTCBSInvestorType(t string) bool {
	return validTCBSInvestorTypes[TCBSInvestorType(t)]
}

// ─── TCBS Session States ──────────────────────────────────────────────────────

// TCBSSessionState là trạng thái phiên giao dịch VN.
type TCBSSessionState string

const (
	TCBSSessionATO        TCBSSessionState = "ATO"        // At The Open
	TCBSSessionATC        TCBSSessionState = "ATC"        // At The Close
	TCBSSessionContinuous TCBSSessionState = "CONTINUOUS" // Khớp lệnh liên tục
	TCBSSessionClosed     TCBSSessionState = "CLOSED"     // Đã đóng cửa
	TCBSSessionHalt       TCBSSessionState = "HALT"       // Tạm ngưng
	TCBSSessionUnknown    TCBSSessionState = "UNKNOWN"    // Không xác định
)

// ─── InfluxDB Measurement Names ───────────────────────────────────────────────
// Tên measurement trong InfluxDB cho TCBS data.
// Consumer viết theo measurement name này → handler query đúng measurement.

const (
	// InfluxMeasurementTCBSMarket là measurement cho TCBS full market snapshot.
	// Data source: tcbs.market.snapshot Kafka topic → Spark → InfluxDB
	InfluxMeasurementTCBSMarket   = "tcbs_market"
	// InfluxMeasurementTCBSBSA là measurement cho Buy/Sell Analysis data.
	// Data source: tcbs.bsa.events Kafka topic → Spark → InfluxDB
	InfluxMeasurementTCBSBSA      = "tcbs_bsa"
	// InfluxMeasurementTCBSIntraday là measurement cho intraday tick data.
	// Data source: tcbs.intraday.ticks Kafka topic → Spark → InfluxDB
	InfluxMeasurementTCBSIntraday = "tcbs_intraday"
	// InfluxMeasurementTCBSOHLCV là measurement cho OHLCV aggregated từ Delta Silver.
	InfluxMeasurementTCBSOHLCV    = "tcbs_ohlcv"
)

// ─── TCBS Symbol Registry ─────────────────────────────────────────────────────

// tcbsHOSESymbols là tập mã cổ phiếu HOSE được hỗ trợ qua TCBS API.
// Đồng bộ với cấu hình bsa_tickers trong exchange_config.yaml của batch pipeline.
var tcbsHOSESymbols = map[string]bool{
	// VN30 Bluechips
	"VCB": true, "BID": true, "CTG": true, "TCB": true, "MBB": true,
	"VPB": true, "HPG": true, "VNM": true, "MSN": true, "MWG": true,
	"FPT": true, "VIC": true, "VHM": true, "VRE": true, "GVR": true,
	"SAB": true, "PLX": true, "GAS": true, "POW": true, "HDB": true,
	// Mid-cap HOSE
	"ACB": true, "STB": true, "EIB": true, "LPB": true, "SHB": true,
	"NVL": true, "PDR": true, "DXG": true, "KDH": true, "BCM": true,
}

// tcbsHOSEIndices là các market index HOSE được TCBS cung cấp.
var tcbsHOSEIndices = map[string]bool{
	"VNINDEX": true,
	"VN30":    true,
	"VN100":   true,
}

// IsValidTCBSSymbol kiểm tra mã CK có nằm trong danh sách HOSE được hỗ trợ không.
// Chấp nhận cả mã cổ phiếu (VCB, HPG) lẫn market indices (VNINDEX, VN30).
func IsValidTCBSSymbol(symbol string) bool {
	return tcbsHOSESymbols[symbol] || tcbsHOSEIndices[symbol]
}

// IsValidTCBSStockSymbol kiểm tra mã có phải cổ phiếu (không phải index) không.
func IsValidTCBSStockSymbol(symbol string) bool {
	return tcbsHOSESymbols[symbol]
}

// IsTCBSMarketIndex kiểm tra symbol có phải HOSE market index không.
func IsTCBSMarketIndex(symbol string) bool {
	return tcbsHOSEIndices[symbol]
}

// GetTCBSHOSESymbols trả về slice tất cả mã HOSE đang được hỗ trợ (không bao gồm indices).
func GetTCBSHOSESymbols() []string {
	result := make([]string, 0, len(tcbsHOSESymbols))
	for s := range tcbsHOSESymbols {
		result = append(result, s)
	}
	return result
}

// ─── Default TCBS Query Params ────────────────────────────────────────────────

const (
	// DefaultTCBSIndex là basket index mặc định khi không truyền param.
	DefaultTCBSIndex = "HOSE"

	// DefaultTCBSInvestorType là investor type mặc định cho BSA query.
	DefaultTCBSInvestorType = "all"

	// DefaultTCBSIntradayPageSize là số lệnh khớp mỗi trang intraday history.
	DefaultTCBSIntradayPageSize = 100

	// MaxTCBSIntradayPages là số trang tối đa để tránh response quá lớn.
	MaxTCBSIntradayPages = 10

	// DefaultTCBSOHLCVLookback là khoảng nhìn lại mặc định cho TCBS OHLCV (90 ngày = 1 quý).
	DefaultTCBSOHLCVLookback = DefaultVNStockOHLCVLookback
)
