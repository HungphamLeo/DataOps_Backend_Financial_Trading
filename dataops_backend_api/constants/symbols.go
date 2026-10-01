// symbols.go — Symbol registry và validation cho Binance và VPS Securities.
//
// Vấn đề cần giải quyết:
//
//	Binance dùng format "BTCUSDT", "ETHUSDT" (không có dấu '-')
//	VPS dùng format "HPG", "VCB", "VNM" (mã chứng khoán thuần, HOSE)
//	Hai hệ thống có convention khác nhau → cần map tập trung để:
//	  1. Validate symbol hợp lệ trước khi query InfluxDB
//	  2. Chuẩn hóa format khi cần cross-reference (ví dụ alert từ Kafka → REST response)
//	  3. Scale thêm symbols mà không sửa rải rác trong code
//
// Phạm vi VPS hiện tại:
//
//	Chỉ hỗ trợ sàn HOSE (HNX và UPCOM không nằm trong scope).
//	Khi cần mở rộng sang HNX/UPCOM, thêm key mới vào hoseSymbols.
//
// Cách thêm symbol mới:
//   - Binance Spot:    thêm vào binanceSpotSymbols
//   - Binance Futures: thêm vào binanceFuturesSymbols
//   - VPS/HOSE:        thêm vào hoseSymbols
//
// Cách sử dụng:
//
//	if !IsValidBinanceSymbol("BTCUSDT") { ... }
//	if !IsValidHOSESymbol("VCB") { ... }
//	instruments := GetBinanceSpotSymbols()
package constants

// ─── Binance Symbol Registry ──────────────────────────────────────────────────

// binanceSpotSymbols là tập các Binance Spot instruments được hỗ trợ.
// Key = symbol theo chuẩn Binance (e.g. "BTCUSDT").
var binanceSpotSymbols = map[string]bool{
	"BTCUSDT":  true,
	"ETHUSDT":  true,
	"SOLUSDT":  true,
	"BNBUSDT":  true,
	"XRPUSDT":  true,
	"ADAUSDT":  true,
	"DOGEUSDT": true,
	"AVAXUSDT": true,
	"DOTUSDT":  true,
	"LINKUSDT": true,
}

// binanceFuturesSymbols là tập các Binance Perpetual Futures instruments.
// Dùng cho funding rate và liquidation queries.
var binanceFuturesSymbols = map[string]bool{
	"BTCUSDT":  true,
	"ETHUSDT":  true,
	"SOLUSDT":  true,
	"BNBUSDT":  true,
	"XRPUSDT":  true,
	"ADAUSDT":  true,
	"DOGEUSDT": true,
}

// IsValidBinanceSymbol kiểm tra symbol có thuộc danh sách Binance (spot hoặc futures) không.
// Trả về false nếu symbol không hợp lệ → handler trả 400 Bad Request.
func IsValidBinanceSymbol(symbol string) bool {
	return binanceSpotSymbols[symbol] || binanceFuturesSymbols[symbol]
}

// IsValidBinanceSpotSymbol kiểm tra symbol có phải Binance Spot instrument không.
func IsValidBinanceSpotSymbol(symbol string) bool {
	return binanceSpotSymbols[symbol]
}

// IsValidBinanceFuturesSymbol kiểm tra symbol có phải Binance Perpetual Futures không.
func IsValidBinanceFuturesSymbol(symbol string) bool {
	return binanceFuturesSymbols[symbol]
}

// GetBinanceSpotSymbols trả về slice tất cả Binance Spot symbols đang được hỗ trợ.
// Dùng trong batch jobs, backfill pipeline, và Spark streaming subscriptions.
func GetBinanceSpotSymbols() []string {
	symbols := make([]string, 0, len(binanceSpotSymbols))
	for s := range binanceSpotSymbols {
		symbols = append(symbols, s)
	}
	return symbols
}

// GetBinanceFuturesSymbols trả về slice tất cả Binance Futures symbols.
func GetBinanceFuturesSymbols() []string {
	symbols := make([]string, 0, len(binanceFuturesSymbols))
	for s := range binanceFuturesSymbols {
		symbols = append(symbols, s)
	}
	return symbols
}

// ─── VPS / HOSE Symbol Registry ───────────────────────────────────────────────
//
// Chỉ hỗ trợ sàn HOSE. HNX và UPCOM không nằm trong scope hiện tại.

// hoseSymbols là tập mã cổ phiếu niêm yết trên HOSE được hỗ trợ.
// Bao gồm VN30 bluechips và các mã mid-cap có thanh khoản cao.
var hoseSymbols = map[string]bool{
	// VN30 Bluechips
	"VCB": true, "BID": true, "CTG": true, "TCB": true, "MBB": true,
	"VPB": true, "HPG": true, "VNM": true, "MSN": true, "MWG": true,
	"FPT": true, "VIC": true, "VHM": true, "VRE": true, "GVR": true,
	"SAB": true, "PLX": true, "GAS": true, "POW": true, "HDB": true,
	// Mid-cap HOSE
	"ACB": true, "STB": true, "EIB": true, "LPB": true, "SHB": true,
	"NVL": true, "PDR": true, "DXG": true, "KDH": true, "BCM": true,
}

// VPS market indices của HOSE — không phải cổ phiếu thông thường, cần xử lý riêng.
var hoseIndices = map[string]bool{
	"VNINDEX": true,
	"VN30":    true,
}

// IsValidHOSESymbol kiểm tra mã có hợp lệ trên sàn HOSE không.
// Chấp nhận cả mã cổ phiếu (VCB, HPG...) lẫn market indices (VNINDEX, VN30).
func IsValidHOSESymbol(symbol string) bool {
	return hoseSymbols[symbol] || hoseIndices[symbol]
}

// IsMarketIndex kiểm tra symbol có phải HOSE market index không (VNINDEX, VN30).
func IsMarketIndex(symbol string) bool {
	return hoseIndices[symbol]
}

// GetHOSESymbols trả về slice tất cả mã cổ phiếu HOSE đang được hỗ trợ.
// Không bao gồm market indices.
func GetHOSESymbols() []string {
	result := make([]string, 0, len(hoseSymbols))
	for s := range hoseSymbols {
		result = append(result, s)
	}
	return result
}
