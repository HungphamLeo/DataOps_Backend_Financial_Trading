// symbols.go — Symbol registry và validation cho OKX và VPS Securities.
//
// Vấn đề cần giải quyết:
//
//	OKX dùng format "BTC-USDT", "ETH-USDT-SWAP"
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
//   - OKX Spot:    thêm vào okxSpotSymbols
//   - OKX Swap:    thêm vào okxSwapSymbols
//   - VPS/HOSE:    thêm vào hoseSymbols
//
// Cách sử dụng:
//
//	if !IsValidOKXSymbol("BTC-USDT") { ... }
//	if !IsValidHOSESymbol("VCB") { ... }
//	instruments := GetOKXSpotSymbols()
package constants

// ─── OKX Symbol Registry ──────────────────────────────────────────────────────

// okxSpotSymbols là tập các OKX Spot instruments được hỗ trợ.
// Key = instId theo chuẩn OKX (e.g. "BTC-USDT").
var okxSpotSymbols = map[string]bool{
	"BTC-USDT":  true,
	"ETH-USDT":  true,
	"SOL-USDT":  true,
	"BNB-USDT":  true,
	"XRP-USDT":  true,
	"ADA-USDT":  true,
	"DOGE-USDT": true,
	"AVAX-USDT": true,
	"DOT-USDT":  true,
	"LINK-USDT": true,
}

// okxSwapSymbols là tập các OKX Perpetual Swap instruments.
// Dùng cho funding rate và liquidation queries.
var okxSwapSymbols = map[string]bool{
	"BTC-USDT-SWAP":  true,
	"ETH-USDT-SWAP":  true,
	"SOL-USDT-SWAP":  true,
	"BNB-USDT-SWAP":  true,
	"XRP-USDT-SWAP":  true,
	"ADA-USDT-SWAP":  true,
	"DOGE-USDT-SWAP": true,
}

// IsValidOKXSymbol kiểm tra symbol có thuộc danh sách OKX (spot hoặc swap) không.
// Trả về false nếu symbol không hợp lệ → handler trả 400 Bad Request.
func IsValidOKXSymbol(symbol string) bool {
	return okxSpotSymbols[symbol] || okxSwapSymbols[symbol]
}

// IsValidOKXSpotSymbol kiểm tra symbol có phải OKX Spot instrument không.
func IsValidOKXSpotSymbol(symbol string) bool {
	return okxSpotSymbols[symbol]
}

// IsValidOKXSwapSymbol kiểm tra symbol có phải OKX Perpetual Swap không.
func IsValidOKXSwapSymbol(symbol string) bool {
	return okxSwapSymbols[symbol]
}

// GetOKXSpotSymbols trả về slice tất cả OKX Spot symbols đang được hỗ trợ.
// Dùng trong batch jobs, backfill pipeline, và Spark streaming subscriptions.
func GetOKXSpotSymbols() []string {
	symbols := make([]string, 0, len(okxSpotSymbols))
	for s := range okxSpotSymbols {
		symbols = append(symbols, s)
	}
	return symbols
}

// GetOKXSwapSymbols trả về slice tất cả OKX Swap symbols.
func GetOKXSwapSymbols() []string {
	symbols := make([]string, 0, len(okxSwapSymbols))
	for s := range okxSwapSymbols {
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
