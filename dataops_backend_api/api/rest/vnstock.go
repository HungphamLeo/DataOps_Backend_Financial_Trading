// vnstock.go — Gin HTTP handlers cho VPS Securities (VN Stock) analytics endpoints.
//
// Mỗi handler làm đúng 3 việc:
//  1. Parse và validate query parameters (symbol, exchange, interval, from, to)
//  2. Gọi VNStockService để query InfluxDB (real-time) hoặc Delta Lake (historical)
//  3. Trả JSON response với format chuẩn
//
// Tất cả magic numbers được lấy từ constants/defaults.go.
// Symbol và exchange validation dùng constants/symbols.go.
//
// Lưu ý: Symbol format của VPS là mã chứng khoán thuần (e.g. "VCB", "HPG"),
// KHÔNG có dạng "VCB-VND" như một số hệ thống khác.
// Market indices (VNINDEX, VN30) cũng được hỗ trợ qua IsMarketIndex().
//
// REUSE pattern: market-maker-api/api/authentication.go
package rest

import (
	"net/http"
	"okx-analytics-api/constants"
	"okx-analytics-api/services/vnstock"
	"time"

	"github.com/gin-gonic/gin"
)

// VNStockHandler xử lý các VPS Securities endpoints.
// Tất cả routes đều yêu cầu JWT (đăng ký trong protected group).
//
// Dependencies được inject qua field VNStockSvc — dễ mock trong unit tests.
type VNStockHandler struct {
	VNStockSvc vnstock.Service
}

// Register đăng ký tất cả VN stock routes vào router group r.
//
// Routes (tất cả đều GET, JWT required):
//
//	/market          → Real-time market snapshot (giá, khối lượng, change%) cho 1 mã
//	/ohlcv           → OHLCV lịch sử cổ phiếu VN (daily default)
//	/orderbook       → Bid/ask book từ VPS (3 bước giá mỗi chiều)
//	/top-movers      → Top cổ phiếu tăng/giảm mạnh nhất trên 1 sàn
//	/market-summary  → Tóm tắt tổng quan: VNINDEX, VN30 (HOSE only)
func (h *VNStockHandler) Register(r *gin.RouterGroup) {
	r.GET("/market", h.GetMarket)
	r.GET("/ohlcv", h.GetOHLCV)
	r.GET("/orderbook", h.GetOrderBook)
	r.GET("/top-movers", h.GetTopMovers)
	r.GET("/market-summary", h.GetMarketSummary)
}

// GetMarket trả về real-time market snapshot của 1 mã cổ phiếu VN.
// Data được ghi vào InfluxDB bởi okx-stream-producer từ VPS REST poller.
//
// Query params:
//
//	symbol (required) — Mã chứng khoán VPS, e.g. "VCB", "HPG", "VNINDEX"
//
// Response 200:
//
//	{ "symbol":"VCB", "exchange":"HOSE", "price":86300, "volume":1234567, "change_pct":1.2, "ts":... }
//
// GET /api/v1/vn-stocks/market
func (h *VNStockHandler) GetMarket(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	// Chấp nhận cả mã cổ phiếu và market indices HOSE (VNINDEX, VN30)
	if !constants.IsValidHOSESymbol(symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error":   "unsupported HOSE symbol",
			"example": "VCB, HPG, VNINDEX, VN30",
		})
		return
	}

	data, err := h.VNStockSvc.GetMarketSnapshot(ctx, symbol)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}

// GetOHLCV trả về OHLCV lịch sử của 1 mã cổ phiếu VN.
// VPS không có WebSocket → data được accumulate từ periodic REST polling.
//
// Query params:
//
//	symbol   (required) — Mã chứng khoán VPS, e.g. "VCB"
//	interval (optional) — Timeframe, mặc định constants.DefaultVNStockInterval ("1d")
//	from     (optional) — Unix timestamp (giây). Mặc định: now - 90 ngày (1 quý)
//	to       (optional) — Unix timestamp (giây). Mặc định: now
//
// Response 200:
//
//	{ "symbol":"VCB", "interval":"1d", "data":[{"ts","open","high","low","close","volume"}] }
//
// GET /api/v1/vn-stocks/ohlcv
func (h *VNStockHandler) GetOHLCV(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	if !constants.IsValidHOSESymbol(symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "unsupported HOSE symbol"})
		return
	}

	interval := ctx.DefaultQuery("interval", constants.DefaultVNStockInterval)
	from := parseTimestamp(ctx.Query("from"), time.Now().Add(constants.DefaultVNStockOHLCVLookback).Unix())
	to := parseTimestamp(ctx.Query("to"), time.Now().Unix())

	data, err := h.VNStockSvc.GetOHLCV(ctx, symbol, interval, from, to)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{
		"symbol": symbol, "interval": interval, "data": data,
	})
}

// GetOrderBook trả về order book (bảng giá) của 1 mã từ VPS.
// VPS cung cấp 3 bước giá mỗi chiều (bid/ask).
//
// Query params:
//
//	symbol (required) — Mã chứng khoán VPS, e.g. "VCB"
//
// Response 200:
//
//	{ "symbol":"VCB", "bids":[[price,qty,...]],"asks":[[price,qty,...]] }
//
// GET /api/v1/vn-stocks/orderbook
func (h *VNStockHandler) GetOrderBook(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	if !constants.IsValidHOSESymbol(symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "unsupported HOSE symbol"})
		return
	}

	data, err := h.VNStockSvc.GetOrderBook(ctx, symbol)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}

// GetTopMovers trả về danh sách cổ phiếu tăng/giảm mạnh nhất trong phiên.
// Sắp xếp theo change_pct tuyệt đối (cả tăng lẫn giảm mạnh đều xuất hiện).
// Chỉ hỗ trợ sàn HOSE.
//
// Query params:
//
//	limit (optional) — Số lượng kết quả. Mặc định: constants.DefaultTopMoversLimit (10), tối đa: constants.MaxTopMoversLimit (50)
//
// Response 200:
//
//	{ "exchange":"HOSE", "data":[{"symbol","price","change_pct"}] }
//
// GET /api/v1/vn-stocks/top-movers
func (h *VNStockHandler) GetTopMovers(ctx *gin.Context) {
	// Chỉ HOSE — exchange không cần validate vì không nhận input
	exchange := constants.DefaultVNExchange

	// Parse limit với giới hạn tối đa để tránh response quá lớn
	limit := int(parseTimestamp(ctx.DefaultQuery("limit", "10"), int64(constants.DefaultTopMoversLimit)))
	if limit > constants.MaxTopMoversLimit {
		limit = constants.MaxTopMoversLimit
	}
	if limit < 1 {
		limit = constants.DefaultTopMoversLimit
	}

	data, err := h.VNStockSvc.GetTopMovers(ctx, exchange, limit)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"exchange": exchange, "data": data})
}

// GetMarketSummary trả về tóm tắt tổng quan thị trường HOSE.
// Bao gồm 2 chỉ số HOSE: VNINDEX (tổng hợp), VN30 (bluechips).
// Không nhận query params — luôn trả cả 2 indices cùng lúc.
//
// Response 200:
//
//	{ "VNINDEX":{snapshot}, "VN30":{snapshot} }
//
// Nếu 1 index không có data (ngoài giờ giao dịch), field tương ứng vẫn xuất hiện
// nhưng các giá trị sẽ là zero-value.
//
// GET /api/v1/vn-stocks/market-summary
func (h *VNStockHandler) GetMarketSummary(ctx *gin.Context) {
	data, err := h.VNStockSvc.GetMarketSummary(ctx)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}
