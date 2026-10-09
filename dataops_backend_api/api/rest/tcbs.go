// tcbs.go — Gin HTTP handlers cho TCBS iFlash Open API analytics endpoints.
//
// Mỗi handler làm đúng 3 việc:
//  1. Parse và validate query parameters (symbol/ticker, investor_type, interval, from, to, page, size)
//  2. Gọi TCBSService để query InfluxDB (real-time) hoặc Delta Lake (historical)
//  3. Trả JSON response với format chuẩn
//
// Lưu ý: Symbol format của TCBS là mã chứng khoán thuần (e.g. "VCB", "HPG"),
// tương tự VPS nhưng data source hoàn toàn khác (TCBS iFlash API thay vì VPS SmartOne).
// Market indices (VNINDEX, VN30, VN100) cũng được hỗ trợ qua IsTCBSMarketIndex().
//
// REUSE pattern: api/rest/vnstock.go
package rest

import (
	"net/http"
	"time"

	"dataops-backend-api/constants"
	"dataops-backend-api/services/tcbs"

	"github.com/gin-gonic/gin"
)

// TCBSHandler xử lý các TCBS iFlash API endpoints.
// Tất cả routes đều yêu cầu JWT (đăng ký trong protected group).
//
// Dependencies được inject qua field TCBSSvc — dễ mock trong unit tests.
type TCBSHandler struct {
	TCBSSvc tcbs.Service
}

// Register đăng ký tất cả TCBS routes vào router group r.
//
// Routes (tất cả đều GET, JWT required):
//
//	/market          → Real-time market snapshot (giá, khối lượng, orderbook, change%) cho 1 mã
//	/orderbook       → Orderbook 3 cấp bid/ask từ TCBS
//	/ohlcv           → OHLCV lịch sử cổ phiếu (daily default)
//	/top-movers      → Top cổ phiếu tăng/giảm mạnh nhất trên sàn HOSE
//	/market-summary  → Tóm tắt tổng quan: VNINDEX, VN30, VN100
//	/bsa             → Buy/Sell Analysis (TCBS-exclusive: sheep/wolf/shark/all)
//	/intraday-ticks  → Lịch sử khớp lệnh intraday với pagination
func (h *TCBSHandler) Register(r *gin.RouterGroup) {
	r.GET("/ticker", h.GetTicker)
	r.GET("/:symbol/ticker", h.GetTickerByParam)
	r.GET("/market", h.GetMarket)
	r.GET("/orderbook", h.GetOrderBook)
	r.GET("/ohlcv", h.GetOHLCV)
	r.GET("/top-movers", h.GetTopMovers)
	r.GET("/market-summary", h.GetMarketSummary)
	r.GET("/bsa", h.GetBSA)
	r.GET("/intraday-ticks", h.GetIntradayTicks)
}

// GetTicker trả về real-time ticker data từ Redis cache
// GET /api/v1/tcbs/ticker?symbol=VCB
func (h *TCBSHandler) GetTicker(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	if !constants.IsValidTCBSSymbol(symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "unsupported TCBS symbol"})
		return
	}

	data, err := h.TCBSSvc.GetTicker(ctx, symbol)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}

// GetTickerByParam trả về ticker theo path param
// GET /api/v1/tcbs/:symbol/ticker
func (h *TCBSHandler) GetTickerByParam(ctx *gin.Context) {
	symbol := ctx.Param("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	if !constants.IsValidTCBSSymbol(symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "unsupported TCBS symbol"})
		return
	}

	data, err := h.TCBSSvc.GetTicker(ctx, symbol)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}

// GetMarket trả về real-time market snapshot của 1 mã cổ phiếu HOSE.
// Data được ghi vào InfluxDB bởi dataops-stream-producer từ TCBSPoller.
// Bao gồm đầy đủ OHLC, orderbook 3 cấp, foreign room, và change%.
//
// Query params:
//
//	symbol (required) — Mã chứng khoán HOSE, e.g. "VCB", "HPG", "VNINDEX"
//
// Response 200:
//
//	{ "symbol":"VCB", "exchange":"HOSE", "match_price":86300, "match_qty":12345,
//	  "change_pct":1.2, "total_vol":1234567, "bid_price_01":86200, ... }
//
// GET /api/v1/tcbs/market
func (h *TCBSHandler) GetMarket(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	if !constants.IsValidTCBSSymbol(symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error":   "unsupported TCBS symbol",
			"example": "VCB, HPG, TCB, VNINDEX, VN30",
		})
		return
	}

	data, err := h.TCBSSvc.GetMarketSnapshot(ctx, symbol)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}

// GetOrderBook trả về orderbook 3 cấp bid/ask của 1 mã cổ phiếu HOSE.
// Trích xuất từ market snapshot — reuse cùng InfluxDB query.
//
// Query params:
//
//	symbol (required) — Mã chứng khoán HOSE, e.g. "VCB"
//
// Response 200:
//
//	{ "symbol":"VCB", "exchange":"HOSE", "bids":[{"price":86200,"quantity":1000},...],
//	  "asks":[{"price":86400,"quantity":2000},...] }
//
// GET /api/v1/tcbs/orderbook
func (h *TCBSHandler) GetOrderBook(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	if !constants.IsValidTCBSStockSymbol(symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error":   "unsupported TCBS stock symbol",
			"hint":    "market indices (VNINDEX, VN30) do not have an order book",
			"example": "VCB, HPG, TCB",
		})
		return
	}

	data, err := h.TCBSSvc.GetOrderBook(ctx, symbol)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}

// GetOHLCV trả về OHLCV lịch sử của 1 mã cổ phiếu HOSE.
// Data được aggregate từ intraday ticks bởi Spark Silver layer rồi ghi vào InfluxDB.
//
// Query params:
//
//	symbol   (required) — Mã chứng khoán HOSE, e.g. "VCB"
//	interval (optional) — Timeframe, mặc định "1d"
//	from     (optional) — Unix timestamp (giây). Mặc định: now - 90 ngày
//	to       (optional) — Unix timestamp (giây). Mặc định: now
//
// Response 200:
//
//	{ "symbol":"VCB", "interval":"1d", "data":[{"ts","open","high","low","close","volume"}] }
//
// GET /api/v1/tcbs/ohlcv
func (h *TCBSHandler) GetOHLCV(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	if !constants.IsValidTCBSStockSymbol(symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "unsupported TCBS stock symbol"})
		return
	}

	interval := ctx.DefaultQuery("interval", constants.DefaultVNStockInterval)
	from := parseTimestamp(ctx.Query("from"), time.Now().Add(constants.DefaultTCBSOHLCVLookback).Unix())
	to := parseTimestamp(ctx.Query("to"), time.Now().Unix())

	data, err := h.TCBSSvc.GetOHLCV(ctx, symbol, interval, from, to)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{
		"symbol": symbol, "interval": interval, "data": data,
	})
}

// GetTopMovers trả về danh sách cổ phiếu tăng/giảm mạnh nhất trong phiên.
// Sắp xếp theo change_pct tuyệt đối. Chỉ hỗ trợ sàn HOSE.
//
// Query params:
//
//	limit (optional) — Số lượng kết quả. Mặc định: 10, tối đa: 50
//
// Response 200:
//
//	{ "exchange":"HOSE", "data":[{"symbol","match_price","change_pct"}] }
//
// GET /api/v1/tcbs/top-movers
func (h *TCBSHandler) GetTopMovers(ctx *gin.Context) {
	exchange := string(constants.TCBSExchangeHOSE)

	limit := int(parseTimestamp(ctx.DefaultQuery("limit", "10"), int64(constants.DefaultTopMoversLimit)))
	if limit > constants.MaxTopMoversLimit {
		limit = constants.MaxTopMoversLimit
	}
	if limit < 1 {
		limit = constants.DefaultTopMoversLimit
	}

	data, err := h.TCBSSvc.GetTopMovers(ctx, exchange, limit)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"exchange": exchange, "data": data})
}

// GetMarketSummary trả về tóm tắt tổng quan thị trường HOSE.
// Bao gồm 3 chỉ số: VNINDEX, VN30, VN100.
// Không nhận query params — luôn trả cả 3 indices cùng lúc.
//
// Response 200:
//
//	{ "trading_date":"2024-01-15", "indices":{"VNINDEX":{snapshot},"VN30":{snapshot},"VN100":{snapshot}} }
//
// Nếu 1 index không có data (ngoài giờ giao dịch), field đó vẫn xuất hiện với zero-value.
//
// GET /api/v1/tcbs/market-summary
func (h *TCBSHandler) GetMarketSummary(ctx *gin.Context) {
	data, err := h.TCBSSvc.GetMarketSummary(ctx)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}

// GetBSA trả về Buy/Sell Analysis (BSA) cho 1 mã cổ phiếu theo loại nhà đầu tư.
// TCBS-exclusive data: bup (buy up pressure), sdp (sell down pressure), bsr (buy/sell ratio).
// Đây là endpoint duy nhất không có tương đương ở VPS hay Binance.
//
// Query params:
//
//	ticker        (required) — Mã chứng khoán HOSE, e.g. "VCB"
//	investor_type (optional) — Loại NĐT: "sheep"|"wolf"|"shark"|"all". Mặc định: "all"
//
// Response 200:
//
//	{ "ticker":"VCB", "investor_type":"all", "data":[{"ts","bup","sdp","bsr","event_time"}] }
//
// GET /api/v1/tcbs/bsa
func (h *TCBSHandler) GetBSA(ctx *gin.Context) {
	ticker := ctx.Query("ticker")
	if ticker == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "ticker is required"})
		return
	}
	if !constants.IsValidTCBSStockSymbol(ticker) {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error":   "unsupported TCBS stock ticker",
			"example": "VCB, HPG, TCB",
		})
		return
	}

	investorType := ctx.DefaultQuery("investor_type", constants.DefaultTCBSInvestorType)
	if !constants.IsValidTCBSInvestorType(investorType) {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error":   "invalid investor_type",
			"allowed": []string{"sheep", "wolf", "shark", "all"},
		})
		return
	}

	data, err := h.TCBSSvc.GetBSA(ctx, ticker, investorType)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{
		"ticker": ticker, "investor_type": investorType, "data": data,
	})
}

// GetIntradayTicks trả về lịch sử khớp lệnh intraday (tick-by-tick) với pagination.
// Bao gồm price, volume, action (Buy Up/Sell Down), highlight (>200M VND).
//
// Query params:
//
//	ticker (required) — Mã chứng khoán HOSE, e.g. "VCB"
//	page   (optional) — Trang (0-based). Mặc định: 0
//	size   (optional) — Số tick mỗi trang. Mặc định: 100, tối đa: 1000
//
// Response 200:
//
//	{ "ticker":"VCB", "page":0, "size":100,
//	  "data":[{"ts","price","volume","action","buy_acc","sell_acc","highlight","event_time"}] }
//
// GET /api/v1/tcbs/intraday-ticks
func (h *TCBSHandler) GetIntradayTicks(ctx *gin.Context) {
	ticker := ctx.Query("ticker")
	if ticker == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "ticker is required"})
		return
	}
	if !constants.IsValidTCBSStockSymbol(ticker) {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error":   "unsupported TCBS stock ticker",
			"example": "VCB, HPG, TCB",
		})
		return
	}

	page := int(parseTimestamp(ctx.DefaultQuery("page", "0"), 0))
	if page < 0 {
		page = 0
	}
	size := int(parseTimestamp(ctx.DefaultQuery("size", "100"), int64(constants.DefaultTCBSIntradayPageSize)))
	maxSize := constants.MaxTCBSIntradayPages * constants.DefaultTCBSIntradayPageSize
	if size < 1 || size > maxSize {
		size = constants.DefaultTCBSIntradayPageSize
	}

	data, err := h.TCBSSvc.GetIntradayTicks(ctx, ticker, page, size)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{
		"ticker": ticker, "page": page, "size": size, "data": data,
	})
}
