// VN Stocks Handler — REUSE pattern: market-maker-api/api/authentication.go
// VPS Securities: market snapshot, OHLCV, order book endpoints
package rest

import (
	"net/http"
	"okx-analytics-api/services/vnstock"
	"time"

	"github.com/gin-gonic/gin"
)

type VNStockHandler struct {
	VNStockSvc vnstock.Service
}

func (h *VNStockHandler) Register(r *gin.RouterGroup) {
	r.GET("/market", h.GetMarket)
	r.GET("/ohlcv", h.GetOHLCV)
	r.GET("/orderbook", h.GetOrderBook)
	r.GET("/top-movers", h.GetTopMovers)
	r.GET("/market-summary", h.GetMarketSummary)
}

// GetMarket trả về real-time market data từ VPS
// GET /api/v1/vn-stocks/market?symbol=VCB
func (h *VNStockHandler) GetMarket(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}

	data, err := h.VNStockSvc.GetMarketSnapshot(ctx, symbol)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}

// GetOHLCV trả về lịch sử OHLCV cổ phiếu VN
// GET /api/v1/vn-stocks/ohlcv?symbol=VCB&interval=1d&from=...&to=...
func (h *VNStockHandler) GetOHLCV(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}

	interval := ctx.DefaultQuery("interval", "1d")
	from := parseTimestamp(ctx.Query("from"), time.Now().Add(-90*24*time.Hour).Unix())
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

// GetOrderBook trả về bid/ask book từ VPS
// GET /api/v1/vn-stocks/orderbook?symbol=VCB
func (h *VNStockHandler) GetOrderBook(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}

	data, err := h.VNStockSvc.GetOrderBook(ctx, symbol)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}

// GetTopMovers trả về top gainers/losers
// GET /api/v1/vn-stocks/top-movers?exchange=HOSE&limit=10
func (h *VNStockHandler) GetTopMovers(ctx *gin.Context) {
	exchange := ctx.DefaultQuery("exchange", "HOSE")
	limit := parseTimestamp(ctx.DefaultQuery("limit", "10"), 10)

	data, err := h.VNStockSvc.GetTopMovers(ctx, exchange, int(limit))
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"exchange": exchange, "data": data})
}

// GetMarketSummary trả về tóm tắt thị trường VN: VN30, VNINDEX, HNXINDEX
// GET /api/v1/vn-stocks/market-summary
func (h *VNStockHandler) GetMarketSummary(ctx *gin.Context) {
	data, err := h.VNStockSvc.GetMarketSummary(ctx)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}
