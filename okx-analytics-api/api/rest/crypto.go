// Analytics API Handlers — REUSE pattern: market-maker-api/api/authentication.go
// Crypto: VWAP, OHLCV, OrderBook, FundingRate, Liquidation endpoints
package rest

import (
	"net/http"
	"okx-analytics-api/services/analytics"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

type CryptoHandler struct {
	AnalyticsSvc analytics.CryptoService
}

func (h *CryptoHandler) Register(r *gin.RouterGroup) {
	r.GET("/vwap", h.GetVWAP)
	r.GET("/ohlcv", h.GetOHLCV)
	r.GET("/orderbook", h.GetOrderBook)
	r.GET("/funding", h.GetFundingRate)
	r.GET("/liquidations", h.GetLiquidations)
}

// GetVWAP trả về VWAP theo symbol + interval + time range
// GET /api/v1/crypto/vwap?symbol=BTC-USDT&interval=5m&from=1712000000&to=1712086400
func (h *CryptoHandler) GetVWAP(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}

	interval := ctx.DefaultQuery("interval", "5m")
	from := parseTimestamp(ctx.Query("from"), time.Now().Add(-24*time.Hour).Unix())
	to := parseTimestamp(ctx.Query("to"), time.Now().Unix())

	data, err := h.AnalyticsSvc.GetVWAP(ctx, symbol, interval, from, to)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"symbol":   symbol,
		"interval": interval,
		"from":     from,
		"to":       to,
		"data":     data,
	})
}

// GetOHLCV trả về OHLCV candles
// GET /api/v1/crypto/ohlcv?symbol=BTC-USDT&interval=1h&from=...&to=...
func (h *CryptoHandler) GetOHLCV(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}

	interval := ctx.DefaultQuery("interval", "1h")
	from := parseTimestamp(ctx.Query("from"), time.Now().Add(-7*24*time.Hour).Unix())
	to := parseTimestamp(ctx.Query("to"), time.Now().Unix())

	data, err := h.AnalyticsSvc.GetOHLCV(ctx, symbol, interval, from, to)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"symbol": symbol, "interval": interval,
		"from": from, "to": to, "data": data,
	})
}

// GetOrderBook trả về real-time order book snapshot
// GET /api/v1/crypto/orderbook?symbol=BTC-USDT
func (h *CryptoHandler) GetOrderBook(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}

	data, err := h.AnalyticsSvc.GetOrderBook(ctx, symbol)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}

// GetFundingRate trả về funding rate history
// GET /api/v1/crypto/funding?symbol=BTC-USDT-SWAP&from=...&to=...
func (h *CryptoHandler) GetFundingRate(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	from := parseTimestamp(ctx.Query("from"), time.Now().Add(-30*24*time.Hour).Unix())
	to := parseTimestamp(ctx.Query("to"), time.Now().Unix())

	data, err := h.AnalyticsSvc.GetFundingRate(ctx, symbol, from, to)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"symbol": symbol, "data": data})
}

// GetLiquidations trả về liquidation events
// GET /api/v1/crypto/liquidations?symbol=BTC-USDT-SWAP&from=...&to=...
func (h *CryptoHandler) GetLiquidations(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	from := parseTimestamp(ctx.Query("from"), time.Now().Add(-7*24*time.Hour).Unix())
	to := parseTimestamp(ctx.Query("to"), time.Now().Unix())

	data, err := h.AnalyticsSvc.GetLiquidations(ctx, symbol, from, to)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"symbol": symbol, "data": data})
}

// parseTimestamp parse string → int64 Unix timestamp
func parseTimestamp(s string, fallback int64) int64 {
	if s == "" {
		return fallback
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fallback
	}
	return v
}
