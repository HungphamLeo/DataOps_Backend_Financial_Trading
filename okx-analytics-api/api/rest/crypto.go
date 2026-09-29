// crypto.go — Gin HTTP handlers cho OKX crypto analytics endpoints.
//
// Mỗi handler trong file này làm đúng 3 việc:
//  1. Parse và validate query parameters (symbol, interval, from, to)
//  2. Gọi CryptoService để query InfluxDB / Delta Lake
//  3. Trả JSON response với format chuẩn
//
// Tất cả magic numbers (time ranges, defaults) được lấy từ constants/defaults.go.
// Symbol validation dùng constants/symbols.go — không hardcode string trong handler.
//
// REUSE pattern: market-maker-api/api/authentication.go
package rest

import (
	"net/http"
	"okx-analytics-api/constants"
	"okx-analytics-api/services/analytics"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// CryptoHandler xử lý các OKX analytics endpoints.
// Tất cả routes đều yêu cầu JWT (đăng ký trong protected group).
//
// Dependencies được inject qua field AnalyticsSvc — dễ mock trong unit tests.
type CryptoHandler struct {
	AnalyticsSvc analytics.CryptoService
}

// Register đăng ký tất cả crypto routes vào router group r.
//
// Routes (tất cả đều GET, JWT required):
//
//	/vwap         → Volume Weighted Average Price theo symbol + interval + time range
//	/ohlcv        → OHLCV candles từ InfluxDB (real-time) hoặc Delta Lake (historical)
//	/orderbook    → Real-time order book snapshot (top 5 bid/ask levels)
//	/funding      → Funding rate history cho perpetual swaps
//	/liquidations → Liquidation events (forced close) trong time range
func (h *CryptoHandler) Register(r *gin.RouterGroup) {
	r.GET("/vwap", h.GetVWAP)
	r.GET("/ohlcv", h.GetOHLCV)
	r.GET("/orderbook", h.GetOrderBook)
	r.GET("/funding", h.GetFundingRate)
	r.GET("/liquidations", h.GetLiquidations)
}

// GetVWAP trả về VWAP data points theo symbol, interval, và time range.
//
// Query params:
//
//	symbol   (required) — OKX instId, e.g. "BTC-USDT". Phải có trong constants.okxSpotSymbols.
//	interval (optional) — timeframe, mặc định constants.DefaultCryptoVWAPInterval ("5m")
//	from     (optional) — Unix timestamp (giây). Mặc định: now - constants.DefaultVWAPLookback
//	to       (optional) — Unix timestamp (giây). Mặc định: now
//
// Response 200:
//
//	{ "symbol":"BTC-USDT", "interval":"5m", "from":..., "to":..., "data":[{"ts","vwap","volume"}] }
//
// GET /api/v1/crypto/vwap
func (h *CryptoHandler) GetVWAP(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	// Validate symbol hợp lệ — tránh inject tuỳ ý vào Flux query
	if !constants.IsValidOKXSymbol(symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error":            "unsupported symbol",
			"supported_spot":   constants.GetOKXSpotSymbols(),
			"supported_swap":   constants.GetOKXSwapSymbols(),
		})
		return
	}

	interval := ctx.DefaultQuery("interval", constants.DefaultCryptoVWAPInterval)
	from := parseTimestamp(ctx.Query("from"), time.Now().Add(constants.DefaultVWAPLookback).Unix())
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

// GetOHLCV trả về OHLCV candles cho 1 OKX instrument.
//
// Query params:
//
//	symbol   (required) — OKX instId, e.g. "BTC-USDT"
//	interval (optional) — timeframe, mặc định constants.DefaultCryptoOHLCVInterval ("1h")
//	from     (optional) — Unix timestamp (giây). Mặc định: now - constants.DefaultOHLCVLookback (7 ngày)
//	to       (optional) — Unix timestamp (giây). Mặc định: now
//
// Response 200:
//
//	{ "symbol":"BTC-USDT", "interval":"1h", "from":..., "to":..., "data":[{ohlcv fields}] }
//
// GET /api/v1/crypto/ohlcv
func (h *CryptoHandler) GetOHLCV(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	if !constants.IsValidOKXSymbol(symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "unsupported symbol"})
		return
	}

	interval := ctx.DefaultQuery("interval", constants.DefaultCryptoOHLCVInterval)
	from := parseTimestamp(ctx.Query("from"), time.Now().Add(constants.DefaultOHLCVLookback).Unix())
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

// GetOrderBook trả về real-time order book snapshot (top 5 bid/ask levels).
// Data được đọc từ Redis cache — được cập nhật bởi okx-stream-producer mỗi tick.
//
// Query params:
//
//	symbol (required) — OKX instId, e.g. "BTC-USDT"
//
// Response 200:
//
//	{ "instId":"BTC-USDT", "bids":[[price,qty]],"asks":[[price,qty]],"ts":... }
//
// GET /api/v1/crypto/orderbook
func (h *CryptoHandler) GetOrderBook(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	if !constants.IsValidOKXSymbol(symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "unsupported symbol"})
		return
	}

	data, err := h.AnalyticsSvc.GetOrderBook(ctx, symbol)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}

// GetFundingRate trả về lịch sử funding rate của OKX Perpetual Swap.
// Funding rate được thu bởi okx-stream-producer từ channel "funding-rate" của OKX WS.
//
// Query params:
//
//	symbol (required) — OKX Swap instId, e.g. "BTC-USDT-SWAP"
//	from   (optional) — Unix timestamp (giây). Mặc định: now - 30 ngày
//	to     (optional) — Unix timestamp (giây). Mặc định: now
//
// Response 200:
//
//	{ "symbol":"BTC-USDT-SWAP", "data":[{"ts","funding_rate","next_funding_rate"}] }
//
// GET /api/v1/crypto/funding
func (h *CryptoHandler) GetFundingRate(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	// Funding rate chỉ có cho Perpetual Swap — validate riêng
	if !constants.IsValidOKXSwapSymbol(symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error":   "symbol must be a perpetual swap (e.g. BTC-USDT-SWAP)",
			"example": "BTC-USDT-SWAP",
		})
		return
	}

	from := parseTimestamp(ctx.Query("from"), time.Now().Add(constants.DefaultFundingLookback).Unix())
	to := parseTimestamp(ctx.Query("to"), time.Now().Unix())

	data, err := h.AnalyticsSvc.GetFundingRate(ctx, symbol, from, to)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"symbol": symbol, "data": data})
}

// GetLiquidations trả về các sự kiện thanh lý bắt buộc (forced liquidation).
// Liquidation event xảy ra khi margin của position không đủ → hữu ích cho
// phân tích market stress và cascade risk.
//
// Query params:
//
//	symbol (required) — OKX instId (spot hoặc swap)
//	from   (optional) — Unix timestamp (giây). Mặc định: now - 7 ngày
//	to     (optional) — Unix timestamp (giây). Mặc định: now
//
// Response 200:
//
//	{ "symbol":"BTC-USDT-SWAP", "data":[{"ts","side","price","size","value_usd"}] }
//
// GET /api/v1/crypto/liquidations
func (h *CryptoHandler) GetLiquidations(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	if !constants.IsValidOKXSymbol(symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "unsupported symbol"})
		return
	}

	from := parseTimestamp(ctx.Query("from"), time.Now().Add(constants.DefaultLiquidationLookback).Unix())
	to := parseTimestamp(ctx.Query("to"), time.Now().Unix())

	data, err := h.AnalyticsSvc.GetLiquidations(ctx, symbol, from, to)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"symbol": symbol, "data": data})
}

// parseTimestamp parse string query param → int64 Unix timestamp (giây).
// Trả về fallback nếu string rỗng hoặc không parse được.
// Dùng chung cho cả crypto và VN stock handlers.
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
