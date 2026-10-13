// crypto.go — Gin HTTP handlers cho Binance crypto analytics endpoints.
package rest

import (
	"net/http"
	"strconv"
	"time"

	"dataops-backend-api/constants"
	"dataops-backend-api/models"
	"dataops-backend-api/services/analytics"
	cryptoaccount "dataops-backend-api/services/crypto_account"
	"dataops-backend-api/services/risk"

	"github.com/gin-gonic/gin"
)

// CryptoHandler xử lý các Binance analytics & order endpoints.
type CryptoHandler struct {
	AnalyticsSvc     analytics.CryptoService
	CryptoAccountSvc cryptoaccount.Service
	PreTradeSvc      risk.PreTradeService // optional: nil = không kiểm tra pre-trade
	PreTradeEnable   bool
}

// Register đăng ký tất cả crypto routes vào router group r.
func (h *CryptoHandler) Register(r *gin.RouterGroup) {
	r.GET("/ticker", h.GetTicker)
	r.GET("/:symbol/ticker", h.GetTickerByParam)
	r.GET("/vwap", h.GetVWAP)
	r.GET("/ohlcv", h.GetOHLCV)
	r.GET("/:symbol/signal", h.GetSignal)
	r.GET("/orderbook", h.GetOrderBook)
	r.GET("/funding", h.GetFundingRate)
	r.GET("/liquidations", h.GetLiquidations)

	// Crypto Order Management & Balance Proxy
	r.GET("/balance", h.GetBalance)
	r.GET("/orders", h.GetOrders)
	r.POST("/order/place", h.PlaceOrder)
	r.DELETE("/order/:id", h.CancelOrder)
}

// GetTicker trả về real-time ticker data từ Redis cache
// GET /api/v1/crypto/ticker?symbol=BTCUSDT
func (h *CryptoHandler) GetTicker(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	if !constants.IsValidBinanceSymbol(symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "unsupported symbol"})
		return
	}

	data, err := h.AnalyticsSvc.GetTicker(ctx, symbol)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}

// GetTickerByParam trả về ticker theo path param
// GET /api/v1/crypto/:symbol/ticker
func (h *CryptoHandler) GetTickerByParam(ctx *gin.Context) {
	symbol := ctx.Param("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	if !constants.IsValidBinanceSymbol(symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "unsupported symbol"})
		return
	}

	data, err := h.AnalyticsSvc.GetTicker(ctx, symbol)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}

// GetVWAP trả về VWAP data points theo symbol, interval, và time range.
//
// Query params:
//
//	symbol   (required) — Binance symbol, e.g. "BTCUSDT". Phải có trong constants.binanceSpotSymbols.
//	interval (optional) — timeframe, mặc định constants.DefaultCryptoVWAPInterval ("5m")
//	from     (optional) — Unix timestamp (giây). Mặc định: now - constants.DefaultVWAPLookback
//	to       (optional) — Unix timestamp (giây). Mặc định: now
//
// Response 200:
//
//	{ "symbol":"BTCUSDT", "interval":"5m", "from":..., "to":..., "data":[{"ts","vwap","volume"}] }
//
// GET /api/v1/crypto/vwap
func (h *CryptoHandler) GetVWAP(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	// Validate symbol hợp lệ — tránh inject tuỳ ý vào Flux query
	if !constants.IsValidBinanceSymbol(symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error":              "unsupported symbol",
			"supported_spot":     constants.GetBinanceSpotSymbols(),
			"supported_futures":  constants.GetBinanceFuturesSymbols(),
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

// GetOHLCV trả về OHLCV candles cho 1 Binance instrument.
//
// Query params:
//
//	symbol   (required) — Binance symbol, e.g. "BTCUSDT"
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
	if !constants.IsValidBinanceSymbol(symbol) {
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
// Data được đọc từ Redis cache — được cập nhật bởi data-stream-producer mỗi tick.
//
// Query params:
//
//	symbol (required) — Binance symbol, e.g. "BTCUSDT"
//
// Response 200:
//
//	{ "instId":"BTCUSDT", "bids":[[price,qty]],"asks":[[price,qty]],"ts":... }
//
// GET /api/v1/crypto/orderbook
func (h *CryptoHandler) GetOrderBook(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	if !constants.IsValidBinanceSymbol(symbol) {
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

// GetFundingRate trả về lịch sử funding rate của Binance Perpetual Futures.
// Funding rate được thu bởi data-stream-producer từ Binance WS funding rate stream.
//
// Query params:
//
//	symbol (required) — Binance Futures symbol, e.g. "BTCUSDT"
//	from   (optional) — Unix timestamp (giây). Mặc định: now - 30 ngày
//	to     (optional) — Unix timestamp (giây). Mặc định: now
//
// Response 200:
//
//	{ "symbol":"BTCUSDT", "data":[{"ts","funding_rate","next_funding_rate"}] }
//
// GET /api/v1/crypto/funding
func (h *CryptoHandler) GetFundingRate(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	// Funding rate chỉ có cho Perpetual Futures — validate riêng
	if !constants.IsValidBinanceFuturesSymbol(symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error":   "symbol must be a perpetual futures (e.g. BTCUSDT)",
			"example": "BTCUSDT",
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
//	symbol (required) — Binance symbol (spot hoặc futures)
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
	if !constants.IsValidBinanceSymbol(symbol) {
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


// GetSignal tính RSI(14) + MA(20) signal cho 1 symbol.
//
// Path param:  symbol   — e.g. "BTCUSDT"
// Query param: timeframe — default "5m"
//
// Response 200:
//
//	{ "symbol":"BTCUSDT", "signal":"BUY"|"SELL"|"HOLD"|"NEUTRAL",
//	  "confidence":0.75, "rsi14":28.5, "timeframe":"5m" }
//
// GET /api/v1/crypto/:symbol/signal
func (h *CryptoHandler) GetSignal(ctx *gin.Context) {
	symbol := ctx.Param("symbol")
	if symbol == "" || !constants.IsValidBinanceSymbol(symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "unsupported symbol"})
		return
	}
	timeframe := ctx.DefaultQuery("timeframe", constants.DefaultCryptoVWAPInterval)

	sig, err := h.AnalyticsSvc.ComputeSignal(ctx, symbol, timeframe)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"symbol":     symbol,
		"signal":     sig.SignalType,
		"confidence": sig.Confidence,
		"rsi14":      sig.Value,
		"timeframe":  sig.Timeframe,
	})
}


// GetBalance lấy số dư tài khoản Binance Spot
// GET /api/v1/crypto/balance
func (h *CryptoHandler) GetBalance(ctx *gin.Context) {
	if h.CryptoAccountSvc == nil {
		ctx.JSON(http.StatusOK, gin.H{"balances": []models.BinanceBalanceItem{}})
		return
	}
	data, err := h.CryptoAccountSvc.GetAccountBalance(ctx)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}

// GetOrders lấy danh sách lệnh Binance Spot đang mở
// GET /api/v1/crypto/orders?symbol=BTCUSDT
func (h *CryptoHandler) GetOrders(ctx *gin.Context) {
	symbol := ctx.Query("symbol")
	if h.CryptoAccountSvc == nil {
		ctx.JSON(http.StatusOK, gin.H{"orders": []models.BinanceOrderResponse{}})
		return
	}
	data, err := h.CryptoAccountSvc.GetOpenOrders(ctx, symbol)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"orders": data})
}

// PlaceOrder đặt lệnh mua / bán Binance Spot
// POST /api/v1/crypto/order/place
func (h *CryptoHandler) PlaceOrder(ctx *gin.Context) {
	var req models.BinancePlaceOrderRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body", "details": err.Error()})
		return
	}
	if !constants.IsValidBinanceSymbol(req.Symbol) {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "unsupported symbol"})
		return
	}
	if h.CryptoAccountSvc == nil {
		ctx.JSON(http.StatusServiceUnavailable, gin.H{"error": "crypto account service not available"})
		return
	}
	if !enforcePreTrade(ctx, h.PreTradeSvc, h.PreTradeEnable, &models.PreTradeCheckRequest{
		AssetType: "CRYPTO", Symbol: req.Symbol, Side: req.Side,
		Quantity: req.Quantity, Price: req.Price,
	}) {
		return
	}
	resp, err := h.CryptoAccountSvc.PlaceOrder(ctx, &req)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, resp)
}

// CancelOrder hủy lệnh Binance Spot
// DELETE /api/v1/crypto/order/:id?symbol=BTCUSDT
func (h *CryptoHandler) CancelOrder(ctx *gin.Context) {
	orderIDStr := ctx.Param("id")
	symbol := ctx.Query("symbol")
	if symbol == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}
	orderID, err := strconv.ParseInt(orderIDStr, 10, 64)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid order id"})
		return
	}
	if h.CryptoAccountSvc == nil {
		ctx.JSON(http.StatusServiceUnavailable, gin.H{"error": "crypto account service not available"})
		return
	}
	resp, err := h.CryptoAccountSvc.CancelOrder(ctx, symbol, orderID)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, resp)
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
