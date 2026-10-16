package models

// ─── Binance Crypto Order & Account Models ────────────────────────────────────

// BinancePlaceOrderRequest là request body để đặt lệnh Binance Spot
type BinancePlaceOrderRequest struct {
	Symbol      string  `json:"symbol" binding:"required"`
	Side        string  `json:"side" binding:"required"` // "BUY", "SELL"
	Type        string  `json:"type" binding:"required"` // "LIMIT", "MARKET"
	TimeInForce string  `json:"time_in_force,omitempty"` // "GTC", "IOC", "FOK"
	Quantity    float64 `json:"quantity" binding:"required"`
	Price       float64 `json:"price,omitempty"`
}

// BinanceOrderResponse là response khi đặt / hủy / tra cứu lệnh Binance
type BinanceOrderResponse struct {
	Symbol        string  `json:"symbol"`
	OrderID       int64   `json:"order_id"`
	ClientOrderID string  `json:"client_order_id,omitempty"`
	TransactTime  int64   `json:"transact_time"`
	Price         float64 `json:"price"`
	OrigQty       float64 `json:"orig_qty"`
	ExecutedQty   float64 `json:"executed_qty"`
	Status        string  `json:"status"` // "NEW", "FILLED", "CANCELED", "REJECTED"
	Type          string  `json:"type"`
	Side          string  `json:"side"`
	Message       string  `json:"message,omitempty"`
}

// BinanceCancelOrderResponse là response khi hủy lệnh Binance
type BinanceCancelOrderResponse struct {
	Symbol  string `json:"symbol"`
	OrderID int64  `json:"order_id"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// BinanceBalanceItem là số dư từng đồng coin trong tài khoản
type BinanceBalanceItem struct {
	Asset  string  `json:"asset"`
	Free   float64 `json:"free"`
	Locked float64 `json:"locked"`
	Total  float64 `json:"total"`
}

// BinanceAccountBalanceResponse là danh sách số dư tài khoản Binance
type BinanceAccountBalanceResponse struct {
	Balances  []BinanceBalanceItem `json:"balances"`
	UpdatedAt int64                `json:"updated_at"`
}
