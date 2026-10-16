package models

// ─── TCBS Account & Portfolio Models ──────────────────────────────────────────

// TCBSAccountCash là số dư tiền mặt và sức mua tài khoản TCBS
// Endpoint: GET /aion/v1/accounts/{accountNo}/cashInvestments
type TCBSAccountCash struct {
	AccountNo        string  `json:"account_no"`
	Cash             float64 `json:"cash"`
	PurchasingPower  float64 `json:"purchasing_power"`
	DepositAmt       float64 `json:"deposit_amt"`
	WithdrawableAmt  float64 `json:"withdrawable_amt"`
	MarginRatio      float64 `json:"margin_ratio"`
	Nav              float64 `json:"nav"`
	UpdatedAt        int64   `json:"updated_at"`
}

// TCBSStockAsset là tài sản cổ phiếu nắm giữ
// Endpoint: GET /aion/v1/accounts/{accountNo}/se
type TCBSStockAsset struct {
	AccountNo       string  `json:"account_no"`
	Symbol          string  `json:"symbol"`
	Quantity        float64 `json:"quantity"`
	AvailableQty    float64 `json:"available_qty"`
	CostPrice       float64 `json:"cost_price"`
	MarketPrice     float64 `json:"market_price"`
	MarketValue     float64 `json:"market_value"`
	UnrealizedPnL   float64 `json:"unrealized_pnl"`
	UnrealizedPnLPct float64 `json:"unrealized_pnl_pct"`
}

// TCBSOrderRecord là thông tin lệnh cổ phiếu
// Endpoint: GET /aion/v1/accounts/{accountNo}/orders
type TCBSOrderRecord struct {
	OrderNo      string  `json:"order_no"`
	AccountNo    string  `json:"account_no"`
	Symbol       string  `json:"symbol"`
	Side         string  `json:"side"` // "NB" (Buy), "NS" (Sell)
	OrderType    string  `json:"order_type"` // "LO", "MP", "ATO", "ATC"
	Price        float64 `json:"price"`
	Quantity     float64 `json:"quantity"`
	MatchedQty   float64 `json:"matched_qty"`
	MatchedPrice float64 `json:"matched_price"`
	Status       string  `json:"status"`
	OrderTime    string  `json:"order_time"`
}

// TCBSPlaceStockOrderRequest là request body đặt lệnh cổ phiếu cơ sở
// Endpoint: POST /akhlys/v1/accounts/{accountNo}/orders
type TCBSPlaceStockOrderRequest struct {
	AccountNo string  `json:"account_no" binding:"required"`
	Symbol    string  `json:"symbol" binding:"required"`
	Side      string  `json:"side" binding:"required"` // "NB" | "NS"
	OrderType string  `json:"order_type" binding:"required"` // "LO", "MP", "ATO", "ATC"
	Price     float64 `json:"price" binding:"required"`
	Quantity  float64 `json:"quantity" binding:"required"`
	Pin       string  `json:"pin,omitempty"`
}

// TCBSPlaceStockOrderResponse là kết quả trả về khi đặt lệnh cổ phiếu
type TCBSPlaceStockOrderResponse struct {
	OrderNo   string `json:"order_no"`
	Status    string `json:"status"`
	ErrorCode string `json:"error_code,omitempty"`
	Message   string `json:"message,omitempty"`
}

<<<<<<< HEAD
=======
// TCBSCancelStockOrderResponse là kết quả hủy lệnh cổ phiếu cơ sở
type TCBSCancelStockOrderResponse struct {
	OrderNo   string `json:"order_no"`
	Status    string `json:"status"`
	ErrorCode string `json:"error_code,omitempty"`
	Message   string `json:"message,omitempty"`
}

// TCBSModifyStockOrderRequest là request body sửa lệnh cổ phiếu
type TCBSModifyStockOrderRequest struct {
	AccountNo string  `json:"account_no" binding:"required"`
	OrderNo   string  `json:"order_no" binding:"required"`
	Price     float64 `json:"price" binding:"required"`
	Quantity  float64 `json:"quantity" binding:"required"`
	Pin       string  `json:"pin,omitempty"`
}

// TCBSModifyStockOrderResponse là kết quả sửa lệnh cổ phiếu
type TCBSModifyStockOrderResponse struct {
	OrderNo   string `json:"order_no"`
	Status    string `json:"status"`
	ErrorCode string `json:"error_code,omitempty"`
	Message   string `json:"message,omitempty"`
}

>>>>>>> 20261010_data_processing
// TCBSDerivativePositionRecord là vị thế phái sinh đang mở
// Endpoint: GET /khronos/v1/account/portfolio/status
type TCBSDerivativePositionRecord struct {
	AccountID    string  `json:"account_id"`
	SubAccountID string  `json:"sub_account_id"`
	Symbol       string  `json:"symbol"`
	Side         string  `json:"side"` // "L" (Long), "S" (Short)
	NetQty       float64 `json:"net_qty"`
	AvgPrice     float64 `json:"avg_price"`
	MarketPrice  float64 `json:"market_price"`
	FloatingPnL  float64 `json:"floating_pnl"`
	TradingDate  string  `json:"trading_date"`
}

// TCBSPlaceDerivOrderRequest là request body đặt lệnh phái sinh VN30F
// Endpoint: POST /khronos/v1/order/place
type TCBSPlaceDerivOrderRequest struct {
	AccountNo string  `json:"account_no" binding:"required"`
	Symbol    string  `json:"symbol" binding:"required"` // e.g. "VN30F2405"
	Side      string  `json:"side" binding:"required"`   // "B" (Buy/Long), "S" (Sell/Short)
	OrderType string  `json:"order_type" binding:"required"` // "LO", "MTL", "MAK", "MOK"
	Price     float64 `json:"price" binding:"required"`
	Quantity  float64 `json:"quantity" binding:"required"`
}

// TCBSPlaceDerivOrderResponse là kết quả đặt lệnh phái sinh
type TCBSPlaceDerivOrderResponse struct {
	RC        string `json:"rc"`
	OrderNo   string `json:"order_no,omitempty"`
	PKOrderNo string `json:"pk_order_no,omitempty"`
	Message   string `json:"message,omitempty"`
}
<<<<<<< HEAD
=======

// TCBSCancelDerivOrderResponse là kết quả hủy lệnh phái sinh
type TCBSCancelDerivOrderResponse struct {
	RC      string `json:"rc"`
	OrderNo string `json:"order_no,omitempty"`
	Message string `json:"message,omitempty"`
}
>>>>>>> 20261010_data_processing
