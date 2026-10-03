// types.go — Go structs mapping TCBS iFlash Open API response payloads
//
// Mỗi struct map 1:1 với JSON response từ TCBS API.
// Fields được đặt tên theo JSON key trong spec openapi-v1.0.0.json,
// với Go field names theo CamelCase convention.
//
// Reference: openapi-v1.0.0.json — TCBS iFlash Open API v1.0.0

package tcbsclient

// ─── Auth ────────────────────────────────────────────────────────────────────

// TokenRequest là body gửi lên /gaia/v1/oauth2/openapi/token
// Cả hai fields đều required theo spec.
type TokenRequest struct {
	APIKey string `json:"apiKey"` // API Key từ TCInvest app
	OTP    string `json:"otp"`    // Smart OTP (iOTP) 6 chữ số — TTL ngắn (~30s), dùng 1 lần
}

// TokenResponse là response từ /gaia/v1/oauth2/openapi/token
type TokenResponse struct {
	Token string `json:"token"` // JWT token — dùng làm Bearer header cho mọi request
}

// ─── Market Data — tickerCommons ─────────────────────────────────────────────

// TickerCommonsResponse là response từ /tartarus/v1/tickerCommons
// Chứa bảng giá đầy đủ với orderbook 3 cấp, OHLC, foreign room.
type TickerCommonsResponse struct {
	TradingDate string          `json:"tradingDate"` // dd/MM/yyyy
	Data        []TickerCommonItem `json:"data"`
}

// TickerCommonItem là dữ liệu thị trường đầy đủ cho 1 mã cổ phiếu.
// Field names từ spec: bidPrice01, bidQtty01, offerPrice01, offerQtty01...
type TickerCommonItem struct {
	Symbol      string  `json:"symbol"`
	CeilPrice   float64 `json:"ceilPrice"`
	FloorPrice  float64 `json:"floorPrice"`
	RefPrice    float64 `json:"refPrice"`

	// Orderbook bên mua — 3 cấp giá tốt nhất
	BidPrice01 float64 `json:"bidPrice01"`
	BidPrice02 float64 `json:"bidPrice02"`
	BidPrice03 float64 `json:"bidPrice03"`
	BidQtty01  float64 `json:"bidQtty01"`
	BidQtty02  float64 `json:"bidQtty02"`
	BidQtty03  float64 `json:"bidQtty03"`

	// Orderbook bên bán — 3 cấp giá tốt nhất
	OfferPrice01 float64 `json:"offerPrice01"`
	OfferPrice02 float64 `json:"offerPrice02"`
	OfferPrice03 float64 `json:"offerPrice03"`
	OfferQtty01  float64 `json:"offerQtty01"`
	OfferQtty02  float64 `json:"offerQtty02"`
	OfferQtty03  float64 `json:"offerQtty03"`

	// Khớp lệnh + biến động
	MatchPrice    float64 `json:"matchPrice"`
	MatchQtty     float64 `json:"matchQtty"`
	Change        float64 `json:"change"`
	ChangePercent float64 `json:"changePercent"`

	// OHLC trong phiên
	Open float64 `json:"open"`
	High float64 `json:"high"`
	Low  float64 `json:"low"`
	Avg  float64 `json:"avg"`

	// Tổng giao dịch
	TotalVol float64 `json:"totalVol"`
	TotalVal float64 `json:"totalVal"`

	// Nhà đầu tư nước ngoài
	BuyForeignQtty  float64 `json:"buyForeignQtty"`
	SellForeignQtty float64 `json:"sellForeignQtty"`
	Room            string  `json:"room"`

	IndexNumber float64 `json:"indexNumber"`
}

// ─── Market Data — tickerSnaps ────────────────────────────────────────────────

// TickerSnapsResponse là response từ /tartarus/v1/tickerSnaps
// Lightweight snapshot: foreign room + basic price info.
type TickerSnapsResponse struct {
	TradingDate string           `json:"tradingDate"`
	Data        []TickerSnapItem `json:"data"`
}

// TickerSnapItem là dữ liệu snapshot nhẹ, tập trung vào foreign room.
type TickerSnapItem struct {
	Symbol          string  `json:"symbol"`
	RefPrice        float64 `json:"refPrice"`
	CeilPrice       float64 `json:"ceilPrice"`
	FloorPrice      float64 `json:"floorPrice"`
	MatchPrice      float64 `json:"matchPrice"`
	MatchQtty       float64 `json:"matchQtty"`
	Change          float64 `json:"change"`
	ChangePercent   float64 `json:"changePercent"`
	TotalVolume     float64 `json:"totalVolume"`
	TotalValue      float64 `json:"totalValue"`
	BuyForeignQtty  float64 `json:"buyForeignQtty"`
	SellForeignQtty float64 `json:"sellForeignQtty"`
	Room            string  `json:"room"`
}

// ─── Intraday Analytics — BSA ─────────────────────────────────────────────────

// BSAResponse là response từ /nyx/v1/intraday/{ticker}/bsa
// Buy/Sell Analysis phân theo loại nhà đầu tư.
type BSAResponse struct {
	Ticker string    `json:"ticker"`
	Data   []BSAItem `json:"data"`
}

// BSAItem là 1 record BSA theo thời gian.
// bup + sdp <= 100 (phần trăm), bsr >= 0 (tỷ lệ)
type BSAItem struct {
	BuyUpPercent   float64 `json:"bup"` // % giao dịch mua chủ động (buy up)
	SellDownPercent float64 `json:"sdp"` // % giao dịch bán chủ động (sell down)
	BuySellRatio   float64 `json:"bsr"` // tỷ lệ mua/bán
	Time           string  `json:"t"`   // thời gian record, e.g. "09:15:00"
}

// ─── Intraday Analytics — History Paging ─────────────────────────────────────

// IntradayHistoryResponse là response từ /nyx/v1/intraday/{ticker}/his/paging
// Tick-by-tick lịch sử khớp lệnh trong ngày.
type IntradayHistoryResponse struct {
	Page          float64          `json:"page"`
	Size          float64          `json:"size"`
	HeadIndex     float64          `json:"headIndex"`
	NumberOfItems float64          `json:"numberOfItems"`
	Total         float64          `json:"total"`
	Ticker        string           `json:"ticker"`
	Data          []IntradayTickItem `json:"data"`
}

// IntradayTickItem là 1 lệnh khớp trong lịch sử intraday.
type IntradayTickItem struct {
	Price       float64 `json:"p"`  // Giá khớp lệnh
	Volume      float64 `json:"v"`  // Khối lượng khớp
	PriceChange float64 `json:"cp"` // Thay đổi giá so với tham chiếu
	Action      string  `json:"a"`  // "Buy Up" | "Sell Down"
	BuyAcc      float64 `json:"ba"` // Lũy kế mua chủ động
	SellAcc     float64 `json:"sa"` // Lũy kế bán chủ động
	Highlight   bool    `json:"hl"` // true nếu giá trị > 200M VND
	Time        string  `json:"t"`  // Thời gian khớp lệnh từ sàn
}

// ─── Derivatives ─────────────────────────────────────────────────────────────

// DerivativesResponse là response từ /tartarus/v1/derivatives
type DerivativesResponse struct {
	Data []DerivativeItem `json:"data"`
}

// DerivativeItem là thông tin 1 hợp đồng phái sinh (VN30F).
type DerivativeItem struct {
	Symbol        string  `json:"symbol"`        // e.g. "VN30F2405"
	CeilPrice     float64 `json:"ceilPrice"`
	FloorPrice    float64 `json:"floorPrice"`
	RefPrice      float64 `json:"refPrice"`
	BidPrice01    float64 `json:"bidPrice01"`
	BidQtty01     float64 `json:"bidQtty01"`
	OfferPrice01  float64 `json:"offerPrice01"`
	OfferQtty01   float64 `json:"offerQtty01"`
	MatchPrice    float64 `json:"matchPrice"`
	MatchQtty     float64 `json:"matchQtty"`
	Change        float64 `json:"change"`
	ChangePercent float64 `json:"changePercent"`
	TotalVolume   float64 `json:"totalVolume"`
}
