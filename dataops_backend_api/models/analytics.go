// Models cho Crypto (Binance) và VN Stocks (TCBS) analytics data
package models

// ─── Crypto (Binance) ─────────────────────────────────────────────

// BinanceTradeTick là 1 trade tick từ Binance WebSocket
type BinanceTradeTick struct {
	InstID    string  `json:"instId"`
	TradeID   string  `json:"tradeId"`
	Price     float64 `json:"px,string"`
	Quantity  float64 `json:"sz,string"`
	Side      string  `json:"side"` // "buy" | "sell"
	Timestamp int64   `json:"ts,string"`
}

// BinanceOrderBook5 là top 5 bid/ask levels
type BinanceOrderBook5 struct {
	InstID    string      `json:"instId"`
	Bids      [][4]string `json:"bids"` // [price, qty, _, numOrders]
	Asks      [][4]string `json:"asks"`
	Timestamp int64       `json:"ts,string"`
}

// BinanceFundingRate là funding rate của perpetual futures
type BinanceFundingRate struct {
	InstID          string  `json:"instId"`
	FundingRate     float64 `json:"fundingRate,string"`
	NextFundingRate float64 `json:"nextFundingRate,string"`
	FundingTime     int64   `json:"fundingTime,string"`
}

// VWAPPoint là kết quả VWAP tính từ Spark Streaming
type VWAPPoint struct {
	Symbol    string  `json:"symbol"`
	Interval  string  `json:"interval"` // "1m", "5m", "1h"
	VWAP      float64 `json:"vwap"`
	Volume    float64 `json:"volume"`
	Timestamp int64   `json:"ts"`
}

// ─── VN Stocks (TCBS) ────────────────────────────────────────────────────────

// TCBSMarketSnapshot là full market snapshot từ TCBS /tartarus/v1/tickerCommons.
// Chứa orderbook 3 cấp (bid/offer × 3), OHLC, foreign room.
// Data flow: TCBSPoller → tcbs.market.snapshot Kafka → InfluxDB tcbs_market measurement.
type TCBSMarketSnapshot struct {
	Symbol          string  `json:"symbol"`
	Exchange        string  `json:"exchange"`          // basket index label: "HOSE" | "VN30" | "HNX"
	RefPrice        float64 `json:"ref_price"`
	CeilPrice       float64 `json:"ceil_price"`
	FloorPrice      float64 `json:"floor_price"`
	MatchPrice      float64 `json:"match_price"`
	MatchQty        float64 `json:"match_qty"`
	// Orderbook bên mua — 3 cấp
	BidPrice01      float64 `json:"bid_price_01"`
	BidPrice02      float64 `json:"bid_price_02"`
	BidPrice03      float64 `json:"bid_price_03"`
	BidQty01        float64 `json:"bid_qty_01"`
	BidQty02        float64 `json:"bid_qty_02"`
	BidQty03        float64 `json:"bid_qty_03"`
	// Orderbook bên bán — 3 cấp
	OfferPrice01    float64 `json:"offer_price_01"`
	OfferPrice02    float64 `json:"offer_price_02"`
	OfferPrice03    float64 `json:"offer_price_03"`
	OfferQty01      float64 `json:"offer_qty_01"`
	OfferQty02      float64 `json:"offer_qty_02"`
	OfferQty03      float64 `json:"offer_qty_03"`
	// OHLC + avg
	Open            float64 `json:"open"`
	High            float64 `json:"high"`
	Low             float64 `json:"low"`
	Avg             float64 `json:"avg"`
	// Tổng giao dịch
	TotalVol        float64 `json:"total_vol"`
	TotalVal        float64 `json:"total_val"`
	// Nhà đầu tư nước ngoài
	BuyForeignQty   float64 `json:"buy_foreign_qty"`
	SellForeignQty  float64 `json:"sell_foreign_qty"`
	Room            string  `json:"room"`
	// Change
	Change          float64 `json:"change"`
	ChangePct       float64 `json:"change_pct"`
	TradingDate     string  `json:"trading_date"`
	Timestamp       int64   `json:"ts"`
}

// TCBSOrderBook là orderbook trích xuất từ TCBSMarketSnapshot — dùng cho endpoint /orderbook.
// Cung cấp 3 cấp giá mỗi chiều theo format chuẩn bid/ask.
type TCBSOrderBook struct {
	Symbol   string         `json:"symbol"`
	Exchange string         `json:"exchange"`
	Bids     []TCBSPriceLevel `json:"bids"` // 3 bước giá mua, sorted desc by price
	Asks     []TCBSPriceLevel `json:"asks"` // 3 bước giá bán, sorted asc by price
	Timestamp int64          `json:"ts"`
}

// TCBSPriceLevel là 1 bước giá trong TCBS orderbook.
type TCBSPriceLevel struct {
	Price    float64 `json:"price"`
	Quantity float64 `json:"quantity"`
}

// TCBSBSASnapshot là Buy/Sell Analysis snapshot từ TCBS /nyx/v1/intraday/{ticker}/bsa.
// Data flow: TCBSPoller → tcbs.bsa.events Kafka → InfluxDB tcbs_bsa measurement.
type TCBSBSASnapshot struct {
	Ticker       string  `json:"ticker"`
	InvestorType string  `json:"investor_type"` // "sheep" | "wolf" | "shark" | "all"
	BUP          float64 `json:"bup"`           // Buy Up Percent (0–100)
	SDP          float64 `json:"sdp"`           // Sell Down Percent (0–100)
	BSR          float64 `json:"bsr"`           // Buy/Sell Ratio (>1 = mua > bán)
	EventTime    string  `json:"event_time"`    // e.g. "09:15:00"
	Timestamp    int64   `json:"ts"`
}

// TCBSIntradayTick là 1 lệnh khớp intraday từ TCBS /nyx/v1/intraday/{ticker}/his/paging.
// Data flow: TCBSPoller → tcbs.intraday.ticks Kafka → InfluxDB tcbs_intraday measurement.
type TCBSIntradayTick struct {
	Ticker      string  `json:"ticker"`
	Price       float64 `json:"price"`
	Volume      float64 `json:"volume"`
	PriceChange float64 `json:"price_change"`  // cp: thay đổi so với tham chiếu
	Action      string  `json:"action"`        // "Buy Up" | "Sell Down"
	BuyAcc      float64 `json:"buy_acc"`       // ba: lũy kế mua chủ động
	SellAcc     float64 `json:"sell_acc"`      // sa: lũy kế bán chủ động
	Highlight   bool    `json:"highlight"`     // hl: true nếu giá trị > 200M VND
	EventTime   string  `json:"event_time"`    // t: thời gian từ sàn
	Timestamp   int64   `json:"ts"`
}

// TCBSMarketSummary là tóm tắt thị trường gồm nhiều indices HOSE.
// Dùng cho endpoint /market-summary, thay thế VPS market summary.
type TCBSMarketSummary struct {
	TradingDate string                         `json:"trading_date"`
	Indices     map[string]*TCBSMarketSnapshot `json:"indices"` // key = "VNINDEX" | "VN30" | "VN100"
}

// OHLCV là candle data — dùng chung cho cả Binance và VPS
type OHLCV struct {
	Symbol    string  `json:"symbol"`
	Interval  string  `json:"interval"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume"`
	Timestamp int64   `json:"ts"`
}

// Alert là alert từ Spark Streaming anomaly detection
type Alert struct {
	Type      string  `json:"type"`   // "funding_anomaly" | "unusual_volume" | "bsa_anomaly"
	Symbol    string  `json:"symbol"`
	Source    string  `json:"source"` // "binance" | "tcbs"
	Value     float64 `json:"value"`
	ZScore    float64 `json:"z_score"`
	Timestamp int64   `json:"ts"`
	Message   string  `json:"message"`
}

// ─── InfluxDB query result types ────────────────────────────────────────────

// OrderBookSnapshot là orderbook snapshot query result từ InfluxDB
type OrderBookSnapshot struct {
	InstID    string `json:"instId"`
	BidsJSON  string `json:"bids"` // JSON-encoded bid levels
	AsksJSON  string `json:"asks"` // JSON-encoded ask levels
	Timestamp int64  `json:"ts"`
}

// FundingRate là funding rate record từ InfluxDB
type FundingRate struct {
	InstID          string  `json:"instId"`
	Rate            float64 `json:"rate"`
	NextFundingTime string  `json:"nextFundingTime"`
	Timestamp       int64   `json:"ts"`
}

// Liquidation là liquidation event từ InfluxDB
type Liquidation struct {
	InstID    string  `json:"instId"`
	Size      float64 `json:"size"`
	Price     float64 `json:"price"`
	Side      string  `json:"side"`
	Timestamp int64   `json:"ts"`
}

// TopMover là cổ phiếu/instrument thay đổi mạnh nhất trong phiên
type TopMover struct {
	Symbol    string  `json:"symbol"`
	Exchange  string  `json:"exchange"`
	Price     float64 `json:"price"`
	ChangePct float64 `json:"changePct"`
}

// DailySummary là tóm tắt thống kê trong ngày (từ Delta Lake Silver)
type DailySummary struct {
	InstID     string  `json:"instId"`
	TradeCount int64   `json:"tradeCount"`
	PriceLow   float64 `json:"priceLow"`
	PriceHigh  float64 `json:"priceHigh"`
	PriceOpen  float64 `json:"priceOpen"`
	PriceClose float64 `json:"priceClose"`
	Volume     float64 `json:"volume"`
	BuyVolume  float64 `json:"buyVolume"`
	SellVolume float64 `json:"sellVolume"`
	VWAP       float64 `json:"vwap"`
	Date       int64   `json:"date"`
}
