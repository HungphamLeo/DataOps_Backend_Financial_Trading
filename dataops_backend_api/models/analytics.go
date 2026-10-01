// Models cho Crypto và VN Stocks analytics data
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

// ─── VN Stocks (VPS) ────────────────────────────────────────────

// VPSMarketSnapshot là snapshot giá cổ phiếu từ VPS
type VPSMarketSnapshot struct {
	Symbol     string  `json:"symbol"`
	Exchange   string  `json:"exchange"`    // "HOSE" | "HNX" | "UPCOM"
	Price      float64 `json:"price"`
	Volume     float64 `json:"volume"`
	MatchedVol float64 `json:"matched_vol"`
	High       float64 `json:"high"`
	Low        float64 `json:"low"`
	Open       float64 `json:"open"`
	Change     float64 `json:"change"`
	ChangePct  float64 `json:"change_pct"`
	Timestamp  int64   `json:"ts"`
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
	Type      string  `json:"type"`   // "funding_anomaly" | "unusual_volume"
	Symbol    string  `json:"symbol"`
	Source    string  `json:"source"` // "binance" | "vps"
	Value     float64 `json:"value"`
	ZScore    float64 `json:"z_score"`
	Timestamp int64   `json:"ts"`
	Message   string  `json:"message"`
}
