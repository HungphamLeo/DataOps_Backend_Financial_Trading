// LEARN: Types cho Kafka Producer messages
// Đây là Go structs mapping 1:1 với Avro schemas trong pkg/schemas/
//
// Convention: structs dùng json tags để Marshal/Unmarshal
// Avro field names (snake_case) → Go field names (CamelCase) qua json tags

package producer

// TradeEvent maps với binance_trade_event.avsc
// LEARN: struct tags `json:"..."` — khi json.Marshal chuyển field name sang key trong JSON
type TradeEvent struct {
	InstID      string  `json:"instId"`
	TradeID     string  `json:"tradeId"`
	Price       float64 `json:"price"`
	Size        float64 `json:"size"`
	Side        string  `json:"side"`        // "buy" | "sell"
	TsMs        int64   `json:"tsMs"`        // Unix milliseconds từ Binance
	ProducedMs  int64   `json:"producedAtMs"` // khi producer gửi lên Kafka
	InstType    string  `json:"instType"`    // "SPOT" | "SWAP" | "FUTURES"
}

// PriceLevel là 1 level trong orderbook (bid hoặc ask)
type PriceLevel struct {
	Price     float64 `json:"price"`
	Quantity  float64 `json:"quantity"`
	NumOrders int     `json:"numOrders"`
}

// OrderBookEvent maps với binance_orderbook_snapshot.avsc
type OrderBookEvent struct {
	InstID     string       `json:"instId"`
	TsMs       int64        `json:"tsMs"`
	ProducedMs int64        `json:"producedAtMs"`
	Bids       []PriceLevel `json:"bids"` // Top 5 bids, sorted desc by price
	Asks       []PriceLevel `json:"asks"` // Top 5 asks, sorted asc by price
	Checksum   *int32       `json:"checksum,omitempty"` // pointer = nullable
}

// LiquidationEvent maps với binance_liquidation_event.avsc
type LiquidationEvent struct {
	InstID     string  `json:"instId"`
	InstType   string  `json:"instType"`
	TotalLoss  float64 `json:"totalLoss"`
	BkLoss     float64 `json:"bkLoss"`
	Side       string  `json:"side"`
	Size       float64 `json:"size"`
	Price      float64 `json:"price"`
	TsMs       int64   `json:"tsMs"`
	ProducedMs int64   `json:"producedAtMs"`
}

// ─── TCBS Events ──────────────────────────────────────────────────────────────

// TCBSMarketEvent maps với tcbs_market_snapshot.avsc
// Full ticker snapshot từ TCBS /tartarus/v1/tickerCommons — orderbook 3 cấp, OHLC, foreign room.
type TCBSMarketEvent struct {
	Symbol          string  `json:"symbol"`
	Exchange        string  `json:"exchange"`       // index basket label: "HOSE" | "VN30" | "HNX"
	RefPrice        float64 `json:"refPrice"`
	CeilPrice       float64 `json:"ceilPrice"`
	FloorPrice      float64 `json:"floorPrice"`
	MatchPrice      float64 `json:"matchPrice"`
	MatchQty        float64 `json:"matchQty"`
	BidPrice01      float64 `json:"bidPrice01"`
	BidPrice02      float64 `json:"bidPrice02"`
	BidPrice03      float64 `json:"bidPrice03"`
	BidQty01        float64 `json:"bidQty01"`
	BidQty02        float64 `json:"bidQty02"`
	BidQty03        float64 `json:"bidQty03"`
	OfferPrice01    float64 `json:"offerPrice01"`
	OfferPrice02    float64 `json:"offerPrice02"`
	OfferPrice03    float64 `json:"offerPrice03"`
	OfferQty01      float64 `json:"offerQty01"`
	OfferQty02      float64 `json:"offerQty02"`
	OfferQty03      float64 `json:"offerQty03"`
	Open            float64 `json:"open"`
	High            float64 `json:"high"`
	Low             float64 `json:"low"`
	Avg             float64 `json:"avg"`
	TotalVol        float64 `json:"totalVol"`
	TotalVal        float64 `json:"totalVal"`
	BuyForeignQty   float64 `json:"buyForeignQty"`
	SellForeignQty  float64 `json:"sellForeignQty"`
	// Room: foreign room — API trả number (float64), không phải string như spec ghi.
	Room            float64 `json:"room"`
	Change          float64 `json:"change"`
	ChangePct       float64 `json:"changePct"`
	TradingDate     string  `json:"tradingDate"`    // dd/MM/yyyy từ API
	TsMs            int64   `json:"tsMs"`
	ProducedMs      int64   `json:"producedAtMs"`
}

// TCBSBSAEvent maps với tcbs_bsa_event.avsc
// Buy/Sell Analysis từ TCBS /nyx/v1/intraday/{ticker}/bsa.
type TCBSBSAEvent struct {
	Ticker       string  `json:"ticker"`
	InvestorType string  `json:"investorType"` // "sheep" | "wolf" | "shark" | "all"
	BUP          float64 `json:"bup"`          // Buy Up Percent (0–100)
	SDP          float64 `json:"sdp"`          // Sell Down Percent (0–100)
	BSR          float64 `json:"bsr"`          // Buy/Sell Ratio
	EventTime    string  `json:"eventTime"`    // e.g. "09:15:00"
	TsMs         int64   `json:"tsMs"`
	ProducedMs   int64   `json:"producedAtMs"`
}

// TCBSIntradayTickEvent maps với tcbs_intraday_tick_event.avsc
// Tick-by-tick khớp lệnh từ TCBS /nyx/v1/intraday/{ticker}/his/paging.
type TCBSIntradayTickEvent struct {
	Ticker      string  `json:"ticker"`
	Price       float64 `json:"price"`
	Volume      float64 `json:"volume"`
	PriceChange float64 `json:"priceChange"`
	Action      string  `json:"action"`    // "Buy Up" | "Sell Down"
	BuyAcc      float64 `json:"buyAcc"`
	SellAcc     float64 `json:"sellAcc"`
	Highlight   bool    `json:"highlight"` // true nếu giá trị > 200M VND
	EventTime   string  `json:"eventTime"`
	TsMs        int64   `json:"tsMs"`
	ProducedMs  int64   `json:"producedAtMs"`
}

// TCBSDerivativeEvent là real-time price update từ TCBS WSS /api/v1/derivatives/contracts.
// Fields mirror DerivativeItem từ /tartarus/v1/derivatives REST endpoint —
// TCBS WSS gửi cùng cấu trúc JSON qua WebSocket push.
type TCBSDerivativeEvent struct {
	Symbol        string  `json:"symbol"`        // e.g. "VN30F2410"
	CeilPrice     float64 `json:"ceilPrice"`
	FloorPrice    float64 `json:"floorPrice"`
	RefPrice      float64 `json:"refPrice"`
	MatchPrice    float64 `json:"matchPrice"`
	MatchQty      float64 `json:"matchQty"`
	BidPrice01    float64 `json:"bidPrice01"`
	BidQty01      float64 `json:"bidQty01"`
	AskPrice01    float64 `json:"askPrice01"`    // offerPrice01 alias cho derivatives
	AskQty01      float64 `json:"askQty01"`
	Change        float64 `json:"change"`
	ChangePct     float64 `json:"changePct"`
	TotalVol      float64 `json:"totalVol"`
	TsMs          int64   `json:"tsMs"`
	ProducedMs    int64   `json:"producedAtMs"`
}
