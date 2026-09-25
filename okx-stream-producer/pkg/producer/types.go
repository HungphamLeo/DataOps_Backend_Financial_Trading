// LEARN: Types cho Kafka Producer messages
// Đây là Go structs mapping 1:1 với Avro schemas trong pkg/schemas/
//
// Convention: structs dùng json tags để Marshal/Unmarshal
// Avro field names (snake_case) → Go field names (CamelCase) qua json tags

package producer

// TradeEvent maps với okx_trade_event.avsc
// LEARN: struct tags `json:"..."` — khi json.Marshal chuyển field name sang key trong JSON
type TradeEvent struct {
	InstID      string  `json:"instId"`
	TradeID     string  `json:"tradeId"`
	Price       float64 `json:"price"`
	Size        float64 `json:"size"`
	Side        string  `json:"side"`        // "buy" | "sell"
	TsMs        int64   `json:"tsMs"`        // Unix milliseconds từ OKX
	ProducedMs  int64   `json:"producedAtMs"` // khi producer gửi lên Kafka
	InstType    string  `json:"instType"`    // "SPOT" | "SWAP" | "FUTURES"
}

// PriceLevel là 1 level trong orderbook (bid hoặc ask)
type PriceLevel struct {
	Price     float64 `json:"price"`
	Quantity  float64 `json:"quantity"`
	NumOrders int     `json:"numOrders"`
}

// OrderBookEvent maps với okx_orderbook_snapshot.avsc
type OrderBookEvent struct {
	InstID     string       `json:"instId"`
	TsMs       int64        `json:"tsMs"`
	ProducedMs int64        `json:"producedAtMs"`
	Bids       []PriceLevel `json:"bids"` // Top 5 bids, sorted desc by price
	Asks       []PriceLevel `json:"asks"` // Top 5 asks, sorted asc by price
	Checksum   *int32       `json:"checksum,omitempty"` // pointer = nullable
}

// LiquidationEvent maps với okx_liquidation_event.avsc
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

// VPSSnapshotEvent maps với vps_market_snapshot.avsc
// LEARN: Tất cả giá VPS tính bằng nghìn VND (VND thousands)
//        VNM = 85.3 nghĩa là 85,300 VND / cổ phiếu
type VPSSnapshotEvent struct {
	Symbol         string  `json:"symbol"`
	Exchange       string  `json:"exchange"` // "HOSE" | "HNX" | "UPCOM"
	Price          float64 `json:"price"`
	Bid            float64 `json:"bid"`
	Ask            float64 `json:"ask"`
	Open           float64 `json:"open"`
	High           float64 `json:"high"`
	Low            float64 `json:"low"`
	ReferencePrice float64 `json:"referencePrice"`
	CeilingPrice   float64 `json:"ceilingPrice"`
	FloorPrice     float64 `json:"floorPrice"`
	Volume         float64 `json:"volume"`
	TotalValue     float64 `json:"totalValue"`
	ChangePct      float64 `json:"changePct"`
	SessionState   string  `json:"sessionState"` // "ATO"|"CONTINUOUS"|"ATC"|"BREAK"|"CLOSED"
	TsMs           int64   `json:"tsMs"`
	ProducedMs     int64   `json:"producedAtMs"`
}
