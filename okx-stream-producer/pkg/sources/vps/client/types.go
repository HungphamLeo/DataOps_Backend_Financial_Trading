// REUSE: ~90% từ trading-bot-finance-service/pkg/sources/vps/types.go
// LEARN: Go struct tags — `json:"fieldName"` map Go field ↔ JSON key
//        Khi JSON field là "p1" nhưng Go field là Username → dùng json tag
//        Khi field optional → thêm `json:",omitempty"` (bỏ qua nếu zero value)

package vpsclient

// ─── Login ───────────────────────────────────────────────────────────────────

// LoginRequest là JSON body gửi lên VPS login endpoint
// LEARN: Nested struct trong Go — struct chứa pointer đến struct khác (*LoginDataRequest)
//        Dùng pointer để nil check, không allocate nếu không cần
type LoginRequest struct {
	Group string            `json:"group"`
	Data  *LoginDataRequest `json:"data"`
}

type LoginDataRequest struct {
	Type     string  `json:"type"`
	Cmd      VPS_CMD `json:"cmd"`
	Username string  `json:"p1"` // VPS API dùng p1, p2 thay vì username, password
	Password string  `json:"p2"`
	P3       string  `json:"p3,omitempty"`
	P4       string  `json:"p4,omitempty"`
}

type LoginResponse struct {
	Cmd  VPS_CMD           `json:"cmd"`
	OID  string            `json:"oID"`
	RC   VPS_STATUS_RC     `json:"rc"`
	RS   string            `json:"rs"`
	Data LoginDataResponse `json:"data"`
}

type LoginDataResponse struct {
	User           string `json:"user"`
	Name           string `json:"name"`
	Sid            string `json:"sid"`           // Session ID — dùng cho mọi request sau
	DefaultAcc     string `json:"defaultAcc"`
	IFlag          int    `json:"iFlag"`
	CountLoginFail int    `json:"CountLoginFail"`
	AuthenType     string `json:"AuthenType"`
}

// ─── Stock Data (Market Snapshots) ───────────────────────────────────────────

// StockDataRequest lấy realtime market data cho danh sách symbols
type StockDataRequest struct {
	Group   string               `json:"group"`
	Session string               `json:"session"` // VPS session ID từ Login
	Data    *StockDataRequestData `json:"data"`
}

type StockDataRequestData struct {
	Type    string   `json:"type"`
	Cmd     VPS_CMD  `json:"cmd"`
	Symbols []string `json:"listShareCode"` // e.g. ["VNM","HPG","VIC"]
}

type StockDataListResponse struct {
	RC   VPS_STATUS_RC      `json:"rc"`
	RS   string             `json:"rs"`
	Data []*StockDataResponse `json:"data"`
}

// StockDataResponse chứa market data cho 1 cổ phiếu
// LEARN: Tất cả giá VPS tính bằng nghìn VND (float)
//        85.3 = 85,300 VND/cổ phiếu
type StockDataResponse struct {
	StockCode    string  `json:"stockCode"`    // Mã CK: "VNM", "HPG"
	Exchange     string  `json:"exchange"`     // "HOSE","HNX","UPCOM"
	MatchPrice   float64 `json:"matchPrice"`   // Giá khớp lệnh gần nhất (×1000 VND)
	MatchVol     float64 `json:"matchVolume"`  // Khối lượng khớp lệnh
	OpenPrice    float64 `json:"openPrice"`    // Giá mở cửa phiên
	HighPrice    float64 `json:"highPrice"`    // Giá cao nhất phiên
	LowPrice     float64 `json:"lowPrice"`     // Giá thấp nhất phiên
	CeilPrice    float64 `json:"ceilPrice"`    // Giá trần (+7% HOSE, +10% HNX)
	FloorPrice   float64 `json:"floorPrice"`   // Giá sàn (-7% HOSE, -10% HNX)
	RefPrice     float64 `json:"referencePrice"` // Giá tham chiếu (đóng cửa hôm qua)
	BidPrice1    float64 `json:"bidPrice1"`    // Giá mua 1 (best bid)
	BidVol1      float64 `json:"bidVolume1"`   // Khối lượng mua 1
	BidPrice2    float64 `json:"bidPrice2"`
	BidVol2      float64 `json:"bidVolume2"`
	BidPrice3    float64 `json:"bidPrice3"`
	BidVol3      float64 `json:"bidVolume3"`
	AskPrice1    float64 `json:"askPrice1"`    // Giá bán 1 (best ask)
	AskVol1      float64 `json:"askVolume1"`
	AskPrice2    float64 `json:"askPrice2"`
	AskVol2      float64 `json:"askVolume2"`
	AskPrice3    float64 `json:"askPrice3"`
	AskVol3      float64 `json:"askVolume3"`
	TotalTradingVol float64 `json:"totalTradingVolume"` // Tổng KL khớp cả phiên
	TotalTradingVal float64 `json:"totalTradingValue"`  // Tổng giá trị khớp cả phiên (VND)
	ChangePct    float64 `json:"changePercent"`  // % thay đổi so với giá tham chiếu
	Session      string  `json:"currentSession"` // "ATO","CONTINUOUS","ATC","BREAK","CLOSED"
	ForeignBuy   float64 `json:"foreignBuyVolume"`  // Khối lượng mua nước ngoài
	ForeignSell  float64 `json:"foreignSellVolume"` // Khối lượng bán nước ngoài
	Room         float64 `json:"foreignCurrentRoom"` // Room nước ngoài còn lại
}

// ─── Stock List ───────────────────────────────────────────────────────────────

type StockListRequest struct {
	Group   string               `json:"group"`
	Session string               `json:"session"`
	Data    *StockListRequestData `json:"data"`
}

type StockListRequestData struct {
	Type     string  `json:"type"`
	Cmd      VPS_CMD `json:"cmd"`
	Exchange string  `json:"exchange"` // "HOSE","HNX","UPCOM"
}

type StockListResponse struct {
	RC   VPS_STATUS_RC  `json:"rc"`
	RS   string         `json:"rs"`
	Data []*StockListItem `json:"data"`
}

// StockListItem thông tin cơ bản của 1 cổ phiếu
type StockListItem struct {
	StockCode  string `json:"stockCode"`
	StockName  string `json:"stockName"`
	Exchange   string `json:"exchange"`
	Industry   string `json:"industry"`
	MarketCap  float64 `json:"marketCap"`
	ListedVol  float64 `json:"listedVolume"`
}
