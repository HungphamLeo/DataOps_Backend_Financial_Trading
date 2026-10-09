package batchdata

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// ─── Messages ─────────────────────────────────────────────────────────────────

type BinanceOHLCVRequest struct {
	Symbol   string                 `json:"symbol,omitempty"`
	InstType string                 `json:"inst_type,omitempty"`
	Interval string                 `json:"interval,omitempty"`
	From     *timestamppb.Timestamp `json:"from,omitempty"`
	To       *timestamppb.Timestamp `json:"to,omitempty"`
	Limit    int32                  `json:"limit,omitempty"`
}

func (x *BinanceOHLCVRequest) Reset()         { *x = BinanceOHLCVRequest{} }
func (x *BinanceOHLCVRequest) String() string { return fmt.Sprintf("%+v", *x) }
func (*BinanceOHLCVRequest) ProtoMessage()    {}

func (x *BinanceOHLCVRequest) GetSymbol() string {
	if x != nil {
		return x.Symbol
	}
	return ""
}

func (x *BinanceOHLCVRequest) GetInstType() string {
	if x != nil {
		return x.InstType
	}
	return ""
}

func (x *BinanceOHLCVRequest) GetInterval() string {
	if x != nil {
		return x.Interval
	}
	return ""
}

func (x *BinanceOHLCVRequest) GetFrom() *timestamppb.Timestamp {
	if x != nil {
		return x.From
	}
	return nil
}

func (x *BinanceOHLCVRequest) GetTo() *timestamppb.Timestamp {
	if x != nil {
		return x.To
	}
	return nil
}

func (x *BinanceOHLCVRequest) GetLimit() int32 {
	if x != nil {
		return x.Limit
	}
	return 0
}

type BinanceOHLCVBar struct {
	Symbol      string  `json:"symbol,omitempty"`
	InstType    string  `json:"inst_type,omitempty"`
	Interval    string  `json:"interval,omitempty"`
	TsOpenMs    int64   `json:"ts_open_ms,omitempty"`
	TsCloseMs   int64   `json:"ts_close_ms,omitempty"`
	Open        float64 `json:"open,omitempty"`
	High        float64 `json:"high,omitempty"`
	Low         float64 `json:"low,omitempty"`
	Close       float64 `json:"close,omitempty"`
	Volume      float64 `json:"volume,omitempty"`
	QuoteVolume float64 `json:"quote_volume,omitempty"`
	NumTrades   int32   `json:"num_trades,omitempty"`
}

func (x *BinanceOHLCVBar) Reset()         { *x = BinanceOHLCVBar{} }
func (x *BinanceOHLCVBar) String() string { return fmt.Sprintf("%+v", *x) }
func (*BinanceOHLCVBar) ProtoMessage()    {}

type BinanceOHLCVResponse struct {
	Symbol   string             `json:"symbol,omitempty"`
	InstType string             `json:"inst_type,omitempty"`
	Interval string             `json:"interval,omitempty"`
	Bars     []*BinanceOHLCVBar `json:"bars,omitempty"`
	Count    int32              `json:"count,omitempty"`
}

func (x *BinanceOHLCVResponse) Reset()         { *x = BinanceOHLCVResponse{} }
func (x *BinanceOHLCVResponse) String() string { return fmt.Sprintf("%+v", *x) }
func (*BinanceOHLCVResponse) ProtoMessage()    {}

type TCBSMarketEODRequest struct {
	Symbol   string `json:"symbol,omitempty"`
	Exchange string `json:"exchange,omitempty"`
	FromDate string `json:"from_date,omitempty"`
	ToDate   string `json:"to_date,omitempty"`
	Limit    int32  `json:"limit,omitempty"`
}

func (x *TCBSMarketEODRequest) Reset()         { *x = TCBSMarketEODRequest{} }
func (x *TCBSMarketEODRequest) String() string { return fmt.Sprintf("%+v", *x) }
func (*TCBSMarketEODRequest) ProtoMessage()    {}

type TCBSMarketEODBar struct {
	TradingDate    string  `json:"trading_date,omitempty"`
	Symbol         string  `json:"symbol,omitempty"`
	Exchange       string  `json:"exchange,omitempty"`
	Open           float64 `json:"open,omitempty"`
	High           float64 `json:"high,omitempty"`
	Low            float64 `json:"low,omitempty"`
	Close          float64 `json:"close,omitempty"`
	TotalVol       float64 `json:"total_vol,omitempty"`
	TotalVal       float64 `json:"total_val,omitempty"`
	BuyForeignQty  float64 `json:"buy_foreign_qty,omitempty"`
	SellForeignQty float64 `json:"sell_foreign_qty,omitempty"`
	NetForeignQty  float64 `json:"net_foreign_qty,omitempty"`
	ChangePct      float64 `json:"change_pct,omitempty"`
	RefPrice       float64 `json:"ref_price,omitempty"`
	CeilPrice      float64 `json:"ceil_price,omitempty"`
	FloorPrice     float64 `json:"floor_price,omitempty"`
}

func (x *TCBSMarketEODBar) Reset()         { *x = TCBSMarketEODBar{} }
func (x *TCBSMarketEODBar) String() string { return fmt.Sprintf("%+v", *x) }
func (*TCBSMarketEODBar) ProtoMessage()    {}

type TCBSMarketEODResponse struct {
	Symbol   string              `json:"symbol,omitempty"`
	Exchange string              `json:"exchange,omitempty"`
	Bars     []*TCBSMarketEODBar `json:"bars,omitempty"`
	Count    int32               `json:"count,omitempty"`
}

func (x *TCBSMarketEODResponse) Reset()         { *x = TCBSMarketEODResponse{} }
func (x *TCBSMarketEODResponse) String() string { return fmt.Sprintf("%+v", *x) }
func (*TCBSMarketEODResponse) ProtoMessage()    {}

type TCBSBSARequest struct {
	Ticker       string `json:"ticker,omitempty"`
	InvestorType string `json:"investor_type,omitempty"`
	FromDate     string `json:"from_date,omitempty"`
	ToDate       string `json:"to_date,omitempty"`
	Limit        int32  `json:"limit,omitempty"`
}

func (x *TCBSBSARequest) Reset()         { *x = TCBSBSARequest{} }
func (x *TCBSBSARequest) String() string { return fmt.Sprintf("%+v", *x) }
func (*TCBSBSARequest) ProtoMessage()    {}

type TCBSBSAPoint struct {
	TradingDate  string  `json:"trading_date,omitempty"`
	Ticker       string  `json:"ticker,omitempty"`
	InvestorType string  `json:"investor_type,omitempty"`
	Bup          float64 `json:"bup,omitempty"`
	Sdp          float64 `json:"sdp,omitempty"`
	Bsr          float64 `json:"bsr,omitempty"`
}

func (x *TCBSBSAPoint) Reset()         { *x = TCBSBSAPoint{} }
func (x *TCBSBSAPoint) String() string { return fmt.Sprintf("%+v", *x) }
func (*TCBSBSAPoint) ProtoMessage()    {}

type TCBSBSAResponse struct {
	Ticker       string          `json:"ticker,omitempty"`
	InvestorType string          `json:"investor_type,omitempty"`
	Points       []*TCBSBSAPoint `json:"points,omitempty"`
	Count        int32           `json:"count,omitempty"`
}

func (x *TCBSBSAResponse) Reset()         { *x = TCBSBSAResponse{} }
func (x *TCBSBSAResponse) String() string { return fmt.Sprintf("%+v", *x) }
func (*TCBSBSAResponse) ProtoMessage()    {}

type DataRangeRequest struct {
	Source       string `json:"source,omitempty"`
	Symbol       string `json:"symbol,omitempty"`
	InstType     string `json:"inst_type,omitempty"`
	InvestorType string `json:"investor_type,omitempty"`
}

func (x *DataRangeRequest) Reset()         { *x = DataRangeRequest{} }
func (x *DataRangeRequest) String() string { return fmt.Sprintf("%+v", *x) }
func (*DataRangeRequest) ProtoMessage()    {}

type DataRangeResponse struct {
	Source        string `json:"source,omitempty"`
	Symbol        string `json:"symbol,omitempty"`
	OldestDate    string `json:"oldest_date,omitempty"`
	NewestDate    string `json:"newest_date,omitempty"`
	TotalCount    int64  `json:"total_count,omitempty"`
	LastUpdatedAt string `json:"last_updated_at,omitempty"`
}

func (x *DataRangeResponse) Reset()         { *x = DataRangeResponse{} }
func (x *DataRangeResponse) String() string { return fmt.Sprintf("%+v", *x) }
func (*DataRangeResponse) ProtoMessage()    {}

// ─── gRPC Service Interface ───────────────────────────────────────────────────

type HistoricalDataServiceClient interface {
	GetBinanceOHLCV(ctx context.Context, in *BinanceOHLCVRequest, opts ...grpc.CallOption) (*BinanceOHLCVResponse, error)
	GetTCBSMarketEOD(ctx context.Context, in *TCBSMarketEODRequest, opts ...grpc.CallOption) (*TCBSMarketEODResponse, error)
	GetTCBSBSAHistory(ctx context.Context, in *TCBSBSARequest, opts ...grpc.CallOption) (*TCBSBSAResponse, error)
	GetDataRange(ctx context.Context, in *DataRangeRequest, opts ...grpc.CallOption) (*DataRangeResponse, error)
}

type historicalDataServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewHistoricalDataServiceClient(cc grpc.ClientConnInterface) HistoricalDataServiceClient {
	return &historicalDataServiceClient{cc}
}

func (c *historicalDataServiceClient) GetBinanceOHLCV(ctx context.Context, in *BinanceOHLCVRequest, opts ...grpc.CallOption) (*BinanceOHLCVResponse, error) {
	out := new(BinanceOHLCVResponse)
	err := c.cc.Invoke(ctx, "/batchdata.v1.HistoricalDataService/GetBinanceOHLCV", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *historicalDataServiceClient) GetTCBSMarketEOD(ctx context.Context, in *TCBSMarketEODRequest, opts ...grpc.CallOption) (*TCBSMarketEODResponse, error) {
	out := new(TCBSMarketEODResponse)
	err := c.cc.Invoke(ctx, "/batchdata.v1.HistoricalDataService/GetTCBSMarketEOD", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *historicalDataServiceClient) GetTCBSBSAHistory(ctx context.Context, in *TCBSBSARequest, opts ...grpc.CallOption) (*TCBSBSAResponse, error) {
	out := new(TCBSBSAResponse)
	err := c.cc.Invoke(ctx, "/batchdata.v1.HistoricalDataService/GetTCBSBSAHistory", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *historicalDataServiceClient) GetDataRange(ctx context.Context, in *DataRangeRequest, opts ...grpc.CallOption) (*DataRangeResponse, error) {
	out := new(DataRangeResponse)
	err := c.cc.Invoke(ctx, "/batchdata.v1.HistoricalDataService/GetDataRange", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Helper to convert time.Time to Protobuf Timestamp
func TimeToProtoTimestamp(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}
