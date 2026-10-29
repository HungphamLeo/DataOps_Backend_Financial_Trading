// Package batch — gRPC client của Backend API gọi sang dataops_batch_api (dữ liệu lịch sử).
//
// # ST16 — Connection Multiplexing (trạng thái HIỆN TẠI)
//
// grpc.ClientConn chạy trên HTTP/2: nhiều RPC đồng thời được multiplex trên MỘT TCP connection
// (mỗi RPC là một stream), và ClientConn an toàn để dùng đồng thời từ nhiều goroutine.
// Vì vậy batchClient giữ đúng một conn dùng chung cho mọi request HTTP — KHÔNG tạo conn mới
// theo request. Tạo conn mỗi request sẽ tốn TCP + HTTP/2 handshake và cạn socket / ephemeral
// port khi tải cao. Giới hạn cần nhớ: số stream đồng thời trên một conn bị chặn bởi
// MAX_CONCURRENT_STREAMS của server (thường 100); vượt ngưỡng thì RPC xếp hàng chờ.
//
// # Timeout và Fail-fast
//
//   - Dial: chờ tối đa 3s (WithBlock + context timeout) lúc khởi động. Quá hạn → không lỗi cứng,
//     trả về client ở "fallback mode" (conn=nil) để Backend API vẫn khởi động được khi
//     Batch API chưa sẵn sàng; IsAvailable() báo false, các method trả lỗi ngay (fail-fast).
//   - Từng RPC: client KHÔNG tự đặt deadline, deadline lấy từ ctx của caller. Caller phải truyền
//     ctx có timeout (ví dụ c.Request.Context() kèm WithTimeout), nếu không một Batch API chậm
//     sẽ giữ goroutine và request HTTP vô hạn.
//   - Sau khi vào fallback mode, client KHÔNG tự thử kết nối lại; cần restart process hoặc tạo
//     client mới. Đây là điểm yếu cần cải thiện nếu Batch API khởi động chậm hơn Backend API.
//
// # Retry, Backoff + Jitter, Thundering Herd (CHƯA triển khai)
//
// Hiện không có retry. Nếu thêm retry, KHÔNG dùng khoảng chờ cố định: khi Batch API restart,
// mọi instance Backend API retry cùng nhịp → Thundering Herd làm server vừa dậy bị quá tải
// lại. Dùng exponential backoff có jitter, ví dụ delay = rand(0, min(cap, base*2^attempt)),
// kết hợp giới hạn số lần thử và chỉ retry mã lỗi idempotent/tạm thời (Unavailable,
// DeadlineExceeded). Các RPC ở đây đều là read-only nên retry an toàn; gRPC cũng hỗ trợ khai
// báo retryPolicy trong service config thay vì tự viết vòng lặp.
//
// # Bảo mật
//
// Dùng insecure.NewCredentials() (plaintext) — chấp nhận được trong mạng nội bộ Docker
// Compose, nhưng cần TLS/mTLS khi ra khỏi network tin cậy (Zero-Trust, xem ST07).
package batch

import (
	"context"
	"fmt"
	"time"

	"dataops-backend-api/logging"
	"dataops-backend-api/models"
	batchpb "dataops-backend-api/repository/batch/batchdata"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client định nghĩa interface cho tầng repository tương tác với dataops_batch_api qua gRPC
type Client interface {
	GetBinanceOHLCV(ctx context.Context, symbol, instType, interval string, from, to time.Time, limit int32) ([]*models.OHLCV, error)
	GetTCBSMarketEOD(ctx context.Context, symbol, exchange, fromDate, toDate string, limit int32) ([]*models.OHLCV, error)
	GetTCBSBSAHistory(ctx context.Context, ticker, investorType, fromDate, toDate string, limit int32) ([]*models.TCBSBSASnapshot, error)
	GetDataRange(ctx context.Context, source, symbol, instType, investorType string) (*batchpb.DataRangeResponse, error)
	Close() error
	IsAvailable() bool
}

type batchClient struct {
	conn   *grpc.ClientConn
	grpc   batchpb.HistoricalDataServiceClient
	logger *logging.Logger
	target string
}

// NewClient tạo kết nối gRPC Client tới Batch API server.
//
// Không bao giờ trả error: dial thất bại chỉ log Warn rồi trả client ở fallback mode
// (xem package doc). target rỗng mặc định localhost:50051. Caller nên kiểm tra IsAvailable()
// và gọi Close() khi shutdown để giải phóng connection.
func NewClient(target string, logger *logging.Logger) (Client, error) {
	if target == "" {
		target = "localhost:50051"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		if logger != nil {
			logger.Warn("BatchGRPCClient.Connect", "gRPC connection to Batch API failed, fallback mode enabled",
				map[string]interface{}{"target": target, "err": err.Error()})
		}
		// Vẫn trả về batchClient nhưng conn = nil để cho phép graceful fallback
		return &batchClient{
			conn:   nil,
			grpc:   nil,
			logger: logger,
			target: target,
		}, nil
	}

	if logger != nil {
		logger.Info("BatchGRPCClient.Connect", "Connected to Batch API gRPC server",
			map[string]interface{}{"target": target})
	}

	return &batchClient{
		conn:   conn,
		grpc:   batchpb.NewHistoricalDataServiceClient(conn),
		logger: logger,
		target: target,
	}, nil
}

// IsAvailable cho biết conn gRPC có được thiết lập hay không (false khi ở fallback mode).
// Chỉ phản ánh trạng thái lúc dial, không health-check server theo thời gian thực.
func (c *batchClient) IsAvailable() bool {
	return c != nil && c.conn != nil && c.grpc != nil
}

func (c *batchClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *batchClient) GetBinanceOHLCV(ctx context.Context, symbol, instType, interval string, from, to time.Time, limit int32) ([]*models.OHLCV, error) {
	if !c.IsAvailable() {
		return nil, fmt.Errorf("batch gRPC client not connected to %s", c.target)
	}

	req := &batchpb.BinanceOHLCVRequest{
		Symbol:   symbol,
		InstType: instType,
		Interval: interval,
		Limit:    limit,
	}
	if !from.IsZero() {
		req.From = batchpb.TimeToProtoTimestamp(from)
	}
	if !to.IsZero() {
		req.To = batchpb.TimeToProtoTimestamp(to)
	}

	res, err := c.grpc.GetBinanceOHLCV(ctx, req)
	if err != nil {
		if c.logger != nil {
			c.logger.Error("BatchGRPC.GetBinanceOHLCV", "gRPC call failed",
				map[string]interface{}{"symbol": symbol, "interval": interval, "err": err.Error()})
		}
		return nil, fmt.Errorf("gRPC GetBinanceOHLCV: %w", err)
	}

	var candles []*models.OHLCV
	for _, bar := range res.Bars {
		candles = append(candles, &models.OHLCV{
			Symbol:    bar.Symbol,
			Interval:  bar.Interval,
			Open:      bar.Open,
			High:      bar.High,
			Low:       bar.Low,
			Close:     bar.Close,
			Volume:    bar.Volume,
			Timestamp: bar.TsOpenMs / 1000,
		})
	}

	return candles, nil
}

func (c *batchClient) GetTCBSMarketEOD(ctx context.Context, symbol, exchange, fromDate, toDate string, limit int32) ([]*models.OHLCV, error) {
	if !c.IsAvailable() {
		return nil, fmt.Errorf("batch gRPC client not connected to %s", c.target)
	}

	req := &batchpb.TCBSMarketEODRequest{
		Symbol:   symbol,
		Exchange: exchange,
		FromDate: fromDate,
		ToDate:   toDate,
		Limit:    limit,
	}

	res, err := c.grpc.GetTCBSMarketEOD(ctx, req)
	if err != nil {
		if c.logger != nil {
			c.logger.Error("BatchGRPC.GetTCBSMarketEOD", "gRPC call failed",
				map[string]interface{}{"symbol": symbol, "exchange": exchange, "err": err.Error()})
		}
		return nil, fmt.Errorf("gRPC GetTCBSMarketEOD: %w", err)
	}

	var candles []*models.OHLCV
	for _, bar := range res.Bars {
		// parse trading_date "YYYY-MM-DD" → timestamp
		var ts int64
		if t, parseErr := time.Parse("2006-01-02", bar.TradingDate); parseErr == nil {
			ts = t.Unix()
		}

		candles = append(candles, &models.OHLCV{
			Symbol:    bar.Symbol,
			Interval:  "1d",
			Open:      bar.Open,
			High:      bar.High,
			Low:       bar.Low,
			Close:     bar.Close,
			Volume:    bar.TotalVol,
			Timestamp: ts,
		})
	}

	return candles, nil
}

func (c *batchClient) GetTCBSBSAHistory(ctx context.Context, ticker, investorType, fromDate, toDate string, limit int32) ([]*models.TCBSBSASnapshot, error) {
	if !c.IsAvailable() {
		return nil, fmt.Errorf("batch gRPC client not connected to %s", c.target)
	}

	req := &batchpb.TCBSBSARequest{
		Ticker:       ticker,
		InvestorType: investorType,
		FromDate:     fromDate,
		ToDate:       toDate,
		Limit:        limit,
	}

	res, err := c.grpc.GetTCBSBSAHistory(ctx, req)
	if err != nil {
		if c.logger != nil {
			c.logger.Error("BatchGRPC.GetTCBSBSAHistory", "gRPC call failed",
				map[string]interface{}{"ticker": ticker, "investorType": investorType, "err": err.Error()})
		}
		return nil, fmt.Errorf("gRPC GetTCBSBSAHistory: %w", err)
	}

	var bsaList []*models.TCBSBSASnapshot
	for _, p := range res.Points {
		var ts int64
		if t, parseErr := time.Parse("2006-01-02", p.TradingDate); parseErr == nil {
			ts = t.Unix()
		}

		bsaList = append(bsaList, &models.TCBSBSASnapshot{
			Ticker:       p.Ticker,
			InvestorType: p.InvestorType,
			BUP:          p.Bup,
			SDP:          p.Sdp,
			BSR:          p.Bsr,
			EventTime:    p.TradingDate,
			Timestamp:    ts,
		})
	}

	return bsaList, nil
}

func (c *batchClient) GetDataRange(ctx context.Context, source, symbol, instType, investorType string) (*batchpb.DataRangeResponse, error) {
	if !c.IsAvailable() {
		return nil, fmt.Errorf("batch gRPC client not connected to %s", c.target)
	}

	req := &batchpb.DataRangeRequest{
		Source:       source,
		Symbol:       symbol,
		InstType:     instType,
		InvestorType: investorType,
	}

	return c.grpc.GetDataRange(ctx, req)
}
