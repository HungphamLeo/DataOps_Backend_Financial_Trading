// tcbs.go — TCBS Market Data Service
//
// Thay thế vnstock.Service bằng TCBSService cho TCBS iFlash API data.
// Query InfluxDB để lấy market snapshots, BSA, intraday ticks đã được
// ingest bởi dataops-stream-producer (TCBSPoller → Kafka → Spark → InfluxDB).
//
// Data flow:
//   TCBS API → TCBSPoller → Kafka Topics → (Spark streaming) → InfluxDB
//              tcbs.market.snapshot  → tcbs_market measurement
//              tcbs.bsa.events       → tcbs_bsa measurement
//              tcbs.intraday.ticks   → tcbs_intraday measurement
//
// Measurement names được đọc từ constants.InfluxMeasurementTCBS* — không hardcode.
package tcbs

import (
	"context"
	"fmt"
	"sort"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"

	"dataops-backend-api/configuration"
	"dataops-backend-api/constants"
	"dataops-backend-api/logging"
	"dataops-backend-api/models"
)

// Service định nghĩa interface cho tất cả TCBS market data queries.
// Tất cả handlers và test mocks dùng interface này — không dùng concrete struct trực tiếp.
type Service interface {
	// GetMarketSnapshot lấy snapshot mới nhất của 1 mã CK từ InfluxDB (tcbs_market measurement).
	GetMarketSnapshot(ctx context.Context, symbol string) (*models.TCBSMarketSnapshot, error)

	// GetOrderBook trích xuất orderbook từ market snapshot — 3 cấp bid/ask.
	GetOrderBook(ctx context.Context, symbol string) (*models.TCBSOrderBook, error)

	// GetOHLCV lấy OHLCV lịch sử từ InfluxDB (tcbs_ohlcv measurement).
	GetOHLCV(ctx context.Context, symbol, interval string, from, to int64) ([]*models.OHLCV, error)

	// GetTopMovers lấy top cổ phiếu tăng/giảm mạnh nhất theo change_pct.
	GetTopMovers(ctx context.Context, exchange string, limit int) ([]*models.TCBSMarketSnapshot, error)

	// GetMarketSummary trả về summary các indices HOSE (VNINDEX, VN30, VN100).
	GetMarketSummary(ctx context.Context) (*models.TCBSMarketSummary, error)

	// GetBSA lấy Buy/Sell Analysis cho 1 mã theo loại nhà đầu tư.
	GetBSA(ctx context.Context, ticker, investorType string) ([]*models.TCBSBSASnapshot, error)

	// GetIntradayTicks lấy lịch sử khớp lệnh intraday (tick-by-tick).
	GetIntradayTicks(ctx context.Context, ticker string, page, size int) ([]*models.TCBSIntradayTick, error)
}

// tcbsService là concrete implementation dùng InfluxDB làm storage backend.
type tcbsService struct {
	cfg    *configuration.Config
	client influxdb2.Client
	logger *logging.Logger
}

// NewService khởi tạo TCBSService với InfluxDB client.
func NewService(cfg *configuration.Config, logger *logging.Logger) Service {
	client := influxdb2.NewClient(cfg.InfluxDB.URL, cfg.InfluxDB.Token)
	return &tcbsService{cfg: cfg, client: client, logger: logger}
}

// ─── GetMarketSnapshot ────────────────────────────────────────────────────────

// GetMarketSnapshot lấy market snapshot mới nhất của 1 mã từ InfluxDB.
// Data được ghi bởi Spark Streaming consumer đọc từ tcbs.market.snapshot topic.
func (s *tcbsService) GetMarketSnapshot(ctx context.Context, symbol string) (*models.TCBSMarketSnapshot, error) {
	queryAPI := s.client.QueryAPI(s.cfg.InfluxDB.Org)

	fluxQuery := fmt.Sprintf(`
		from(bucket: "%s")
		  |> range(start: -2h)
		  |> filter(fn: (r) => r._measurement == "%s" and r.symbol == "%s")
		  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
		  |> last()
	`, s.cfg.InfluxDB.Bucket, constants.InfluxMeasurementTCBSMarket, symbol)

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		s.logger.Error("TCBSSvc.GetMarketSnapshot", "InfluxDB query failed",
			map[string]interface{}{"symbol": symbol, "err": err.Error()})
		return nil, fmt.Errorf("[TCBSSvc] GetMarketSnapshot symbol=%s: %w", symbol, err)
	}
	defer result.Close()

	snapshot := &models.TCBSMarketSnapshot{Symbol: symbol}
	if result.Next() {
		r := result.Record()
		snapshot.Timestamp = r.Time().Unix()
		getF := func(key string) float64 {
			if v, ok := r.ValueByKey(key).(float64); ok {
				return v
			}
			return 0
		}
		getS := func(key string) string {
			if v, ok := r.ValueByKey(key).(string); ok {
				return v
			}
			return ""
		}
		snapshot.Exchange       = getS("exchange")
		snapshot.RefPrice       = getF("ref_price")
		snapshot.CeilPrice      = getF("ceil_price")
		snapshot.FloorPrice     = getF("floor_price")
		snapshot.MatchPrice     = getF("match_price")
		snapshot.MatchQty       = getF("match_qty")
		snapshot.BidPrice01     = getF("bid_price_01")
		snapshot.BidPrice02     = getF("bid_price_02")
		snapshot.BidPrice03     = getF("bid_price_03")
		snapshot.BidQty01       = getF("bid_qty_01")
		snapshot.BidQty02       = getF("bid_qty_02")
		snapshot.BidQty03       = getF("bid_qty_03")
		snapshot.OfferPrice01   = getF("offer_price_01")
		snapshot.OfferPrice02   = getF("offer_price_02")
		snapshot.OfferPrice03   = getF("offer_price_03")
		snapshot.OfferQty01     = getF("offer_qty_01")
		snapshot.OfferQty02     = getF("offer_qty_02")
		snapshot.OfferQty03     = getF("offer_qty_03")
		snapshot.Open           = getF("open")
		snapshot.High           = getF("high")
		snapshot.Low            = getF("low")
		snapshot.Avg            = getF("avg")
		snapshot.TotalVol       = getF("total_vol")
		snapshot.TotalVal       = getF("total_val")
		snapshot.BuyForeignQty  = getF("buy_foreign_qty")
		snapshot.SellForeignQty = getF("sell_foreign_qty")
		snapshot.Room           = getS("room")
		snapshot.Change         = getF("change")
		snapshot.ChangePct      = getF("change_pct")
		snapshot.TradingDate    = getS("trading_date")
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("[TCBSSvc] GetMarketSnapshot result error: %w", result.Err())
	}

	return snapshot, nil
}

// ─── GetOrderBook ────────────────────────────────────────────────────────────

// GetOrderBook trích xuất orderbook 3 cấp từ market snapshot.
// Không query riêng — reuse GetMarketSnapshot để tránh double query.
func (s *tcbsService) GetOrderBook(ctx context.Context, symbol string) (*models.TCBSOrderBook, error) {
	snap, err := s.GetMarketSnapshot(ctx, symbol)
	if err != nil {
		return nil, fmt.Errorf("[TCBSSvc] GetOrderBook: %w", err)
	}

	ob := &models.TCBSOrderBook{
		Symbol:    symbol,
		Exchange:  snap.Exchange,
		Timestamp: snap.Timestamp,
		Bids: []models.TCBSPriceLevel{
			{Price: snap.BidPrice01, Quantity: snap.BidQty01},
			{Price: snap.BidPrice02, Quantity: snap.BidQty02},
			{Price: snap.BidPrice03, Quantity: snap.BidQty03},
		},
		Asks: []models.TCBSPriceLevel{
			{Price: snap.OfferPrice01, Quantity: snap.OfferQty01},
			{Price: snap.OfferPrice02, Quantity: snap.OfferQty02},
			{Price: snap.OfferPrice03, Quantity: snap.OfferQty03},
		},
	}

	// Lọc bỏ các bước giá = 0 (không có lệnh chờ)
	bids := ob.Bids[:0]
	for _, b := range ob.Bids {
		if b.Price > 0 {
			bids = append(bids, b)
		}
	}
	ob.Bids = bids

	asks := ob.Asks[:0]
	for _, a := range ob.Asks {
		if a.Price > 0 {
			asks = append(asks, a)
		}
	}
	ob.Asks = asks

	return ob, nil
}

// ─── GetOHLCV ────────────────────────────────────────────────────────────────

// GetOHLCV lấy OHLCV lịch sử cổ phiếu từ InfluxDB (tcbs_ohlcv measurement).
// Data được aggregate từ intraday ticks bởi Spark Silver layer.
func (s *tcbsService) GetOHLCV(ctx context.Context, symbol, interval string, from, to int64) ([]*models.OHLCV, error) {
	queryAPI := s.client.QueryAPI(s.cfg.InfluxDB.Org)

	fluxQuery := fmt.Sprintf(`
		from(bucket: "%s")
		  |> range(start: %d, stop: %d)
		  |> filter(fn: (r) => r._measurement == "%s" and r.symbol == "%s" and r.interval == "%s")
		  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
		  |> sort(columns: ["_time"])
	`, s.cfg.InfluxDB.Bucket, from, to, constants.InfluxMeasurementTCBSOHLCV, symbol, interval)

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		s.logger.Error("TCBSSvc.GetOHLCV", "InfluxDB query failed",
			map[string]interface{}{"symbol": symbol, "interval": interval, "err": err.Error()})
		return nil, fmt.Errorf("[TCBSSvc] GetOHLCV symbol=%s interval=%s: %w", symbol, interval, err)
	}
	defer result.Close()

	var candles []*models.OHLCV
	for result.Next() {
		r := result.Record()
		c := &models.OHLCV{Symbol: symbol, Interval: interval, Timestamp: r.Time().Unix()}
		if v, ok := r.ValueByKey("open").(float64); ok {
			c.Open = v
		}
		if v, ok := r.ValueByKey("high").(float64); ok {
			c.High = v
		}
		if v, ok := r.ValueByKey("low").(float64); ok {
			c.Low = v
		}
		if v, ok := r.ValueByKey("close").(float64); ok {
			c.Close = v
		}
		if v, ok := r.ValueByKey("volume").(float64); ok {
			c.Volume = v
		}
		candles = append(candles, c)
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("[TCBSSvc] GetOHLCV result error: %w", result.Err())
	}

	return candles, nil
}

// ─── GetTopMovers ─────────────────────────────────────────────────────────────

// GetTopMovers lấy top cổ phiếu tăng/giảm mạnh nhất theo change_pct từ InfluxDB.
// Chỉ hỗ trợ sàn HOSE theo exchange label trong tcbs_market measurement.
func (s *tcbsService) GetTopMovers(ctx context.Context, exchange string, limit int) ([]*models.TCBSMarketSnapshot, error) {
	queryAPI := s.client.QueryAPI(s.cfg.InfluxDB.Org)

	fluxQuery := fmt.Sprintf(`
		from(bucket: "%s")
		  |> range(start: -2h)
		  |> filter(fn: (r) => r._measurement == "%s" and r.exchange == "%s" and r._field == "change_pct")
		  |> last()
		  |> top(n: %d, columns: ["_value"])
	`, s.cfg.InfluxDB.Bucket, constants.InfluxMeasurementTCBSMarket, exchange, limit)

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		s.logger.Error("TCBSSvc.GetTopMovers", "InfluxDB query failed",
			map[string]interface{}{"exchange": exchange, "limit": limit, "err": err.Error()})
		return nil, fmt.Errorf("[TCBSSvc] GetTopMovers exchange=%s: %w", exchange, err)
	}
	defer result.Close()

	var movers []*models.TCBSMarketSnapshot
	for result.Next() {
		r := result.Record()
		m := &models.TCBSMarketSnapshot{
			Exchange:  exchange,
			Timestamp: r.Time().Unix(),
		}
		if sym, ok := r.ValueByKey("symbol").(string); ok {
			m.Symbol = sym
		}
		if v, ok := r.Value().(float64); ok {
			m.ChangePct = v
		}
		movers = append(movers, m)
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("[TCBSSvc] GetTopMovers result error: %w", result.Err())
	}

	return movers, nil
}

// ─── GetMarketSummary ─────────────────────────────────────────────────────────

// GetMarketSummary trả về tóm tắt HOSE indices (VNINDEX, VN30, VN100).
// Query 3 indices song song, bỏ qua nếu 1 index không có data.
func (s *tcbsService) GetMarketSummary(ctx context.Context) (*models.TCBSMarketSummary, error) {
	indices := []string{"VNINDEX", "VN30", "VN100"}
	summary := &models.TCBSMarketSummary{
		Indices: make(map[string]*models.TCBSMarketSnapshot),
	}

	for _, idx := range indices {
		snap, err := s.GetMarketSnapshot(ctx, idx)
		if err != nil {
			s.logger.Error("TCBSSvc.GetMarketSummary", "Failed to get index snapshot",
				map[string]interface{}{"index": idx, "err": err.Error()})
			continue // bỏ qua index lỗi, không fail toàn bộ request
		}
		summary.Indices[idx] = snap
		if snap.TradingDate != "" && summary.TradingDate == "" {
			summary.TradingDate = snap.TradingDate
		}
	}

	return summary, nil
}

// ─── GetBSA ───────────────────────────────────────────────────────────────────

// GetBSA lấy Buy/Sell Analysis cho 1 mã theo loại nhà đầu tư từ InfluxDB.
// Data được ghi bởi Spark Streaming consumer đọc từ tcbs.bsa.events topic.
func (s *tcbsService) GetBSA(ctx context.Context, ticker, investorType string) ([]*models.TCBSBSASnapshot, error) {
	queryAPI := s.client.QueryAPI(s.cfg.InfluxDB.Org)

	fluxQuery := fmt.Sprintf(`
		from(bucket: "%s")
		  |> range(start: -24h)
		  |> filter(fn: (r) => r._measurement == "%s" and r.ticker == "%s" and r.investor_type == "%s")
		  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
		  |> sort(columns: ["_time"])
	`, s.cfg.InfluxDB.Bucket, constants.InfluxMeasurementTCBSBSA, ticker, investorType)

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		s.logger.Error("TCBSSvc.GetBSA", "InfluxDB query failed",
			map[string]interface{}{"ticker": ticker, "investor_type": investorType, "err": err.Error()})
		return nil, fmt.Errorf("[TCBSSvc] GetBSA ticker=%s investorType=%s: %w", ticker, investorType, err)
	}
	defer result.Close()

	var bsaList []*models.TCBSBSASnapshot
	for result.Next() {
		r := result.Record()
		bsa := &models.TCBSBSASnapshot{
			Ticker:       ticker,
			InvestorType: investorType,
			Timestamp:    r.Time().Unix(),
		}
		if v, ok := r.ValueByKey("bup").(float64); ok {
			bsa.BUP = v
		}
		if v, ok := r.ValueByKey("sdp").(float64); ok {
			bsa.SDP = v
		}
		if v, ok := r.ValueByKey("bsr").(float64); ok {
			bsa.BSR = v
		}
		if v, ok := r.ValueByKey("event_time").(string); ok {
			bsa.EventTime = v
		}
		bsaList = append(bsaList, bsa)
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("[TCBSSvc] GetBSA result error: %w", result.Err())
	}

	return bsaList, nil
}

// ─── GetIntradayTicks ─────────────────────────────────────────────────────────

// GetIntradayTicks lấy lịch sử khớp lệnh intraday từ InfluxDB với pagination.
// Data được ghi bởi Spark Streaming consumer đọc từ tcbs.intraday.ticks topic.
func (s *tcbsService) GetIntradayTicks(ctx context.Context, ticker string, page, size int) ([]*models.TCBSIntradayTick, error) {
	if size <= 0 || size > constants.MaxTCBSIntradayPages*constants.DefaultTCBSIntradayPageSize {
		size = constants.DefaultTCBSIntradayPageSize
	}
	offset := page * size

	queryAPI := s.client.QueryAPI(s.cfg.InfluxDB.Org)

	fluxQuery := fmt.Sprintf(`
		from(bucket: "%s")
		  |> range(start: -24h)
		  |> filter(fn: (r) => r._measurement == "%s" and r.ticker == "%s")
		  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
		  |> sort(columns: ["_time"], desc: true)
		  |> limit(n: %d, offset: %d)
	`, s.cfg.InfluxDB.Bucket, constants.InfluxMeasurementTCBSIntraday, ticker, size, offset)

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		s.logger.Error("TCBSSvc.GetIntradayTicks", "InfluxDB query failed",
			map[string]interface{}{"ticker": ticker, "page": page, "size": size, "err": err.Error()})
		return nil, fmt.Errorf("[TCBSSvc] GetIntradayTicks ticker=%s: %w", ticker, err)
	}
	defer result.Close()

	var ticks []*models.TCBSIntradayTick
	for result.Next() {
		r := result.Record()
		tick := &models.TCBSIntradayTick{
			Ticker:    ticker,
			Timestamp: r.Time().Unix(),
		}
		if v, ok := r.ValueByKey("price").(float64); ok {
			tick.Price = v
		}
		if v, ok := r.ValueByKey("volume").(float64); ok {
			tick.Volume = v
		}
		if v, ok := r.ValueByKey("price_change").(float64); ok {
			tick.PriceChange = v
		}
		if v, ok := r.ValueByKey("action").(string); ok {
			tick.Action = v
		}
		if v, ok := r.ValueByKey("buy_acc").(float64); ok {
			tick.BuyAcc = v
		}
		if v, ok := r.ValueByKey("sell_acc").(float64); ok {
			tick.SellAcc = v
		}
		if v, ok := r.ValueByKey("highlight").(bool); ok {
			tick.Highlight = v
		}
		if v, ok := r.ValueByKey("event_time").(string); ok {
			tick.EventTime = v
		}
		ticks = append(ticks, tick)
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("[TCBSSvc] GetIntradayTicks result error: %w", result.Err())
	}

	// Sort ascending by timestamp cho client dễ render
	sort.Slice(ticks, func(i, j int) bool {
		return ticks[i].Timestamp < ticks[j].Timestamp
	})

	return ticks, nil
}
