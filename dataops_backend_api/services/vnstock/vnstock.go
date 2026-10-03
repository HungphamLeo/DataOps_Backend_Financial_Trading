// VNStock Service — query TCBS market data từ InfluxDB
package vnstock

import (
	"context"
	"fmt"
	"dataops-backend-api/configuration"
	"dataops-backend-api/logging"
	"dataops-backend-api/models"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

type Service interface {
	GetMarketSnapshot(ctx context.Context, symbol string) (*models.TCBSMarketSnapshot, error)
	GetOHLCV(ctx context.Context, symbol, interval string, from, to int64) ([]*models.OHLCV, error)
	GetOrderBook(ctx context.Context, symbol string) (map[string]interface{}, error)
	GetTopMovers(ctx context.Context, exchange string, limit int) ([]*models.TCBSMarketSnapshot, error)
	GetMarketSummary(ctx context.Context) (map[string]interface{}, error)
}

type vnStockService struct {
	cfg    *configuration.Config
	client influxdb2.Client
	logger *logging.Logger
}

func NewService(cfg *configuration.Config, logger *logging.Logger) Service {
	client := influxdb2.NewClient(cfg.InfluxDB.URL, cfg.InfluxDB.Token)
	return &vnStockService{cfg: cfg, client: client, logger: logger}
}

// GetMarketSnapshot lấy snapshot mới nhất của 1 mã CK từ InfluxDB (TCBS measurement)
func (s *vnStockService) GetMarketSnapshot(ctx context.Context, symbol string) (*models.TCBSMarketSnapshot, error) {
	queryAPI := s.client.QueryAPI(s.cfg.InfluxDB.Org)

	fluxQuery := fmt.Sprintf(`
		from(bucket: "%s")
		  |> range(start: -1h)
		  |> filter(fn: (r) => r._measurement == "tcbs_market" and r.symbol == "%s")
		  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
		  |> last()
	`, s.cfg.InfluxDB.Bucket, symbol)

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		s.logger.Error("GetMarketSnapshot", "InfluxDB query failed", map[string]interface{}{"symbol": symbol, "err": err.Error()})
		return nil, fmt.Errorf("[VNStockSvc] GetMarketSnapshot: %w", err)
	}
	defer result.Close()

	snapshot := &models.TCBSMarketSnapshot{Symbol: symbol}
	if result.Next() {
		r := result.Record()
		snapshot.Timestamp = r.Time().UnixMilli()
		if v, ok := r.ValueByKey("match_price").(float64); ok {
			snapshot.MatchPrice = v
		}
		if v, ok := r.ValueByKey("total_vol").(float64); ok {
			snapshot.TotalVol = v
		}
		if v, ok := r.ValueByKey("change_pct").(float64); ok {
			snapshot.ChangePct = v
		}
		if v, ok := r.ValueByKey("exchange").(string); ok {
			snapshot.Exchange = v
		}
	}

	return snapshot, nil
}

// GetOHLCV lấy OHLCV lịch sử cổ phiếu VN từ TCBS measurement
func (s *vnStockService) GetOHLCV(ctx context.Context, symbol, interval string, from, to int64) ([]*models.OHLCV, error) {
	queryAPI := s.client.QueryAPI(s.cfg.InfluxDB.Org)

	fluxQuery := fmt.Sprintf(`
		from(bucket: "%s")
		  |> range(start: %d, stop: %d)
		  |> filter(fn: (r) => r._measurement == "tcbs_market" and r.symbol == "%s")
		  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
	`, s.cfg.InfluxDB.Bucket, from, to, symbol)

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		s.logger.Error("GetOHLCV", "InfluxDB query failed", map[string]interface{}{"symbol": symbol, "interval": interval, "err": err.Error()})
		return nil, fmt.Errorf("[VNStockSvc] GetOHLCV: %w", err)
	}
	defer result.Close()

	var candles []*models.OHLCV
	for result.Next() {
		r := result.Record()
		c := &models.OHLCV{Symbol: symbol, Interval: interval, Timestamp: r.Time().Unix()}
		if v, ok := r.ValueByKey("open").(float64); ok {
			c.Open = v
		}
		if v, ok := r.ValueByKey("close").(float64); ok {
			c.Close = v
		}
		candles = append(candles, c)
	}
	return candles, nil
}

// GetOrderBook — placeholder (TCBS orderbook 3 cấp nằm trong GetMarketSnapshot)
func (s *vnStockService) GetOrderBook(_ context.Context, symbol string) (map[string]interface{}, error) {
	// TODO: extract bid/offer từ TCBSMarketSnapshot
	return map[string]interface{}{"symbol": symbol, "bids": []interface{}{}, "asks": []interface{}{}}, nil
}

// GetTopMovers lấy top gainers/losers từ InfluxDB (TCBS measurement)
func (s *vnStockService) GetTopMovers(ctx context.Context, exchange string, limit int) ([]*models.TCBSMarketSnapshot, error) {
	queryAPI := s.client.QueryAPI(s.cfg.InfluxDB.Org)

	fluxQuery := fmt.Sprintf(`
		from(bucket: "%s")
		  |> range(start: -1h)
		  |> filter(fn: (r) => r._measurement == "tcbs_market" and r.exchange == "%s" and r._field == "change_pct")
		  |> last()
		  |> top(n: %d, columns: ["_value"])
	`, s.cfg.InfluxDB.Bucket, exchange, limit)

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		s.logger.Error("GetTopMovers", "InfluxDB query failed", map[string]interface{}{"exchange": exchange, "limit": limit, "err": err.Error()})
		return nil, fmt.Errorf("[VNStockSvc] GetTopMovers: %w", err)
	}
	defer result.Close()

	var movers []*models.TCBSMarketSnapshot
	for result.Next() {
		r := result.Record()
		m := &models.TCBSMarketSnapshot{
			Exchange:  exchange,
			Timestamp: r.Time().UnixMilli(),
		}
		if sym, ok := r.ValueByKey("symbol").(string); ok {
			m.Symbol = sym
		}
		if v, ok := r.Value().(float64); ok {
			m.ChangePct = v
		}
		movers = append(movers, m)
	}
	return movers, nil
}

// GetMarketSummary trả về VNINDEX và VN30 (HOSE indices).
// HNXINDEX không nằm trong scope hiện tại.
func (s *vnStockService) GetMarketSummary(ctx context.Context) (map[string]interface{}, error) {
	// Query 2 HOSE indices
	indices := []string{"VN30", "VNINDEX"}
	summary := map[string]interface{}{}

	for _, idx := range indices {
		snapshot, err := s.GetMarketSnapshot(ctx, idx)
		if err != nil {
			continue
		}
		summary[idx] = snapshot
	}

	return summary, nil
}
