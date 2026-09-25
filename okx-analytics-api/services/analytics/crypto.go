// CryptoService interface + implementation
// Query VWAP, OHLCV, OrderBook từ InfluxDB (real-time) và Delta Lake (historical)
package analytics

import (
	"context"
	"fmt"
	"okx-analytics-api/configuration"
	"okx-analytics-api/models"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

// CryptoService định nghĩa contract — handlers chỉ depend vào interface này
type CryptoService interface {
	GetVWAP(ctx context.Context, symbol, interval string, from, to int64) ([]*models.VWAPPoint, error)
	GetOHLCV(ctx context.Context, symbol, interval string, from, to int64) ([]*models.OHLCV, error)
	GetOrderBook(ctx context.Context, symbol string) (*models.OKXOrderBook5, error)
	GetFundingRate(ctx context.Context, symbol string, from, to int64) ([]*models.OKXFundingRate, error)
	GetLiquidations(ctx context.Context, symbol string, from, to int64) ([]map[string]interface{}, error)
}

type cryptoService struct {
	cfg    *configuration.Config
	client influxdb2.Client
}

func NewCryptoService(cfg *configuration.Config) CryptoService {
	client := influxdb2.NewClient(cfg.InfluxDB.URL, cfg.InfluxDB.Token)
	return &cryptoService{cfg: cfg, client: client}
}

// GetVWAP query VWAP từ InfluxDB
// InfluxDB bucket: "market-data", measurement: "vwap"
func (s *cryptoService) GetVWAP(ctx context.Context, symbol, interval string, from, to int64) ([]*models.VWAPPoint, error) {
	queryAPI := s.client.QueryAPI(s.cfg.InfluxDB.Org)

	// Flux query — ngôn ngữ query của InfluxDB 2.x
	fluxQuery := fmt.Sprintf(`
		from(bucket: "%s")
		  |> range(start: %d, stop: %d)
		  |> filter(fn: (r) => r._measurement == "vwap" and r.symbol == "%s" and r.interval == "%s")
		  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
	`, s.cfg.InfluxDB.Bucket, from, to, symbol, interval)

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("[CryptoService] GetVWAP: %w", err)
	}
	defer result.Close()

	var points []*models.VWAPPoint
	for result.Next() {
		record := result.Record()
		point := &models.VWAPPoint{
			Symbol:    symbol,
			Interval:  interval,
			Timestamp: record.Time().Unix(),
		}
		if v, ok := record.ValueByKey("vwap").(float64); ok {
			point.VWAP = v
		}
		if v, ok := record.ValueByKey("volume").(float64); ok {
			point.Volume = v
		}
		points = append(points, point)
	}

	return points, nil
}

// GetOHLCV query OHLCV từ InfluxDB
func (s *cryptoService) GetOHLCV(ctx context.Context, symbol, interval string, from, to int64) ([]*models.OHLCV, error) {
	queryAPI := s.client.QueryAPI(s.cfg.InfluxDB.Org)

	fluxQuery := fmt.Sprintf(`
		from(bucket: "%s")
		  |> range(start: %d, stop: %d)
		  |> filter(fn: (r) => r._measurement == "ohlcv" and r.symbol == "%s" and r.interval == "%s")
		  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
	`, s.cfg.InfluxDB.Bucket, from, to, symbol, interval)

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("[CryptoService] GetOHLCV: %w", err)
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

	return candles, nil
}

// GetOrderBook — placeholder, real-time data từ Redis cache
func (s *cryptoService) GetOrderBook(_ context.Context, symbol string) (*models.OKXOrderBook5, error) {
	// TODO: đọc từ Redis cache key "okx_{symbol}_books5"
	return &models.OKXOrderBook5{InstID: symbol}, nil
}

// GetFundingRate query funding rate từ InfluxDB
func (s *cryptoService) GetFundingRate(ctx context.Context, symbol string, from, to int64) ([]*models.OKXFundingRate, error) {
	queryAPI := s.client.QueryAPI(s.cfg.InfluxDB.Org)

	fluxQuery := fmt.Sprintf(`
		from(bucket: "%s")
		  |> range(start: %d, stop: %d)
		  |> filter(fn: (r) => r._measurement == "funding_rate" and r.symbol == "%s")
		  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
	`, s.cfg.InfluxDB.Bucket, from, to, symbol)

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("[CryptoService] GetFundingRate: %w", err)
	}
	defer result.Close()

	var rates []*models.OKXFundingRate
	for result.Next() {
		r := result.Record()
		rate := &models.OKXFundingRate{
			InstID:      symbol,
			FundingTime: r.Time().Unix(),
		}
		if v, ok := r.ValueByKey("funding_rate").(float64); ok {
			rate.FundingRate = v
		}
		rates = append(rates, rate)
	}

	return rates, nil
}

// GetLiquidations — placeholder
func (s *cryptoService) GetLiquidations(_ context.Context, symbol string, from, to int64) ([]map[string]interface{}, error) {
	// TODO: query từ InfluxDB measurement "liquidations"
	return []map[string]interface{}{}, nil
}
