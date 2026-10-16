// CryptoService interface + implementation
// Query VWAP, OHLCV, OrderBook, Ticker từ Redis (real-time cache), InfluxDB, và Batch gRPC (historical)
// Source: Binance data via dataops-stream-producer → Kafka → InfluxDB / Delta Lake
package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"dataops-backend-api/configuration"
	"dataops-backend-api/logging"
	"dataops-backend-api/models"
	"dataops-backend-api/redis"
	"dataops-backend-api/repository/batch"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

// CryptoService định nghĩa contract — handlers chỉ depend vào interface này
type CryptoService interface {
	GetTicker(ctx context.Context, symbol string) (*models.BinanceTradeTick, error)
	GetVWAP(ctx context.Context, symbol, interval string, from, to int64) ([]*models.VWAPPoint, error)
	GetOHLCV(ctx context.Context, symbol, interval string, from, to int64) ([]*models.OHLCV, error)
	GetOrderBook(ctx context.Context, symbol string) (*models.BinanceOrderBook5, error)
	GetFundingRate(ctx context.Context, symbol string, from, to int64) ([]*models.BinanceFundingRate, error)
	GetLiquidations(ctx context.Context, symbol string, from, to int64) ([]map[string]interface{}, error)
	// ComputeSignal tính RSI(14) + MA(20) signal từ InfluxDB VWAP series
	ComputeSignal(ctx context.Context, symbol, timeframe string) (*models.IndicatorSignal, error)
}

type cryptoService struct {
	cfg         *configuration.Config
	client      influxdb2.Client
	redis       redis.Service
	batchClient batch.Client
	logger      *logging.Logger
}

func NewCryptoService(
	cfg *configuration.Config,
	redisSvc redis.Service,
	batchClient batch.Client,
	logger *logging.Logger,
) CryptoService {
	client := influxdb2.NewClient(cfg.InfluxDB.URL, cfg.InfluxDB.Token)
	return &cryptoService{
		cfg:         cfg,
		client:      client,
		redis:       redisSvc,
		batchClient: batchClient,
		logger:      logger,
	}
}

// GetTicker lấy ticker mới nhất của 1 symbol từ Redis cache (TTL 10s)
func (s *cryptoService) GetTicker(ctx context.Context, symbol string) (*models.BinanceTradeTick, error) {
	symLower := strings.ToLower(symbol)
	cacheKey := fmt.Sprintf("binance_%s_ticker", symLower)

	if s.redis != nil {
		val := s.redis.Get(cacheKey)
		if val != "" {
			var tick models.BinanceTradeTick
			if err := json.Unmarshal([]byte(val), &tick); err == nil {
				return &tick, nil
			}
		}
	}

	return &models.BinanceTradeTick{
		InstID:    symbol,
		Timestamp: time.Now().Unix(),
	}, nil
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
		s.logger.Error("GetVWAP", "InfluxDB query failed", map[string]interface{}{"symbol": symbol, "interval": interval, "err": err.Error()})
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

// GetOHLCV query OHLCV từ Batch gRPC (historical 4h, 1d) hoặc fallback InfluxDB
func (s *cryptoService) GetOHLCV(ctx context.Context, symbol, interval string, from, to int64) ([]*models.OHLCV, error) {
	// 1. Nếu interval là 4h hoặc 1d và Batch gRPC khả dụng → query Delta Lake qua gRPC
	if (interval == "4h" || interval == "1d") && s.batchClient != nil && s.batchClient.IsAvailable() {
		fromTime := time.Unix(from, 0)
		toTime := time.Unix(to, 0)
		candles, err := s.batchClient.GetBinanceOHLCV(ctx, symbol, "SPOT", interval, fromTime, toTime, 1000)
		if err == nil && len(candles) > 0 {
			return candles, nil
		}
		if err != nil {
			s.logger.Warn("GetOHLCV.BatchGRPC", "Batch gRPC query failed, falling back to InfluxDB",
				map[string]interface{}{"symbol": symbol, "interval": interval, "err": err.Error()})
		}
	}

	// 2. Fallback / Intraday: query từ InfluxDB
	queryAPI := s.client.QueryAPI(s.cfg.InfluxDB.Org)

	fluxQuery := fmt.Sprintf(`
		from(bucket: "%s")
		  |> range(start: %d, stop: %d)
		  |> filter(fn: (r) => r._measurement == "ohlcv" and r.symbol == "%s" and r.interval == "%s")
		  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
	`, s.cfg.InfluxDB.Bucket, from, to, symbol, interval)

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		s.logger.Error("GetOHLCV", "InfluxDB query failed", map[string]interface{}{"symbol": symbol, "interval": interval, "err": err.Error()})
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

// GetOrderBook lấy real-time order book snapshot top 5 từ Redis cache
func (s *cryptoService) GetOrderBook(_ context.Context, symbol string) (*models.BinanceOrderBook5, error) {
	symLower := strings.ToLower(symbol)
	cacheKey := fmt.Sprintf("binance_%s_depth5", symLower)

	if s.redis != nil {
		val := s.redis.Get(cacheKey)
		if val != "" {
			var ob models.BinanceOrderBook5
			if err := json.Unmarshal([]byte(val), &ob); err == nil {
				return &ob, nil
			}
		}
	}

	return &models.BinanceOrderBook5{
		InstID:    symbol,
		Timestamp: time.Now().Unix(),
		Bids:      [][4]string{},
		Asks:      [][4]string{},
	}, nil
}

// GetFundingRate query funding rate từ InfluxDB
func (s *cryptoService) GetFundingRate(ctx context.Context, symbol string, from, to int64) ([]*models.BinanceFundingRate, error) {
	queryAPI := s.client.QueryAPI(s.cfg.InfluxDB.Org)

	fluxQuery := fmt.Sprintf(`
		from(bucket: "%s")
		  |> range(start: %d, stop: %d)
		  |> filter(fn: (r) => r._measurement == "funding_rate" and r.symbol == "%s")
		  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
	`, s.cfg.InfluxDB.Bucket, from, to, symbol)

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		s.logger.Error("GetFundingRate", "InfluxDB query failed", map[string]interface{}{"symbol": symbol, "err": err.Error()})
		return nil, fmt.Errorf("[CryptoService] GetFundingRate: %w", err)
	}
	defer result.Close()

	var rates []*models.BinanceFundingRate
	for result.Next() {
		r := result.Record()
		rate := &models.BinanceFundingRate{
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

// GetLiquidations query liquidation events từ InfluxDB
func (s *cryptoService) GetLiquidations(ctx context.Context, symbol string, from, to int64) ([]map[string]interface{}, error) {
	queryAPI := s.client.QueryAPI(s.cfg.InfluxDB.Org)

	fluxQuery := fmt.Sprintf(`
		from(bucket: "%s")
		  |> range(start: %d, stop: %d)
		  |> filter(fn: (r) => r._measurement == "liquidations" and r.symbol == "%s")
		  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
	`, s.cfg.InfluxDB.Bucket, from, to, symbol)

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		s.logger.Error("GetLiquidations", "InfluxDB query failed", map[string]interface{}{"symbol": symbol, "err": err.Error()})
		return nil, fmt.Errorf("[CryptoService] GetLiquidations: %w", err)
	}
	defer result.Close()

	var events []map[string]interface{}
	for result.Next() {
		r := result.Record()
		ev := map[string]interface{}{
			"symbol":    symbol,
			"timestamp": r.Time().Unix(),
		}
		for k, v := range r.Values() {
			ev[k] = v
		}
		events = append(events, ev)
	}

	return events, nil
<<<<<<< HEAD
=======
}

// ComputeSignal tính RSI(14) + MA(20) từ InfluxDB VWAP series.
//
// Fetch 21 VWAP points từ measurement "vwap" (written bởi dataops_processing VWAP Spark job).
// Nếu không đủ data → trả NEUTRAL với confidence 0.
func (s *cryptoService) ComputeSignal(_ context.Context, symbol, timeframe string) (*models.IndicatorSignal, error) {
	queryAPI := s.client.QueryAPI(s.cfg.InfluxDB.Org)

	const n = 21
	lookbackHours := n * 3
	if timeframe == "1d" {
		lookbackHours = n * 48
	} else if timeframe == "5m" {
		lookbackHours = 6
	}

	fluxQuery := fmt.Sprintf(`
		from(bucket: "%s")
		  |> range(start: -%dh)
		  |> filter(fn: (r) => r._measurement == "vwap" and r.symbol == "%s" and r.interval == "%s")
		  |> filter(fn: (r) => r._field == "vwap")
		  |> sort(columns: ["_time"], desc: false)
		  |> tail(n: %d)
	`, s.cfg.InfluxDB.Bucket, lookbackHours, symbol, timeframe, n)

	result, err := queryAPI.Query(context.Background(), fluxQuery)
	if err != nil {
		s.logger.Warn("ComputeSignal", "InfluxDB query failed",
			map[string]interface{}{"symbol": symbol, "err": err.Error()})
		return neutralSignal(symbol), nil
	}
	defer result.Close()

	var prices []float64
	for result.Next() {
		if v, ok := result.Record().Value().(float64); ok && v > 0 {
			prices = append(prices, v)
		}
	}

	if len(prices) < 15 {
		s.logger.Warn("ComputeSignal", "insufficient data",
			map[string]interface{}{"symbol": symbol, "timeframe": timeframe, "points": len(prices)})
		return neutralSignal(symbol), nil
	}

	ma20 := calcCryptoMA(prices, 20)
	rsi14 := calcCryptoRSI(prices, 14)
	current := prices[len(prices)-1]
	signalType, confidence := classifyCryptoSignal(current, ma20, rsi14)

	s.logger.Info("ComputeSignal", "signal computed",
		map[string]interface{}{
			"symbol": symbol, "timeframe": timeframe,
			"price": current, "ma20": ma20, "rsi14": rsi14, "signal": signalType,
		})

	return &models.IndicatorSignal{
		Symbol:     symbol,
		SignalType: signalType,
		Confidence: confidence,
		Value:      rsi14,
		Timeframe:  timeframe,
	}, nil
}

func neutralSignal(symbol string) *models.IndicatorSignal {
	return &models.IndicatorSignal{Symbol: symbol, SignalType: "NEUTRAL", Confidence: 0.0}
}

func calcCryptoMA(prices []float64, n int) float64 {
	if len(prices) < n {
		n = len(prices)
	}
	slice := prices[len(prices)-n:]
	sum := 0.0
	for _, p := range slice {
		sum += p
	}
	return sum / float64(n)
}

func calcCryptoRSI(prices []float64, period int) float64 {
	if len(prices) < period+1 {
		return 50.0
	}
	slice := prices[len(prices)-(period+1):]
	var gains, losses float64
	for i := 1; i < len(slice); i++ {
		d := slice[i] - slice[i-1]
		if d > 0 {
			gains += d
		} else {
			losses += -d
		}
	}
	avgGain := gains / float64(period)
	avgLoss := losses / float64(period)
	if avgLoss == 0 {
		return 100.0
	}
	rs := avgGain / avgLoss
	return 100.0 - (100.0 / (1.0 + rs))
}

func classifyCryptoSignal(price, ma20, rsi14 float64) (string, float64) {
	switch {
	case rsi14 > 70 && price > ma20:
		return "SELL", 0.75
	case rsi14 < 30 && price < ma20:
		return "BUY", 0.75
	case rsi14 > 60 || price > ma20:
		return "HOLD", 0.55
	case rsi14 < 40 || price < ma20:
		return "HOLD", 0.55
	default:
		return "NEUTRAL", 0.5
	}
>>>>>>> 20261010_data_processing
}
