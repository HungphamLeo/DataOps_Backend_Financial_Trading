// REUSE: NEW — không có trong các repos cũ (InfluxDB v2 là mới với project này)
// LEARN: Repository Pattern — tách biệt "truy vấn data" khỏi "business logic"
//
// InfluxDB là time-series database:
//   - Tối ưu cho dữ liệu có timestamp (price ticks, trades, metrics)
//   - Dùng Flux query language (giống SQL nhưng cho time-series)
//   - Tổ chức: Organization → Bucket → Measurement → Fields + Tags
//
// So sánh với MySQL:
//   MySQL: Database → Table → Row → Columns
//   InfluxDB: Org → Bucket → Measurement → (Fields=values, Tags=indexed labels)

package influxdb

import (
	"context"
	"fmt"
	"log"
	"okx-analytics-api/configuration"
	"okx-analytics-api/models"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

// Repository định nghĩa tất cả các queries vào InfluxDB
// LEARN: Interface pattern — services gọi Repository.Method(), không gọi InfluxDB trực tiếp
type Repository interface {
	// OKX Crypto queries
	QueryOKXTrades(ctx context.Context, instID string, from, to time.Time) ([]models.OKXTradeTick, error)
	QueryOKXOHLCV(ctx context.Context, instID string, timeframe string, from, to time.Time) ([]models.OHLCV, error)
	QueryOKXOrderBook(ctx context.Context, instID string) (*models.OrderBookSnapshot, error)
	QueryFundingRates(ctx context.Context, instID string, from, to time.Time) ([]models.FundingRate, error)
	QueryLiquidations(ctx context.Context, instID string, from, to time.Time) ([]models.Liquidation, error)

	// VPS Vietnam Stock queries
	QueryVPSSnapshot(ctx context.Context, symbol string, from, to time.Time) ([]models.VPSMarketSnapshot, error)
	QueryVPSOHLCV(ctx context.Context, symbol string, timeframe string, from, to time.Time) ([]models.OHLCV, error)
	QueryVPSTopMovers(ctx context.Context, exchange string, limit int) ([]models.TopMover, error)

	// Housekeeping
	WritePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time) error
	Close()
}

// influxRepo là concrete implementation
type influxRepo struct {
	client   influxdb2.Client
	queryAPI api.QueryAPI
	writeAPI api.WriteAPIBlocking
	org      string
	bucket   string
}

// NewRepository khởi tạo kết nối InfluxDB
// LEARN: influxdb2.NewClient trả về client (không connect ngay — lazy connection)
func NewRepository(cfg *configuration.Config) Repository {
	client := influxdb2.NewClient(cfg.InfluxDB.URL, cfg.InfluxDB.Token)

	return &influxRepo{
		client:   client,
		queryAPI: client.QueryAPI(cfg.InfluxDB.Org),
		writeAPI: client.WriteAPIBlocking(cfg.InfluxDB.Org, cfg.InfluxDB.Bucket),
		org:      cfg.InfluxDB.Org,
		bucket:   cfg.InfluxDB.Bucket,
	}
}

// QueryOKXTrades truy vấn trade ticks từ InfluxDB bằng Flux query language
// LEARN: Flux query — functional data pipeline:
//   from(bucket:...) → range(start:...) → filter(fn:...) → pivot → yield
func (r *influxRepo) QueryOKXTrades(ctx context.Context, instID string, from, to time.Time) ([]models.OKXTradeTick, error) {
	// LEARN: Heredoc-style string trong Go dùng backtick (`)
	// %s là format verbs — fmt.Sprintf thay thế %s bằng giá trị thực
	fluxQuery := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "okx_trades")
  |> filter(fn: (r) => r.instId == "%s")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"], desc: false)`,
		r.bucket,
		from.Format(time.RFC3339),
		to.Format(time.RFC3339),
		instID,
	)

	result, err := r.queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("influxdb query okx_trades: %w", err)
	}
	defer result.Close()

	var trades []models.OKXTradeTick
	// LEARN: result.Next() là iterator pattern — duyệt kết quả từng dòng
	for result.Next() {
		record := result.Record()
		trade := models.OKXTradeTick{
			InstID:    instID,
			Timestamp: record.Time(),
		}

		// LEARN: type assertion với ok-check (safe) vs tanpa ok (panic nếu sai type)
		if v, ok := record.ValueByKey("price").(float64); ok {
			trade.Price = v
		}
		if v, ok := record.ValueByKey("size").(float64); ok {
			trade.Size = v
		}
		if v, ok := record.ValueByKey("side").(string); ok {
			trade.Side = v
		}
		trades = append(trades, trade)
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("influxdb iterate okx_trades: %w", result.Err())
	}

	return trades, nil
}

// QueryOKXOHLCV tính OHLCV (Open/High/Low/Close/Volume) bằng Flux aggregation
// LEARN: Flux window + aggregateWindow = resample time-series vào nến (candles)
func (r *influxRepo) QueryOKXOHLCV(ctx context.Context, instID string, timeframe string, from, to time.Time) ([]models.OHLCV, error) {
	// timeframe: "1m", "5m", "1h", "1d" → chuyển sang Flux duration
	fluxDuration := timeframeToDuration(timeframe)

	fluxQuery := fmt.Sprintf(`
priceData = from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "okx_trades")
  |> filter(fn: (r) => r.instId == "%s")
  |> filter(fn: (r) => r._field == "price")

open = priceData |> aggregateWindow(every: %s, fn: first, createEmpty: false) |> rename(columns: {_value: "open"})
high = priceData |> aggregateWindow(every: %s, fn: max,   createEmpty: false) |> rename(columns: {_value: "high"})
low  = priceData |> aggregateWindow(every: %s, fn: min,   createEmpty: false) |> rename(columns: {_value: "low"})
close = priceData |> aggregateWindow(every: %s, fn: last,  createEmpty: false) |> rename(columns: {_value: "close"})

volume = from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "okx_trades")
  |> filter(fn: (r) => r.instId == "%s")
  |> filter(fn: (r) => r._field == "size")
  |> aggregateWindow(every: %s, fn: sum, createEmpty: false)
  |> rename(columns: {_value: "volume"})

join(tables: {open: open, high: high, low: low, close: close, volume: volume}, on: ["_time"])`,
		r.bucket, from.Format(time.RFC3339), to.Format(time.RFC3339), instID,
		fluxDuration, fluxDuration, fluxDuration, fluxDuration,
		r.bucket, from.Format(time.RFC3339), to.Format(time.RFC3339), instID,
		fluxDuration,
	)

	result, err := r.queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("influxdb query ohlcv: %w", err)
	}
	defer result.Close()

	var candles []models.OHLCV
	for result.Next() {
		record := result.Record()
		candle := models.OHLCV{
			Symbol:    instID,
			Timeframe: timeframe,
			Timestamp: record.Time(),
		}
		if v, ok := record.ValueByKey("open").(float64); ok {
			candle.Open = v
		}
		if v, ok := record.ValueByKey("high").(float64); ok {
			candle.High = v
		}
		if v, ok := record.ValueByKey("low").(float64); ok {
			candle.Low = v
		}
		if v, ok := record.ValueByKey("close").(float64); ok {
			candle.Close = v
		}
		if v, ok := record.ValueByKey("volume").(float64); ok {
			candle.Volume = v
		}
		candles = append(candles, candle)
	}
	return candles, result.Err()
}

// QueryOKXOrderBook lấy orderbook snapshot mới nhất (last = giá trị gần nhất)
func (r *influxRepo) QueryOKXOrderBook(ctx context.Context, instID string) (*models.OrderBookSnapshot, error) {
	fluxQuery := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "okx_orderbook")
  |> filter(fn: (r) => r.instId == "%s")
  |> last()
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")`,
		r.bucket, instID,
	)

	result, err := r.queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("influxdb query orderbook: %w", err)
	}
	defer result.Close()

	if result.Next() {
		record := result.Record()
		ob := &models.OrderBookSnapshot{
			InstID:    instID,
			Timestamp: record.Time(),
		}
		// Bid/Ask levels được store như JSON string trong InfluxDB
		if v, ok := record.ValueByKey("bids").(string); ok {
			ob.BidsJSON = v
		}
		if v, ok := record.ValueByKey("asks").(string); ok {
			ob.AsksJSON = v
		}
		return ob, nil
	}
	return nil, fmt.Errorf("no orderbook data found for %s", instID)
}

// QueryFundingRates lấy funding rates của perpetual futures
// LEARN: Funding rate = phí định kỳ giữa long/short holders để neo giá futures với spot
func (r *influxRepo) QueryFundingRates(ctx context.Context, instID string, from, to time.Time) ([]models.FundingRate, error) {
	fluxQuery := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "okx_funding_rate")
  |> filter(fn: (r) => r.instId == "%s")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"])`,
		r.bucket, from.Format(time.RFC3339), to.Format(time.RFC3339), instID,
	)

	result, err := r.queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("influxdb query funding: %w", err)
	}
	defer result.Close()

	var rates []models.FundingRate
	for result.Next() {
		record := result.Record()
		fr := models.FundingRate{
			InstID:    instID,
			Timestamp: record.Time(),
		}
		if v, ok := record.ValueByKey("funding_rate").(float64); ok {
			fr.Rate = v
		}
		if v, ok := record.ValueByKey("next_funding_time").(string); ok {
			fr.NextFundingTime = v
		}
		rates = append(rates, fr)
	}
	return rates, result.Err()
}

// QueryLiquidations lấy dữ liệu thanh lý vị thế (liquidations)
// LEARN: Liquidation = forced close khi margin không đủ — signal quan trọng về market stress
func (r *influxRepo) QueryLiquidations(ctx context.Context, instID string, from, to time.Time) ([]models.Liquidation, error) {
	fluxQuery := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "okx_liquidation")
  |> filter(fn: (r) => r.instId == "%s")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"], desc: true)`,
		r.bucket, from.Format(time.RFC3339), to.Format(time.RFC3339), instID,
	)

	result, err := r.queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("influxdb query liquidation: %w", err)
	}
	defer result.Close()

	var liqList []models.Liquidation
	for result.Next() {
		record := result.Record()
		liq := models.Liquidation{
			InstID:    instID,
			Timestamp: record.Time(),
		}
		if v, ok := record.ValueByKey("size").(float64); ok {
			liq.Size = v
		}
		if v, ok := record.ValueByKey("price").(float64); ok {
			liq.Price = v
		}
		if v, ok := record.ValueByKey("side").(string); ok {
			liq.Side = v
		}
		liqList = append(liqList, liq)
	}
	return liqList, result.Err()
}

// QueryVPSSnapshot lấy market data snapshots của VPS Vietnam
func (r *influxRepo) QueryVPSSnapshot(ctx context.Context, symbol string, from, to time.Time) ([]models.VPSMarketSnapshot, error) {
	fluxQuery := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "vps_market")
  |> filter(fn: (r) => r.symbol == "%s")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"])`,
		r.bucket, from.Format(time.RFC3339), to.Format(time.RFC3339), symbol,
	)

	result, err := r.queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("influxdb query vps_market: %w", err)
	}
	defer result.Close()

	var snapshots []models.VPSMarketSnapshot
	for result.Next() {
		record := result.Record()
		snap := models.VPSMarketSnapshot{
			Symbol:    symbol,
			Timestamp: record.Time(),
		}
		if v, ok := record.ValueByKey("price").(float64); ok {
			snap.Price = v
		}
		if v, ok := record.ValueByKey("volume").(float64); ok {
			snap.Volume = v
		}
		if v, ok := record.ValueByKey("bid").(float64); ok {
			snap.Bid = v
		}
		if v, ok := record.ValueByKey("ask").(float64); ok {
			snap.Ask = v
		}
		if v, ok := record.ValueByKey("change_pct").(float64); ok {
			snap.ChangePct = v
		}
		snapshots = append(snapshots, snap)
	}
	return snapshots, result.Err()
}

// QueryVPSOHLCV lấy OHLCV candles cho cổ phiếu Việt Nam
func (r *influxRepo) QueryVPSOHLCV(ctx context.Context, symbol string, timeframe string, from, to time.Time) ([]models.OHLCV, error) {
	fluxDuration := timeframeToDuration(timeframe)

	fluxQuery := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "vps_market")
  |> filter(fn: (r) => r.symbol == "%s")
  |> filter(fn: (r) => r._field == "price")
  |> aggregateWindow(every: %s, fn: mean, createEmpty: false)`,
		r.bucket, from.Format(time.RFC3339), to.Format(time.RFC3339), symbol, fluxDuration,
	)

	result, err := r.queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("influxdb query vps ohlcv: %w", err)
	}
	defer result.Close()

	var candles []models.OHLCV
	for result.Next() {
		record := result.Record()
		candle := models.OHLCV{
			Symbol:    symbol,
			Timeframe: timeframe,
			Timestamp: record.Time(),
		}
		if v, ok := record.Value().(float64); ok {
			candle.Close = v
		}
		candles = append(candles, candle)
	}
	return candles, result.Err()
}

// QueryVPSTopMovers lấy top cổ phiếu tăng/giảm mạnh nhất
// LEARN: Flux sort + limit = ORDER BY ... LIMIT trong SQL
func (r *influxRepo) QueryVPSTopMovers(ctx context.Context, exchange string, limit int) ([]models.TopMover, error) {
	fluxQuery := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: -1d)
  |> filter(fn: (r) => r._measurement == "vps_market")
  |> filter(fn: (r) => r.exchange == "%s")
  |> filter(fn: (r) => r._field == "change_pct")
  |> last()
  |> sort(columns: ["_value"], desc: true)
  |> limit(n: %d)
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")`,
		r.bucket, exchange, limit,
	)

	result, err := r.queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("influxdb query top movers: %w", err)
	}
	defer result.Close()

	var movers []models.TopMover
	for result.Next() {
		record := result.Record()
		mover := models.TopMover{
			Exchange: exchange,
		}
		if v, ok := record.ValueByKey("symbol").(string); ok {
			mover.Symbol = v
		}
		if v, ok := record.ValueByKey("change_pct").(float64); ok {
			mover.ChangePct = v
		}
		if v, ok := record.ValueByKey("price").(float64); ok {
			mover.Price = v
		}
		movers = append(movers, mover)
	}
	return movers, result.Err()
}

// WritePoint ghi 1 data point vào InfluxDB
// LEARN: InfluxDB write model:
//   - measurement = tên "bảng"
//   - tags = indexed labels (dùng để filter, GROUP BY)
//   - fields = actual values (numbers, strings — không index)
//   - timestamp = thời điểm data point
func (r *influxRepo) WritePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time) error {
	p := influxdb2.NewPoint(measurement, tags, fields, ts)
	if err := r.writeAPI.WritePoint(ctx, p); err != nil {
		log.Printf("[InfluxDB] WritePoint error measurement=%s: %v", measurement, err)
		return err
	}
	return nil
}

func (r *influxRepo) Close() {
	r.client.Close()
}

// timeframeToDuration chuyển đổi timeframe string → Flux duration string
// LEARN: Helper function — nhỏ, thuần túy, dễ test đơn lẻ
func timeframeToDuration(tf string) string {
	mapping := map[string]string{
		"1m":  "1m",
		"5m":  "5m",
		"15m": "15m",
		"30m": "30m",
		"1h":  "1h",
		"4h":  "4h",
		"1d":  "1d",
		"1w":  "1w",
	}
	if d, ok := mapping[tf]; ok {
		return d
	}
	return "1h" // default
}
