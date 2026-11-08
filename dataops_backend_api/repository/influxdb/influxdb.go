// Package influxdb — InfluxDB v2 Repository cho dataops-backend-api.
//
// ═══════════════════════════════════════════════════════════════════════════════
// ST06 — INFLUXDB BUFFER POOL DEFENSE + CACHE-ASIDE PATTERN (PLAN_REPO03)
// ═══════════════════════════════════════════════════════════════════════════════
//
// VẤN ĐỀ — Buffer Pool LRU Pollution:
//   InfluxDB (như mọi time-series DB) quản lý Data Pages trong RAM qua thuật toán LRU.
//   Khi API dashboard chạy query phân tích lịch sử 30 ngày (large scan):
//     → Kéo hàng GB pages từ disk lên RAM.
//     → Đẩy sạch "hot pages" (VWAP 5 phút gần nhất) ra khỏi RAM.
//     → Query real-time API tiếp theo phải đọc lại từ disk → latency tăng 10-100x.
//   Đây là hiện tượng "Cache Eviction Pollution" — analytics query làm hỏng OLTP cache.
//
// GIẢI PHÁP — Redis Cache-Aside (PACELC: EL - Latency Optimization):
//   Trong điều kiện mạng bình thường (Else), ưu tiên giảm Latency (L):
//     1. API nhận request: check Redis với key "vwap:{symbol}:{interval}:{ts_bucket}".
//     2. Cache HIT: trả về ngay từ RAM Redis (<1ms). Không đụng InfluxDB.
//     3. Cache MISS: query InfluxDB → cache kết quả với TTL=30s → trả về client.
//   Target: Cache Hit Ratio > 80% cho VWAP queries (dashboard refresh mỗi 5-30s).
//
// KEY DESIGN (deterministic & cache-friendly):
//   "vwap:{symbol}:{interval}:{timestamp_bucket}"
//   timestamp_bucket = Unix epoch / TTL_seconds → cùng bucket trong 1 TTL window.
//   Ví dụ: TTL=30s, ts=1700000060 → bucket = 1700000060/30 = 56666668.
//   → Tất cả requests trong 30s sẽ hit cùng cache key → high hit ratio.
//
// CARDINALITY WARNING:
//   InfluxDB có giới hạn Time Series Cardinality (~1-10M series tùy phiên bản).
//   Cardinality = unique combinations của (measurement, tags).
//   Nếu thêm user_id vào tag → cardinality × num_users → RAM InfluxDB cạn → crash.
//   Rule: KHÔNG BAO GIỜ dùng high-cardinality fields (UUID, user_id) làm InfluxDB tag.
//   Monitor: influxdb_tsdb_series_total > 500K → alert → cleanup old series.
//
// REPOSITORY PATTERN:
//   Services không gọi InfluxDB trực tiếp — chỉ qua interface Repository.
//   Cho phép: swap implementation (mock trong test), thêm cache layer sau mà không
//   thay đổi business logic, dễ add observability (metrics, tracing) ở 1 chỗ.
//
// So sánh với MySQL:
//   MySQL: Database → Table → Row → Columns
//   InfluxDB: Org → Bucket → Measurement → (Fields=values, Tags=indexed labels)
//   Flux query language: functional pipeline — from() |> range() |> filter() |> aggregateWindow()

package influxdb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"dataops-backend-api/configuration"
	"dataops-backend-api/models"
	"dataops-backend-api/redis"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

// Repository định nghĩa tất cả các queries vào InfluxDB
// LEARN: Interface pattern — services gọi Repository.Method(), không gọi InfluxDB trực tiếp
type Repository interface {
	// Binance Crypto queries
	QueryBinanceTrades(ctx context.Context, instID string, from, to time.Time) ([]models.BinanceTradeTick, error)
	QueryBinanceOHLCV(ctx context.Context, instID string, timeframe string, from, to time.Time) ([]models.OHLCV, error)
	QueryBinanceOrderBook(ctx context.Context, instID string) (*models.OrderBookSnapshot, error)
	QueryFundingRates(ctx context.Context, instID string, from, to time.Time) ([]models.FundingRate, error)
	QueryLiquidations(ctx context.Context, instID string, from, to time.Time) ([]models.Liquidation, error)
	QueryVWAP(ctx context.Context, symbol, interval string, from, to time.Time) ([]models.VWAPPoint, error)

	// TCBS Vietnam Stock queries
	QueryTCBSTopMovers(ctx context.Context, exchange string, limit int) ([]models.TopMover, error)

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

type CacheAwareInfluxRepository struct {
	repo     Repository
	redis    redis.Service
	cacheTTL time.Duration
}

func NewCacheAwareInfluxRepository(repo Repository, r redis.Service, ttl time.Duration) *CacheAwareInfluxRepository {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &CacheAwareInfluxRepository{
		repo:     repo,
		redis:    r,
		cacheTTL: ttl,
	}
}

func (r *CacheAwareInfluxRepository) QueryVWAP(
	ctx context.Context,
	symbol string,
	interval string,
	from time.Time,
	to time.Time,
) ([]models.VWAPPoint, error) {
	if r.cacheTTL <= 0 {
		r.cacheTTL = 30 * time.Second
	}

	key := fmt.Sprintf("vwap:%s:%s:%d",
		strings.ToUpper(symbol),
		interval,
		from.Unix()/int64(r.cacheTTL.Seconds()),
	)

	if r.redis != nil {
		cached := r.redis.Get(key)
		if cached != "" {
			var out []models.VWAPPoint
			if err := json.Unmarshal([]byte(cached), &out); err == nil && len(out) > 0 {
				return out, nil
			}
		}
	}

	vwap, err := r.repo.QueryVWAP(ctx, symbol, interval, from, to)
	if err != nil {
		return nil, err
	}

	if r.redis != nil && len(vwap) > 0 {
		payload, err := json.Marshal(vwap)
		if err == nil {
			_ = r.redis.Set(key, string(payload), r.cacheTTL)
		}
	}

	return vwap, nil
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

// QueryBinanceTrades truy vấn trade ticks từ InfluxDB bằng Flux query language
// LEARN: Flux query — functional data pipeline:
//   from(bucket:...) → range(start:...) → filter(fn:...) → pivot → yield
func (r *influxRepo) QueryBinanceTrades(ctx context.Context, instID string, from, to time.Time) ([]models.BinanceTradeTick, error) {
	// LEARN: Heredoc-style string trong Go dùng backtick (`)
	// %s là format verbs — fmt.Sprintf thay thế %s bằng giá trị thực
	fluxQuery := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "binance_trades")
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
		return nil, fmt.Errorf("influxdb query binance_trades: %w", err)
	}
	defer result.Close()

	var trades []models.BinanceTradeTick
	// LEARN: result.Next() là iterator pattern — duyệt kết quả từng dòng
	for result.Next() {
		record := result.Record()
		trade := models.BinanceTradeTick{
			InstID:    instID,
			Timestamp: record.Time().UnixMilli(),
		}

		// LEARN: type assertion với ok-check (safe) vs tanpa ok (panic nếu sai type)
		if v, ok := record.ValueByKey("price").(float64); ok {
			trade.Price = v
		}
		if v, ok := record.ValueByKey("quantity").(float64); ok {
			trade.Quantity = v
		}
		if v, ok := record.ValueByKey("side").(string); ok {
			trade.Side = v
		}
		trades = append(trades, trade)
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("influxdb iterate binance_trades: %w", result.Err())
	}

	return trades, nil
}

// QueryBinanceOHLCV tính OHLCV (Open/High/Low/Close/Volume) bằng Flux aggregation
// LEARN: Flux window + aggregateWindow = resample time-series vào nến (candles)
func (r *influxRepo) QueryBinanceOHLCV(ctx context.Context, instID string, timeframe string, from, to time.Time) ([]models.OHLCV, error) {
	// timeframe: "1m", "5m", "1h", "1d" → chuyển sang Flux duration
	fluxDuration := timeframeToDuration(timeframe)

	fluxQuery := fmt.Sprintf(`
priceData = from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "binance_trades")
  |> filter(fn: (r) => r.instId == "%s")
  |> filter(fn: (r) => r._field == "price")

open = priceData |> aggregateWindow(every: %s, fn: first, createEmpty: false) |> rename(columns: {_value: "open"})
high = priceData |> aggregateWindow(every: %s, fn: max,   createEmpty: false) |> rename(columns: {_value: "high"})
low  = priceData |> aggregateWindow(every: %s, fn: min,   createEmpty: false) |> rename(columns: {_value: "low"})
close = priceData |> aggregateWindow(every: %s, fn: last,  createEmpty: false) |> rename(columns: {_value: "close"})

volume = from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "binance_trades")
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
			Interval: timeframe,
			Timestamp: record.Time().UnixMilli(),
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

// QueryBinanceOrderBook lấy orderbook snapshot mới nhất (last = giá trị gần nhất)
func (r *influxRepo) QueryBinanceOrderBook(ctx context.Context, instID string) (*models.OrderBookSnapshot, error) {
	fluxQuery := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "binance_orderbook")
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
			Timestamp: record.Time().UnixMilli(),
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
  |> filter(fn: (r) => r._measurement == "binance_funding_rate")
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
			Timestamp: record.Time().UnixMilli(),
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
  |> filter(fn: (r) => r._measurement == "binance_liquidation")
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
			Timestamp: record.Time().UnixMilli(),
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

// QueryTCBSTopMovers lấy top cổ phiếu tăng/giảm mạnh nhất từ TCBS measurement.
func (r *influxRepo) QueryTCBSTopMovers(ctx context.Context, exchange string, limit int) ([]models.TopMover, error) {
	fluxQuery := fmt.Sprintf(`
from(bucket: "%s")
	 |> range(start: -1d)
	 |> filter(fn: (r) => r._measurement == "tcbs_market")
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
