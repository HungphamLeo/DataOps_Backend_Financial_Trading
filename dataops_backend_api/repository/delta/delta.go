//go:build ignore

// REUSE: NEW — Delta Lake query via DuckDB (no Spark overhead for API queries)
// LEARN: Delta Lake là storage layer trên top của Parquet files + transaction log
//
// Tại sao Delta Lake?
//   - ACID transactions: không bị corrupt khi write và read cùng lúc
//   - Time travel: đọc data ở thời điểm bất kỳ trong quá khứ
//   - Schema evolution: thêm columns mà không cần rewrite toàn bộ data
//   - Compaction: gộp nhiều file nhỏ thành file lớn để đọc nhanh hơn
//
// DuckDB: SQL engine chạy in-process (không cần server riêng)
//   - Đọc Parquet/Delta files trực tiếp trên MinIO/S3
//   - Phù hợp cho API queries (single-user, low concurrency)
//   - Spark phù hợp hơn cho batch processing (nhiều dữ liệu, nhiều users)

package delta

import (
	"context"
	"database/sql"
	"fmt"
	"dataops-backend-api/configuration"
	"dataops-backend-api/models"
	"time"

	// DuckDB driver cho database/sql standard interface
	// LEARN: database/sql là standard interface — swap driver mà không sửa code
	_ "github.com/marcboeker/go-duckdb"
)

// Repository interface cho Delta Lake queries
type Repository interface {
	QueryHistoricalTrades(ctx context.Context, instID string, from, to time.Time, limit int) ([]models.BinanceTradeTick, error)
	QueryDailySummary(ctx context.Context, instID string, date time.Time) (*models.DailySummary, error)
	QueryVPSDailyOHLCV(ctx context.Context, symbol string, from, to time.Time) ([]models.OHLCV, error)
	Close() error
}

// deltaRepo kết nối DuckDB và trỏ vào MinIO storage
type deltaRepo struct {
	db       *sql.DB
	s3Bucket string
	s3Prefix string
}

// NewRepository khởi tạo DuckDB với S3/MinIO extension
// LEARN: DuckDB có thể đọc file từ S3 bằng httpfs extension
func NewRepository(cfg *configuration.Config) (Repository, error) {
	// Mở DuckDB in-memory (":memory:") — không ghi file xuống disk
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	// Cài đặt extensions và cấu hình S3/MinIO
	// LEARN: DuckDB extensions = plugins thêm tính năng (httpfs cho S3, delta cho Delta Lake)
	setupSQL := fmt.Sprintf(`
INSTALL httpfs;
LOAD httpfs;
INSTALL delta;
LOAD delta;

SET s3_endpoint='%s';
SET s3_access_key_id='%s';
SET s3_secret_access_key='%s';
SET s3_region='us-east-1';
SET s3_use_ssl=false;
SET s3_url_style='path';`,
		cfg.MinIO.Endpoint,
		cfg.MinIO.AccessKey,
		cfg.MinIO.SecretKey,
	)

	if _, err := db.ExecContext(context.Background(), setupSQL); err != nil {
		return nil, fmt.Errorf("setup duckdb s3: %w", err)
	}

	return &deltaRepo{
		db:       db,
		s3Bucket: cfg.MinIO.Bucket,
		s3Prefix: "delta",
	}, nil
}

// deltaPath tạo S3 path đến Delta table
// Pattern: s3://bucket/delta/layer/table_name
func (r *deltaRepo) deltaPath(layer, table string) string {
	return fmt.Sprintf("s3://%s/%s/%s/%s", r.s3Bucket, r.s3Prefix, layer, table)
}

// QueryHistoricalTrades đọc trade data từ Delta Lake Bronze layer
// LEARN: Bronze layer = raw data, chưa transform, giữ nguyên format từ Kafka
func (r *deltaRepo) QueryHistoricalTrades(ctx context.Context, instID string, from, to time.Time, limit int) ([]models.BinanceTradeTick, error) {
	query := fmt.Sprintf(`
SELECT
    ts AS timestamp,
    inst_id,
    price,
    size,
    side
FROM delta_scan('%s')
WHERE inst_id = $1
  AND ts >= $2
  AND ts <= $3
ORDER BY ts ASC
LIMIT $4`,
		r.deltaPath("bronze", "binance_trades"),
	)

	// LEARN: QueryContext với $1, $2... = parameterized query, tránh SQL injection
	rows, err := r.db.QueryContext(ctx, query, instID, from.UnixMilli(), to.UnixMilli(), limit)
	if err != nil {
		return nil, fmt.Errorf("delta query historical trades: %w", err)
	}
	defer rows.Close()

	var trades []models.BinanceTradeTick
	for rows.Next() {
		var (
			tsMs  int64
			trade models.BinanceTradeTick
		)
		// LEARN: rows.Scan map columns vào Go variables theo thứ tự SELECT
		if err := rows.Scan(&tsMs, &trade.InstID, &trade.Price, &trade.Size, &trade.Side); err != nil {
			return nil, fmt.Errorf("scan trade row: %w", err)
		}
		trade.Timestamp = time.UnixMilli(tsMs)
		trades = append(trades, trade)
	}
	return trades, rows.Err()
}

// QueryDailySummary lấy tóm tắt thống kê trong ngày cho 1 instrument
// LEARN: Silver layer = đã aggregate, clean, ready-to-use
func (r *deltaRepo) QueryDailySummary(ctx context.Context, instID string, date time.Time) (*models.DailySummary, error) {
	dayStart := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.Add(24 * time.Hour)

	query := fmt.Sprintf(`
SELECT
    inst_id,
    COUNT(*) AS trade_count,
    MIN(price) AS price_low,
    MAX(price) AS price_high,
    FIRST(price ORDER BY ts ASC) AS price_open,
    LAST(price ORDER BY ts ASC) AS price_close,
    SUM(size) AS total_volume,
    SUM(CASE WHEN side='buy' THEN size ELSE 0 END) AS buy_volume,
    SUM(CASE WHEN side='sell' THEN size ELSE 0 END) AS sell_volume,
    SUM(price * size) / SUM(size) AS vwap
FROM delta_scan('%s')
WHERE inst_id = $1
  AND ts >= $2
  AND ts < $3
GROUP BY inst_id`,
		r.deltaPath("bronze", "binance_trades"),
	)

	row := r.db.QueryRowContext(ctx, query, instID, dayStart.UnixMilli(), dayEnd.UnixMilli())

	summary := &models.DailySummary{
		InstID: instID,
		Date:   date.UnixMilli(),
	}
	err := row.Scan(
		&summary.InstID,
		&summary.TradeCount,
		&summary.PriceLow,
		&summary.PriceHigh,
		&summary.PriceOpen,
		&summary.PriceClose,
		&summary.Volume,
		&summary.BuyVolume,
		&summary.SellVolume,
		&summary.VWAP,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("no data for %s on %s", instID, date.Format("2006-01-02"))
	}
	return summary, err
}

// QueryVPSDailyOHLCV lấy OHLCV hàng ngày cho cổ phiếu VPS
func (r *deltaRepo) QueryVPSDailyOHLCV(ctx context.Context, symbol string, from, to time.Time) ([]models.OHLCV, error) {
	query := fmt.Sprintf(`
SELECT
    DATE_TRUNC('day', to_timestamp(ts/1000)) AS date,
    symbol,
    FIRST(price ORDER BY ts) AS open,
    MAX(price) AS high,
    MIN(price) AS low,
    LAST(price ORDER BY ts) AS close,
    SUM(volume) AS volume
FROM delta_scan('%s')
WHERE symbol = $1
  AND ts >= $2
  AND ts <= $3
GROUP BY 1, 2
ORDER BY 1 ASC`,
		r.deltaPath("bronze", "vps_market"),
	)

	rows, err := r.db.QueryContext(ctx, query, symbol, from.UnixMilli(), to.UnixMilli())
	if err != nil {
		return nil, fmt.Errorf("delta query vps ohlcv: %w", err)
	}
	defer rows.Close()

	var candles []models.OHLCV
	for rows.Next() {
		var (
			dateStr string
			candle  models.OHLCV
		)
		if err := rows.Scan(&dateStr, &candle.Symbol, &candle.Open, &candle.High, &candle.Low, &candle.Close, &candle.Volume); err != nil {
			return nil, fmt.Errorf("scan ohlcv row: %w", err)
		}
		candle.Interval = "1d"
		// Parse date string về UnixMilli
		t, _ := time.Parse("2006-01-02", dateStr[:10])
		candle.Timestamp = t.UnixMilli()
		candles = append(candles, candle)
	}
	return candles, rows.Err()
}

func (r *deltaRepo) Close() error {
	return r.db.Close()
}
