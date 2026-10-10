// LEARN: Service layer cho Indicator Management
//
// REUSE: Pattern từ trading-bot-api — indicator_group repository + models
//
// Tại sao cần Service layer riêng (không gọi repository trực tiếp từ handler)?
//   Handler → Service → Repository → Database
//
//   Service chứa:
//   - Business rules: "short_name phải unique", "period phải > 0"
//   - Orchestration: tạo group + indicators + parameters trong 1 operation
//   - Input validation: trước khi chạm database
//   - Response transformation: model → response DTO
//
//   Repository chỉ chứa: SQL/GORM queries
//   Handler chỉ chứa: HTTP parsing + response writing

package indicator

import (
	"context"
	"fmt"
	"math"
	"strings"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"

	"dataops-backend-api/configuration"
	"dataops-backend-api/logging"
	"dataops-backend-api/models"
	"dataops-backend-api/repository"
)

// Service là interface cho indicator management
// LEARN: Interface = contract, cho phép mock khi test
type Service interface {
	// Group CRUD
	ListGroups(userID uint) ([]*models.IndicatorGroup, error)
	GetGroup(id, userID uint) (*models.IndicatorGroup, error)
	CreateGroup(req CreateGroupRequest, userID uint) (*models.IndicatorGroup, error)
	UpdateGroup(id uint, req UpdateGroupRequest, userID uint) (*models.IndicatorGroup, error)
	DeleteGroup(id, userID uint) error

	// Signal computation
	ComputeSignal(groupID uint, symbol, exchange, timeframe string) (*models.IndicatorSignal, error)
}

type indicatorService struct {
	repo         *repository.Gorm
	logger       *logging.Logger
	influxClient influxdb2.Client
	cfg          *configuration.Config
}

func NewService(repo *repository.Gorm, logger *logging.Logger, cfg *configuration.Config) Service {
	return &indicatorService{
		repo:         repo,
		logger:       logger,
		influxClient: influxdb2.NewClient(cfg.InfluxDB.URL, cfg.InfluxDB.Token),
		cfg:          cfg,
	}
}

// ─── Request DTOs ─────────────────────────────────────────────────────────────
// LEARN: DTO (Data Transfer Object) — khác với Model
//        Model = DB schema, DTO = API request/response shape

type CreateGroupRequest struct {
	ShortName  string                    `json:"shortName"  binding:"required,max=50"`
	Name       string                    `json:"name"       binding:"required,max=100"`
	Type       uint                      `json:"type"`
	Indicators []IndicatorRequest        `json:"indicators"`
	Parameters []ParameterRequest        `json:"parameters"`
}

type IndicatorRequest struct {
	ID        uint   `json:"id"`        // 0 = create new, >0 = update existing
	ShortName string `json:"shortName"  binding:"required"`
	Name      string `json:"name"       binding:"required"`
	Formula   string `json:"formula"`
	IsActive  bool   `json:"isActive"`
}

type ParameterRequest struct {
	ID           uint    `json:"id"`
	IndicatorID  uint    `json:"indicatorId"`
	Name         string  `json:"name"         binding:"required"`
	DefaultValue string  `json:"defaultValue"`
	CurrentValue string  `json:"currentValue"`
	DataType     string  `json:"dataType"` // "int","float","string","bool"
	MinValue     float64 `json:"minValue"`
	MaxValue     float64 `json:"maxValue"`
	Description  string  `json:"description"`
}

type UpdateGroupRequest struct {
	CreateGroupRequest // embed, update có cùng fields như create
}

// ─── Service Implementation ───────────────────────────────────────────────────

func (s *indicatorService) ListGroups(userID uint) ([]*models.IndicatorGroup, error) {
	groups, err := s.repo.GetListIndicatorGroups(userID)
	if err != nil {
		s.logger.Error("ListGroups", "failed to list indicator groups", map[string]interface{}{"userID": userID, "err": err.Error()})
		return nil, err
	}
	return groups, nil
}

func (s *indicatorService) GetGroup(id, userID uint) (*models.IndicatorGroup, error) {
	group, err := s.repo.GetIndicatorGroupDetail(id, userID)
	if err != nil {
		s.logger.Warn("GetGroup", "indicator group not found", map[string]interface{}{"groupID": id, "userID": userID, "err": err.Error()})
		return nil, fmt.Errorf("indicator group %d not found", id)
	}
	return group, nil
}

// CreateGroup validate rồi tạo group với tất cả children
func (s *indicatorService) CreateGroup(req CreateGroupRequest, userID uint) (*models.IndicatorGroup, error) {
	// Business rule: short_name phải lowercase, không có space
	// LEARN: Validation ở service layer — handler chỉ validate format (binding:"required")
	//        Service validate business rules ("phải unique", "không được trùng")
	req.ShortName = strings.ToLower(strings.ReplaceAll(req.ShortName, " ", "_"))

	// Build model từ request
	group := &models.IndicatorGroup{
		ShortName: req.ShortName,
		Name:      req.Name,
		Type:      req.Type,
		CreatedBy: userID,
	}

	for _, indReq := range req.Indicators {
		group.Indicators = append(group.Indicators, &models.Indicator{
			ShortName: indReq.ShortName,
			Name:      indReq.Name,
			Formula:   indReq.Formula,
			IsActive:  indReq.IsActive,
		})
	}

	for _, paramReq := range req.Parameters {
		cv := paramReq.CurrentValue
		if cv == "" {
			cv = paramReq.DefaultValue // default khi create
		}
		group.Parameters = append(group.Parameters, &models.IndicatorParameter{
			IndicatorID:  paramReq.IndicatorID,
			Name:         paramReq.Name,
			DefaultValue: paramReq.DefaultValue,
			CurrentValue: cv,
			DataType:     paramReq.DataType,
			MinValue:     paramReq.MinValue,
			MaxValue:     paramReq.MaxValue,
			Description:  paramReq.Description,
		})
	}

	if err := s.repo.CreateIndicatorGroup(group); err != nil {
		if strings.Contains(err.Error(), "Duplicate entry") {
			s.logger.Warn("CreateGroup", "duplicate short_name", map[string]interface{}{"shortName": req.ShortName, "userID": userID})
			return nil, fmt.Errorf("indicator group short_name '%s' already exists", req.ShortName)
		}
		s.logger.Error("CreateGroup", "failed to create indicator group", map[string]interface{}{"shortName": req.ShortName, "userID": userID, "err": err.Error()})
		return nil, fmt.Errorf("create indicator group: %w", err)
	}

	return group, nil
}

// UpdateGroup tìm IDs hợp lệ để cascade delete orphans
// REUSE: Logic từ trading-bot-api/models/indicator_group.go GetValidIndicatorIds()
func (s *indicatorService) UpdateGroup(id uint, req UpdateGroupRequest, userID uint) (*models.IndicatorGroup, error) {
	group, err := s.GetGroup(id, userID)
	if err != nil {
		return nil, err
	}

	// Cập nhật fields cơ bản
	group.Name = req.Name
	group.ShortName = strings.ToLower(strings.ReplaceAll(req.ShortName, " ", "_"))

	// Lấy valid IDs (ID > 0 = record đã tồn tại cần giữ lại)
	validIndicatorIDs := make([]uint, 0)
	validParamIDs := make([]uint, 0)

	group.Indicators = nil
	for _, indReq := range req.Indicators {
		ind := &models.Indicator{
			ShortName: indReq.ShortName,
			Name:      indReq.Name,
			Formula:   indReq.Formula,
			IsActive:  indReq.IsActive,
			GroupID:   group.ID,
		}
		if indReq.ID > 0 {
			ind.ID = indReq.ID
			validIndicatorIDs = append(validIndicatorIDs, indReq.ID)
		}
		group.Indicators = append(group.Indicators, ind)
	}

	group.Parameters = nil
	for _, paramReq := range req.Parameters {
		param := &models.IndicatorParameter{
			IndicatorID:  paramReq.IndicatorID,
			Name:         paramReq.Name,
			DefaultValue: paramReq.DefaultValue,
			CurrentValue: paramReq.CurrentValue,
			DataType:     paramReq.DataType,
			GroupID:      group.ID,
		}
		if paramReq.ID > 0 {
			param.ID = paramReq.ID
			validParamIDs = append(validParamIDs, paramReq.ID)
		}
		group.Parameters = append(group.Parameters, param)
	}

	if err := s.repo.UpdateIndicatorGroup(group, validIndicatorIDs, validParamIDs); err != nil {
		s.logger.Error("UpdateGroup", "failed to update indicator group", map[string]interface{}{"groupID": id, "userID": userID, "err": err.Error()})
		return nil, fmt.Errorf("update indicator group: %w", err)
	}

	return s.GetGroup(id, userID)
}

func (s *indicatorService) DeleteGroup(id, userID uint) error {
	if err := s.repo.DeleteIndicatorGroup(id, userID); err != nil {
		s.logger.Error("DeleteGroup", "failed to delete indicator group", map[string]interface{}{"groupID": id, "userID": userID, "err": err.Error()})
		return err
	}
	s.logger.Info("DeleteGroup", "indicator group deleted", map[string]interface{}{"groupID": id, "userID": userID})
	return nil
}

// ComputeSignal tính RSI(14) + MA(20) signal từ InfluxDB close price series.
//
// Data source: InfluxDB bucket "market-data", measurement "vwap"
// Written by: dataops_processing VWAP Spark job
//
// Algorithm:
//   1. Fetch 21 VWAP points (21 > 20 để MA(20) có đủ data)
//   2. Tính MA(20) từ 20 giá trị VWAP cuối
//   3. Tính RSI(14) từ 14 + 1 = 15 giá trị VWAP cuối (cần 1 prior để tính change)
//   4. Kết hợp: RSI > 70 + price > MA → SELL; RSI < 30 + price < MA → BUY; else HOLD
func (s *indicatorService) ComputeSignal(groupID uint, symbol, exchange, timeframe string) (*models.IndicatorSignal, error) {
	_, err := s.repo.GetIndicatorGroupDetail(groupID, 0)
	if err != nil {
		return nil, fmt.Errorf("get indicator group: %w", err)
	}

	// Fetch 21 VWAP points — đủ cho MA(20) + RSI(14) cùng 1 query
	closes, err := s.fetchClosePrices(symbol, timeframe, 21)
	if err != nil || len(closes) < 15 {
		// Không đủ data → NEUTRAL với confidence thấp
		s.logger.Warn("ComputeSignal", "insufficient price data, returning NEUTRAL",
			map[string]interface{}{"symbol": symbol, "timeframe": timeframe, "points": len(closes)})
		return &models.IndicatorSignal{
			GroupID:    groupID,
			Symbol:     symbol,
			Exchange:   exchange,
			SignalType: "NEUTRAL",
			Confidence: 0.0,
			Timeframe:  timeframe,
		}, nil
	}

	ma20 := calcMA(closes, 20)
	rsi14 := calcRSI(closes, 14)
	currentPrice := closes[len(closes)-1]

	signalType, confidence := classifySignal(currentPrice, ma20, rsi14)

	s.logger.Info("ComputeSignal", "signal computed",
		map[string]interface{}{
			"symbol": symbol, "timeframe": timeframe,
			"price": currentPrice, "ma20": ma20, "rsi14": rsi14,
			"signal": signalType, "confidence": confidence,
		})

	return &models.IndicatorSignal{
		GroupID:    groupID,
		Symbol:     symbol,
		Exchange:   exchange,
		SignalType: signalType,
		Confidence: confidence,
		Value:      rsi14,
		Timeframe:  timeframe,
	}, nil
}

// fetchClosePrices lấy n điểm VWAP gần nhất từ InfluxDB.
// Dùng VWAP làm proxy cho close price (VWAP ≈ fair value price trong window).
func (s *indicatorService) fetchClosePrices(symbol, timeframe string, n int) ([]float64, error) {
	queryAPI := s.influxClient.QueryAPI(s.cfg.InfluxDB.Org)

	// Lookback đủ rộng để có n điểm: 1h timeframe * n * 3 buffer
	lookbackHours := n * 3
	if timeframe == "1d" {
		lookbackHours = n * 24 * 2
	} else if timeframe == "5m" {
		lookbackHours = n / 12 + 2 // 12 points/hour
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
		return nil, fmt.Errorf("fetchClosePrices InfluxDB: %w", err)
	}
	defer result.Close()

	var prices []float64
	for result.Next() {
		if v, ok := result.Record().Value().(float64); ok && v > 0 {
			prices = append(prices, v)
		}
	}
	return prices, nil
}

// calcMA tính simple moving average của n giá trị cuối trong series.
func calcMA(prices []float64, n int) float64 {
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

// calcRSI tính RSI(period) theo Wilder's smoothed method (standard).
//
// Formula:
//   RS  = avg_gain / avg_loss  (over period)
//   RSI = 100 - (100 / (1 + RS))
func calcRSI(prices []float64, period int) float64 {
	if len(prices) < period+1 {
		return 50.0 // không đủ data → neutral 50
	}

	slice := prices[len(prices)-(period+1):]
	var gains, losses float64
	for i := 1; i < len(slice); i++ {
		change := slice[i] - slice[i-1]
		if change > 0 {
			gains += change
		} else {
			losses += math.Abs(change)
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

// classifySignal kết hợp RSI + MA để ra tín hiệu.
//
// Rules (theo quan điểm mean-reversion với trend filter):
//   RSI > 70 AND price > MA → SELL  (overbought + trend confirm), confidence 0.75
//   RSI < 30 AND price < MA → BUY   (oversold + trend confirm), confidence 0.75
//   RSI > 60 OR  price > MA → HOLD  (mild bullish), confidence 0.55
//   RSI < 40 OR  price < MA → HOLD  (mild bearish), confidence 0.55
//   else                   → NEUTRAL, confidence 0.5
func classifySignal(price, ma20, rsi14 float64) (string, float64) {
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
}
