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
	"fmt"
	"okx-analytics-api/logging"
	"okx-analytics-api/models"
	"okx-analytics-api/repository"
	"strings"
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
	repo   repository.Repository
	logger *logging.Logger
}

func NewService(repo repository.Repository, logger *logging.Logger) Service {
	return &indicatorService{repo: repo, logger: logger}
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

// ComputeSignal tính indicator signal — placeholder, extend với real logic
// LEARN: Tách "tính toán signal" khỏi "lưu config" — single responsibility
func (s *indicatorService) ComputeSignal(groupID uint, symbol, exchange, timeframe string) (*models.IndicatorSignal, error) {
	group, err := s.repo.GetIndicatorGroupDetail(groupID, 0) // 0 = system call, no user filter
	if err != nil {
		return nil, fmt.Errorf("get indicator group: %w", err)
	}

	// Placeholder: thực tế sẽ fetch price data từ InfluxDB và tính toán
	// VD: nếu group có MA(20) indicator → fetch 20 candles → tính average
	signal := &models.IndicatorSignal{
		GroupID:   groupID,
		Symbol:    symbol,
		Exchange:  exchange,
		SignalType: "NEUTRAL",
		Confidence: 0.5,
		Timeframe: timeframe,
	}

	_ = group // dùng trong real implementation
	return signal, nil
}
