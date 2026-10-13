package tcbsaccount

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"dataops-backend-api/configuration"
	"dataops-backend-api/logging"
	"dataops-backend-api/models"
	"dataops-backend-api/redis"
)

// Service định nghĩa interface cho TCBS Account Proxy
type Service interface {
	GetCashInvestments(ctx context.Context, accountNo string) (*models.TCBSAccountCash, error)
	GetStockAssets(ctx context.Context, accountNo string) ([]*models.TCBSStockAsset, error)
	GetOrders(ctx context.Context, accountNo string) ([]*models.TCBSOrderRecord, error)
	PlaceStockOrder(ctx context.Context, req *models.TCBSPlaceStockOrderRequest) (*models.TCBSPlaceStockOrderResponse, error)
	CancelStockOrder(ctx context.Context, accountNo, orderNo string) (*models.TCBSCancelStockOrderResponse, error)
	ModifyStockOrder(ctx context.Context, req *models.TCBSModifyStockOrderRequest) (*models.TCBSModifyStockOrderResponse, error)
	GetDerivativePositions(ctx context.Context, accountID string) ([]*models.TCBSDerivativePositionRecord, error)
	PlaceDerivativeOrder(ctx context.Context, req *models.TCBSPlaceDerivOrderRequest) (*models.TCBSPlaceDerivOrderResponse, error)
	CancelDerivativeOrder(ctx context.Context, accountNo, orderNo string) (*models.TCBSCancelDerivOrderResponse, error)
}

type tcbsAccountService struct {
	cfg        *configuration.Config
	redis      redis.Service
	logger     *logging.Logger
	httpClient *http.Client
}

// NewService khởi tạo service quản lý tài khoản và đặt lệnh TCBS
func NewService(cfg *configuration.Config, redisSvc redis.Service, logger *logging.Logger) Service {
	return &tcbsAccountService{
		cfg:    cfg,
		redis:  redisSvc,
		logger: logger,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (s *tcbsAccountService) setAuthHeaders(req *http.Request) {
	if s.cfg.TCBS.JWTToken != "" {
		req.Header.Set("Authorization", "Bearer "+s.cfg.TCBS.JWTToken)
	}
	if s.cfg.TCBS.APIKey != "" {
		req.Header.Set("apiKey", s.cfg.TCBS.APIKey)
	}
	req.Header.Set("Content-Type", "application/json")
}

// ─── GetCashInvestments ───────────────────────────────────────────────────────
func (s *tcbsAccountService) GetCashInvestments(ctx context.Context, accountNo string) (*models.TCBSAccountCash, error) {
	cacheKey := fmt.Sprintf("tcbs_account_%s_cash", accountNo)
	if s.redis != nil {
		cached := s.redis.Get(cacheKey)
		if cached != "" {
			var cash models.TCBSAccountCash
			if err := json.Unmarshal([]byte(cached), &cash); err == nil {
				return &cash, nil
			}
		}
	}

	url := fmt.Sprintf("%s/aion/v1/accounts/%s/cashInvestments", s.cfg.TCBS.BaseURL, accountNo)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create cashInvestments request: %w", err)
	}
	s.setAuthHeaders(req)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Error("TCBSAccount.GetCashInvestments", "HTTP request failed",
			map[string]interface{}{"accountNo": accountNo, "err": err.Error()})
		return nil, fmt.Errorf("execute cashInvestments request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("TCBS upstream error (status %d): %s", resp.StatusCode, string(body))
	}

	var result struct {
		Cash            float64 `json:"cash"`
		PurchasingPower float64 `json:"purchasingPower"`
		DepositAmt      float64 `json:"depositAmt"`
		WithdrawableAmt float64 `json:"withdrawableAmt"`
		MarginRatio     float64 `json:"marginRatio"`
		Nav             float64 `json:"nav"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode cashInvestments response: %w", err)
	}

	cash := &models.TCBSAccountCash{
		AccountNo:       accountNo,
		Cash:            result.Cash,
		PurchasingPower: result.PurchasingPower,
		DepositAmt:      result.DepositAmt,
		WithdrawableAmt: result.WithdrawableAmt,
		MarginRatio:     result.MarginRatio,
		Nav:             result.Nav,
		UpdatedAt:       time.Now().Unix(),
	}

	if s.redis != nil {
		_ = s.redis.Set(cacheKey, cash, 30*time.Second)
	}

	return cash, nil
}

// ─── GetStockAssets ───────────────────────────────────────────────────────────
func (s *tcbsAccountService) GetStockAssets(ctx context.Context, accountNo string) ([]*models.TCBSStockAsset, error) {
	cacheKey := fmt.Sprintf("tcbs_account_%s_stock_asset", accountNo)
	if s.redis != nil {
		cached := s.redis.Get(cacheKey)
		if cached != "" {
			var assets []*models.TCBSStockAsset
			if err := json.Unmarshal([]byte(cached), &assets); err == nil {
				return assets, nil
			}
		}
	}

	url := fmt.Sprintf("%s/aion/v1/accounts/%s/se", s.cfg.TCBS.BaseURL, accountNo)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create stock assets request: %w", err)
	}
	s.setAuthHeaders(req)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Error("TCBSAccount.GetStockAssets", "HTTP request failed",
			map[string]interface{}{"accountNo": accountNo, "err": err.Error()})
		return nil, fmt.Errorf("execute stock assets request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("TCBS upstream error (status %d): %s", resp.StatusCode, string(body))
	}

	var rawAssets []struct {
		Symbol           string  `json:"symbol"`
		Quantity         float64 `json:"quantity"`
		AvailableQty     float64 `json:"availableQty"`
		CostPrice        float64 `json:"costPrice"`
		MarketPrice      float64 `json:"marketPrice"`
		MarketValue      float64 `json:"marketValue"`
		UnrealizedPnL    float64 `json:"unrealizedPnL"`
		UnrealizedPnLPct float64 `json:"unrealizedPnLPct"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rawAssets); err != nil {
		return nil, fmt.Errorf("decode stock assets response: %w", err)
	}

	var assets []*models.TCBSStockAsset
	for _, raw := range rawAssets {
		assets = append(assets, &models.TCBSStockAsset{
			AccountNo:        accountNo,
			Symbol:           raw.Symbol,
			Quantity:         raw.Quantity,
			AvailableQty:     raw.AvailableQty,
			CostPrice:        raw.CostPrice,
			MarketPrice:      raw.MarketPrice,
			MarketValue:      raw.MarketValue,
			UnrealizedPnL:    raw.UnrealizedPnL,
			UnrealizedPnLPct: raw.UnrealizedPnLPct,
		})
	}

	if s.redis != nil {
		_ = s.redis.Set(cacheKey, assets, 30*time.Second)
	}

	return assets, nil
}

// ─── GetOrders ────────────────────────────────────────────────────────────────
func (s *tcbsAccountService) GetOrders(ctx context.Context, accountNo string) ([]*models.TCBSOrderRecord, error) {
	// Orders là real-time dữ liệu giao dịch → Không cache Redis
	url := fmt.Sprintf("%s/aion/v1/accounts/%s/orders", s.cfg.TCBS.BaseURL, accountNo)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create orders request: %w", err)
	}
	s.setAuthHeaders(req)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Error("TCBSAccount.GetOrders", "HTTP request failed",
			map[string]interface{}{"accountNo": accountNo, "err": err.Error()})
		return nil, fmt.Errorf("execute orders request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("TCBS upstream error (status %d): %s", resp.StatusCode, string(body))
	}

	var rawOrders []struct {
		OrderNo      string  `json:"orderNo"`
		Symbol       string  `json:"symbol"`
		Side         string  `json:"side"`
		OrderType    string  `json:"orderType"`
		Price        float64 `json:"price"`
		Quantity     float64 `json:"quantity"`
		MatchedQty   float64 `json:"matchedQty"`
		MatchedPrice float64 `json:"matchedPrice"`
		Status       string  `json:"status"`
		OrderTime    string  `json:"orderTime"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rawOrders); err != nil {
		return nil, fmt.Errorf("decode orders response: %w", err)
	}

	var orders []*models.TCBSOrderRecord
	for _, raw := range rawOrders {
		orders = append(orders, &models.TCBSOrderRecord{
			OrderNo:      raw.OrderNo,
			AccountNo:    accountNo,
			Symbol:       raw.Symbol,
			Side:         raw.Side,
			OrderType:    raw.OrderType,
			Price:        raw.Price,
			Quantity:     raw.Quantity,
			MatchedQty:   raw.MatchedQty,
			MatchedPrice: raw.MatchedPrice,
			Status:       raw.Status,
			OrderTime:    raw.OrderTime,
		})
	}

	return orders, nil
}

// ─── PlaceStockOrder ──────────────────────────────────────────────────────────
func (s *tcbsAccountService) PlaceStockOrder(ctx context.Context, reqData *models.TCBSPlaceStockOrderRequest) (*models.TCBSPlaceStockOrderResponse, error) {
	url := fmt.Sprintf("%s/akhlys/v1/accounts/%s/orders", s.cfg.TCBS.BaseURL, reqData.AccountNo)
	payloadBytes, err := json.Marshal(reqData)
	if err != nil {
		return nil, fmt.Errorf("marshal place stock order request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return nil, fmt.Errorf("create place stock order request: %w", err)
	}
	s.setAuthHeaders(req)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Error("TCBSAccount.PlaceStockOrder", "HTTP request failed",
			map[string]interface{}{"accountNo": reqData.AccountNo, "symbol": reqData.Symbol, "err": err.Error()})
		return nil, fmt.Errorf("execute place stock order: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		OrderNo string `json:"orderNo"`
		Error   string `json:"error"`
		Message string `json:"message"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode place stock order response: %w", err)
	}

	status := "SUCCESS"
	if result.Error != "0" && result.Error != "" {
		status = "FAILED"
	}

	return &models.TCBSPlaceStockOrderResponse{
		OrderNo:   result.OrderNo,
		Status:    status,
		ErrorCode: result.Error,
		Message:   result.Message,
	}, nil
}

// ─── CancelStockOrder ─────────────────────────────────────────────────────────
func (s *tcbsAccountService) CancelStockOrder(ctx context.Context, accountNo, orderNo string) (*models.TCBSCancelStockOrderResponse, error) {
	url := fmt.Sprintf("%s/akhlys/v1/accounts/%s/orders/%s", s.cfg.TCBS.BaseURL, accountNo, orderNo)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create cancel stock order request: %w", err)
	}
	s.setAuthHeaders(req)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Error("TCBSAccount.CancelStockOrder", "HTTP request failed",
			map[string]interface{}{"accountNo": accountNo, "orderNo": orderNo, "err": err.Error()})
		return nil, fmt.Errorf("execute cancel stock order: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		OrderNo string `json:"orderNo"`
		Error   string `json:"error"`
		Message string `json:"message"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode cancel stock order response: %w", err)
	}

	status := "SUCCESS"
	if result.Error != "0" && result.Error != "" {
		status = "FAILED"
	}

	return &models.TCBSCancelStockOrderResponse{
		OrderNo:   result.OrderNo,
		Status:    status,
		ErrorCode: result.Error,
		Message:   result.Message,
	}, nil
}

// ─── ModifyStockOrder ─────────────────────────────────────────────────────────
func (s *tcbsAccountService) ModifyStockOrder(ctx context.Context, reqData *models.TCBSModifyStockOrderRequest) (*models.TCBSModifyStockOrderResponse, error) {
	url := fmt.Sprintf("%s/akhlys/v1/accounts/%s/orders/%s", s.cfg.TCBS.BaseURL, reqData.AccountNo, reqData.OrderNo)
	payloadBytes, err := json.Marshal(reqData)
	if err != nil {
		return nil, fmt.Errorf("marshal modify stock order request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return nil, fmt.Errorf("create modify stock order request: %w", err)
	}
	s.setAuthHeaders(req)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Error("TCBSAccount.ModifyStockOrder", "HTTP request failed",
			map[string]interface{}{"accountNo": reqData.AccountNo, "orderNo": reqData.OrderNo, "err": err.Error()})
		return nil, fmt.Errorf("execute modify stock order: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		OrderNo string `json:"orderNo"`
		Error   string `json:"error"`
		Message string `json:"message"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode modify stock order response: %w", err)
	}

	status := "SUCCESS"
	if result.Error != "0" && result.Error != "" {
		status = "FAILED"
	}

	return &models.TCBSModifyStockOrderResponse{
		OrderNo:   result.OrderNo,
		Status:    status,
		ErrorCode: result.Error,
		Message:   result.Message,
	}, nil
}

// ─── GetDerivativePositions ───────────────────────────────────────────────────
func (s *tcbsAccountService) GetDerivativePositions(ctx context.Context, accountID string) ([]*models.TCBSDerivativePositionRecord, error) {
	cacheKey := fmt.Sprintf("tcbs_deriv_positions_%s", accountID)
	if s.redis != nil {
		cached := s.redis.Get(cacheKey)
		if cached != "" {
			var positions []*models.TCBSDerivativePositionRecord
			if err := json.Unmarshal([]byte(cached), &positions); err == nil {
				return positions, nil
			}
		}
	}

	url := fmt.Sprintf("%s/khronos/v1/account/portfolio/status?accountId=%s", s.cfg.TCBS.BaseURL, accountID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create derivative positions request: %w", err)
	}
	s.setAuthHeaders(req)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Error("TCBSAccount.GetDerivativePositions", "HTTP request failed",
			map[string]interface{}{"accountId": accountID, "err": err.Error()})
		return nil, fmt.Errorf("execute derivative positions request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("TCBS upstream error (status %d): %s", resp.StatusCode, string(body))
	}

	var rawPositions []struct {
		SubAccountID string  `json:"subAccountId"`
		Symbol       string  `json:"symbol"`
		Side         string  `json:"side"` // "L" or "S"
		NetQty       float64 `json:"netQty"`
		AvgPrice     float64 `json:"avgPrice"`
		MarketPrice  float64 `json:"marketPrice"`
		FloatingPnL  float64 `json:"floatingPnL"`
		TradingDate  string  `json:"tradingDate"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rawPositions); err != nil {
		return nil, fmt.Errorf("decode derivative positions response: %w", err)
	}

	var positions []*models.TCBSDerivativePositionRecord
	for _, raw := range rawPositions {
		positions = append(positions, &models.TCBSDerivativePositionRecord{
			AccountID:    accountID,
			SubAccountID: raw.SubAccountID,
			Symbol:       raw.Symbol,
			Side:         raw.Side,
			NetQty:       raw.NetQty,
			AvgPrice:     raw.AvgPrice,
			MarketPrice:  raw.MarketPrice,
			FloatingPnL:  raw.FloatingPnL,
			TradingDate:  raw.TradingDate,
		})
	}

	if s.redis != nil {
		_ = s.redis.Set(cacheKey, positions, 10*time.Second)
	}

	return positions, nil
}

// ─── PlaceDerivativeOrder ─────────────────────────────────────────────────────
func (s *tcbsAccountService) PlaceDerivativeOrder(ctx context.Context, reqData *models.TCBSPlaceDerivOrderRequest) (*models.TCBSPlaceDerivOrderResponse, error) {
	url := fmt.Sprintf("%s/khronos/v1/order/place", s.cfg.TCBS.BaseURL)
	payloadBytes, err := json.Marshal(reqData)
	if err != nil {
		return nil, fmt.Errorf("marshal place derivative order request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return nil, fmt.Errorf("create place derivative order request: %w", err)
	}
	s.setAuthHeaders(req)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Error("TCBSAccount.PlaceDerivativeOrder", "HTTP request failed",
			map[string]interface{}{"accountNo": reqData.AccountNo, "symbol": reqData.Symbol, "err": err.Error()})
		return nil, fmt.Errorf("execute place derivative order: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		RC   string `json:"rc"`
		Data []struct {
			OrderNo   string `json:"orderNo"`
			PKOrderNo string `json:"pk_orderNo"`
		} `json:"data"`
		Message string `json:"message"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode place derivative order response: %w", err)
	}

	var orderNo, pkOrderNo string
	if len(result.Data) > 0 {
		orderNo = result.Data[0].OrderNo
		pkOrderNo = result.Data[0].PKOrderNo
	}

	return &models.TCBSPlaceDerivOrderResponse{
		RC:        result.RC,
		OrderNo:   orderNo,
		PKOrderNo: pkOrderNo,
		Message:   result.Message,
	}, nil
}

// ─── CancelDerivativeOrder ───────────────────────────────────────────────────
func (s *tcbsAccountService) CancelDerivativeOrder(ctx context.Context, accountNo, orderNo string) (*models.TCBSCancelDerivOrderResponse, error) {
	url := fmt.Sprintf("%s/khronos/v1/order/cancel", s.cfg.TCBS.BaseURL)
	payload := map[string]string{
		"accountNo": accountNo,
		"orderNo":   orderNo,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal cancel deriv order: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return nil, fmt.Errorf("create cancel deriv order request: %w", err)
	}
	s.setAuthHeaders(req)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Error("TCBSAccount.CancelDerivativeOrder", "HTTP request failed",
			map[string]interface{}{"accountNo": accountNo, "orderNo": orderNo, "err": err.Error()})
		return nil, fmt.Errorf("execute cancel derivative order: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		RC      string `json:"rc"`
		OrderNo string `json:"orderNo"`
		Message string `json:"message"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode cancel deriv order response: %w", err)
	}

	return &models.TCBSCancelDerivOrderResponse{
		RC:      result.RC,
		OrderNo: result.OrderNo,
		Message: result.Message,
	}, nil
}
