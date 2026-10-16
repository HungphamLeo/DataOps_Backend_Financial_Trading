package cryptoaccount

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"dataops-backend-api/configuration"
	"dataops-backend-api/logging"
	"dataops-backend-api/models"
	"dataops-backend-api/redis"
)

// Service định nghĩa interface quản lý giao dịch và số dư Binance Spot
type Service interface {
	GetAccountBalance(ctx context.Context) (*models.BinanceAccountBalanceResponse, error)
	PlaceOrder(ctx context.Context, req *models.BinancePlaceOrderRequest) (*models.BinanceOrderResponse, error)
	CancelOrder(ctx context.Context, symbol string, orderID int64) (*models.BinanceCancelOrderResponse, error)
	GetOpenOrders(ctx context.Context, symbol string) ([]*models.BinanceOrderResponse, error)
}

type cryptoAccountService struct {
	cfg        *configuration.Config
	redis      redis.Service
	logger     *logging.Logger
	httpClient *http.Client
}

// NewService khởi tạo Binance Crypto Account & Order Proxy Service
func NewService(cfg *configuration.Config, redisSvc redis.Service, logger *logging.Logger) Service {
	return &cryptoAccountService{
		cfg:    cfg,
		redis:  redisSvc,
		logger: logger,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (s *cryptoAccountService) signQuery(params url.Values) string {
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	queryString := params.Encode()
	if s.cfg.Binance.SecretKey == "" {
		return queryString
	}
	h := hmac.New(sha256.New, []byte(s.cfg.Binance.SecretKey))
	h.Write([]byte(queryString))
	signature := hex.EncodeToString(h.Sum(nil))
	params.Set("signature", signature)
	return params.Encode()
}

func (s *cryptoAccountService) setHeaders(req *http.Request) {
	if s.cfg.Binance.APIKey != "" {
		req.Header.Set("X-MBX-APIKEY", s.cfg.Binance.APIKey)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
}

// ─── GetAccountBalance ────────────────────────────────────────────────────────
func (s *cryptoAccountService) GetAccountBalance(ctx context.Context) (*models.BinanceAccountBalanceResponse, error) {
	cacheKey := "binance_spot_account_balance"
	if s.redis != nil {
		cached := s.redis.Get(cacheKey)
		if cached != "" {
			var resp models.BinanceAccountBalanceResponse
			if err := json.Unmarshal([]byte(cached), &resp); err == nil {
				return &resp, nil
			}
		}
	}

	if s.cfg.Binance.APIKey == "" || s.cfg.Binance.SecretKey == "" {
		// Mock response for dev mode when API keys are not yet configured
		mockResp := &models.BinanceAccountBalanceResponse{
			Balances: []models.BinanceBalanceItem{
				{Asset: "USDT", Free: 10000.0, Locked: 0.0, Total: 10000.0},
				{Asset: "BTC", Free: 0.25, Locked: 0.0, Total: 0.25},
				{Asset: "ETH", Free: 3.5, Locked: 0.0, Total: 3.5},
			},
			UpdatedAt: time.Now().Unix(),
		}
		if s.redis != nil {
			_ = s.redis.Set(cacheKey, mockResp, 10*time.Second)
		}
		return mockResp, nil
	}

	params := url.Values{}
	signedQuery := s.signQuery(params)
	endpoint := fmt.Sprintf("%s/api/v3/account?%s", s.cfg.Binance.BaseURL, signedQuery)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create binance account request: %w", err)
	}
	s.setHeaders(req)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Error("CryptoAccount.GetAccountBalance", "HTTP request failed", map[string]interface{}{"err": err.Error()})
		return nil, fmt.Errorf("execute binance account request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Binance upstream error (status %d): %s", resp.StatusCode, string(body))
	}

	var rawAccount struct {
		Balances []struct {
			Asset  string `json:"asset"`
			Free   string `json:"free"`
			Locked string `json:"locked"`
		} `json:"balances"`
		UpdateTime int64 `json:"updateTime"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rawAccount); err != nil {
		return nil, fmt.Errorf("decode binance account response: %w", err)
	}

	var balances []models.BinanceBalanceItem
	for _, b := range rawAccount.Balances {
		free, _ := strconv.ParseFloat(b.Free, 64)
		locked, _ := strconv.ParseFloat(b.Locked, 64)
		if free > 0 || locked > 0 {
			balances = append(balances, models.BinanceBalanceItem{
				Asset:  b.Asset,
				Free:   free,
				Locked: locked,
				Total:  free + locked,
			})
		}
	}

	balanceResp := &models.BinanceAccountBalanceResponse{
		Balances:  balances,
		UpdatedAt: time.Now().Unix(),
	}

	if s.redis != nil {
		_ = s.redis.Set(cacheKey, balanceResp, 10*time.Second)
	}

	return balanceResp, nil
}

// ─── PlaceOrder ───────────────────────────────────────────────────────────────
func (s *cryptoAccountService) PlaceOrder(ctx context.Context, reqData *models.BinancePlaceOrderRequest) (*models.BinanceOrderResponse, error) {
	if s.cfg.Binance.APIKey == "" || s.cfg.Binance.SecretKey == "" {
		// Mock order placement for dev/testing
		return &models.BinanceOrderResponse{
			Symbol:        reqData.Symbol,
			OrderID:       time.Now().UnixNano() / 1e6,
			ClientOrderID: fmt.Sprintf("mock_%d", time.Now().Unix()),
			TransactTime:  time.Now().UnixMilli(),
			Price:         reqData.Price,
			OrigQty:       reqData.Quantity,
			ExecutedQty:   reqData.Quantity,
			Status:        "FILLED",
			Type:          reqData.Type,
			Side:          reqData.Side,
		}, nil
	}

	params := url.Values{}
	params.Set("symbol", reqData.Symbol)
	params.Set("side", reqData.Side)
	params.Set("type", reqData.Type)
	params.Set("quantity", strconv.FormatFloat(reqData.Quantity, 'f', -1, 64))

	if reqData.Type == "LIMIT" {
		if reqData.TimeInForce == "" {
			params.Set("timeInForce", "GTC")
		} else {
			params.Set("timeInForce", reqData.TimeInForce)
		}
		params.Set("price", strconv.FormatFloat(reqData.Price, 'f', -1, 64))
	}

	signedQuery := s.signQuery(params)
	endpoint := fmt.Sprintf("%s/api/v3/order?%s", s.cfg.Binance.BaseURL, signedQuery)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create binance place order request: %w", err)
	}
	s.setHeaders(req)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Error("CryptoAccount.PlaceOrder", "HTTP request failed", map[string]interface{}{"symbol": reqData.Symbol, "err": err.Error()})
		return nil, fmt.Errorf("execute binance place order: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Binance place order error (status %d): %s", resp.StatusCode, string(body))
	}

	var rawResp struct {
		Symbol        string `json:"symbol"`
		OrderID       int64  `json:"orderId"`
		ClientOrderID string `json:"clientOrderId"`
		TransactTime  int64  `json:"transactTime"`
		Price         string `json:"price"`
		OrigQty       string `json:"origQty"`
		ExecutedQty   string `json:"executedQty"`
		Status        string `json:"status"`
		Type          string `json:"type"`
		Side          string `json:"side"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rawResp); err != nil {
		return nil, fmt.Errorf("decode place order response: %w", err)
	}

	p, _ := strconv.ParseFloat(rawResp.Price, 64)
	origQty, _ := strconv.ParseFloat(rawResp.OrigQty, 64)
	execQty, _ := strconv.ParseFloat(rawResp.ExecutedQty, 64)

	return &models.BinanceOrderResponse{
		Symbol:        rawResp.Symbol,
		OrderID:       rawResp.OrderID,
		ClientOrderID: rawResp.ClientOrderID,
		TransactTime:  rawResp.TransactTime,
		Price:         p,
		OrigQty:       origQty,
		ExecutedQty:   execQty,
		Status:        rawResp.Status,
		Type:          rawResp.Type,
		Side:          rawResp.Side,
	}, nil
}

// ─── CancelOrder ──────────────────────────────────────────────────────────────
func (s *cryptoAccountService) CancelOrder(ctx context.Context, symbol string, orderID int64) (*models.BinanceCancelOrderResponse, error) {
	if s.cfg.Binance.APIKey == "" || s.cfg.Binance.SecretKey == "" {
		return &models.BinanceCancelOrderResponse{
			Symbol:  symbol,
			OrderID: orderID,
			Status:  "CANCELED",
			Message: "Mock order canceled successfully",
		}, nil
	}

	params := url.Values{}
	params.Set("symbol", symbol)
	params.Set("orderId", strconv.FormatInt(orderID, 10))

	signedQuery := s.signQuery(params)
	endpoint := fmt.Sprintf("%s/api/v3/order?%s", s.cfg.Binance.BaseURL, signedQuery)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create cancel order request: %w", err)
	}
	s.setHeaders(req)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Error("CryptoAccount.CancelOrder", "HTTP request failed", map[string]interface{}{"symbol": symbol, "orderID": orderID, "err": err.Error()})
		return nil, fmt.Errorf("execute cancel order: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Binance cancel order error (status %d): %s", resp.StatusCode, string(body))
	}

	var rawResp struct {
		Symbol  string `json:"symbol"`
		OrderID int64  `json:"orderId"`
		Status  string `json:"status"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rawResp); err != nil {
		return nil, fmt.Errorf("decode cancel order response: %w", err)
	}

	return &models.BinanceCancelOrderResponse{
		Symbol:  rawResp.Symbol,
		OrderID: rawResp.OrderID,
		Status:  rawResp.Status,
		Message: "Order canceled successfully",
	}, nil
}

// ─── GetOpenOrders ────────────────────────────────────────────────────────────
func (s *cryptoAccountService) GetOpenOrders(ctx context.Context, symbol string) ([]*models.BinanceOrderResponse, error) {
	if s.cfg.Binance.APIKey == "" || s.cfg.Binance.SecretKey == "" {
		return []*models.BinanceOrderResponse{}, nil
	}

	params := url.Values{}
	if symbol != "" {
		params.Set("symbol", symbol)
	}

	signedQuery := s.signQuery(params)
	endpoint := fmt.Sprintf("%s/api/v3/openOrders?%s", s.cfg.Binance.BaseURL, signedQuery)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create open orders request: %w", err)
	}
	s.setHeaders(req)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Error("CryptoAccount.GetOpenOrders", "HTTP request failed", map[string]interface{}{"symbol": symbol, "err": err.Error()})
		return nil, fmt.Errorf("execute open orders request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Binance open orders error (status %d): %s", resp.StatusCode, string(body))
	}

	var rawOrders []struct {
		Symbol        string `json:"symbol"`
		OrderID       int64  `json:"orderId"`
		ClientOrderID string `json:"clientOrderId"`
		Time          int64  `json:"time"`
		Price         string `json:"price"`
		OrigQty       string `json:"origQty"`
		ExecutedQty   string `json:"executedQty"`
		Status        string `json:"status"`
		Type          string `json:"type"`
		Side          string `json:"side"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rawOrders); err != nil {
		return nil, fmt.Errorf("decode open orders response: %w", err)
	}

	var orders []*models.BinanceOrderResponse
	for _, raw := range rawOrders {
		p, _ := strconv.ParseFloat(raw.Price, 64)
		origQty, _ := strconv.ParseFloat(raw.OrigQty, 64)
		execQty, _ := strconv.ParseFloat(raw.ExecutedQty, 64)

		orders = append(orders, &models.BinanceOrderResponse{
			Symbol:        raw.Symbol,
			OrderID:       raw.OrderID,
			ClientOrderID: raw.ClientOrderID,
			TransactTime:  raw.Time,
			Price:         p,
			OrigQty:       origQty,
			ExecutedQty:   execQty,
			Status:        raw.Status,
			Type:          raw.Type,
			Side:          raw.Side,
		})
	}

	return orders, nil
}
