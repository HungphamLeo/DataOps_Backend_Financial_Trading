package rest_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"dataops-backend-api/api/rest"
	"dataops-backend-api/configuration"
	"dataops-backend-api/constants"
	"dataops-backend-api/logging"
	"dataops-backend-api/services"
	"dataops-backend-api/services/analytics"
	"dataops-backend-api/services/authentication"
	"dataops-backend-api/services/tcbs"
	tcbsaccount "dataops-backend-api/services/tcbs_account"
	"dataops-backend-api/services/vnstock"

	goredis "github.com/go-redis/redis/v8"
)

// MockRedis implement redis.Service
type mockRedis struct {
	data map[string]string
}

func newMockRedis() *mockRedis {
	return &mockRedis{data: make(map[string]string)}
}

func (m *mockRedis) Set(key string, value interface{}, _ time.Duration) error {
	switch v := value.(type) {
	case string:
		m.data[key] = v
	default:
		bytes, _ := json.Marshal(v)
		m.data[key] = string(bytes)
	}
	return nil
}

func (m *mockRedis) Get(key string) string {
	return m.data[key]
}

func (m *mockRedis) Scan(_ string) []string { return nil }
func (m *mockRedis) Del(key string) error {
	delete(m.data, key)
	return nil
}
func (m *mockRedis) Dels(_ []string) error { return nil }
func (m *mockRedis) Ping() bool            { return true }
func (m *mockRedis) Close() error          { return nil }
func (m *mockRedis) Publish(_ string, _ []byte) error {
	return nil
}
func (m *mockRedis) Subscribe(_ string) *goredis.PubSub {
	return nil
}

func TestPublicRoutes(t *testing.T) {
	logger, closeLogger, _ := logging.NewLogger(logging.Config{
		Service: "test",
		Level:   logging.INFO,
	})
	defer closeLogger()

	cfg := &configuration.Config{}
	cfg.App.Mode = "dev"
	cfg.Authentication.Secret = "12345678901234567890123456789012"

	mockR := newMockRedis()

	svc := &services.Services{
		AuthSvc:        authentication.NewService(nil, cfg, mockR, logger),
		CryptoSvc:      analytics.NewCryptoService(cfg, mockR, nil, logger),
		VNStockSvc:     vnstock.NewService(cfg, logger),
		TCBSSvc:        tcbs.NewService(cfg, mockR, nil, logger),
		TCBSAccountSvc: tcbsaccount.NewService(cfg, mockR, logger),
	}

	router := rest.NewAPI(svc, cfg, mockR, nil)

	// Test 1: Public endpoint (e.g. login with bad payload returns 400 or 401, not 404)
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/login", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code == http.StatusNotFound {
		t.Errorf("Expected route /api/v1/login to exist, got 404")
	}

	// Test 1b: Root and API v1 Healthcheck endpoints
	for _, path := range []string{"/health", "/api/v1/health"} {
		reqHealth, _ := http.NewRequest(http.MethodGet, path, nil)
		wHealth := httptest.NewRecorder()
		router.ServeHTTP(wHealth, reqHealth)
		if wHealth.Code != http.StatusOK {
			t.Errorf("Expected route %s to return 200 OK, got %d", path, wHealth.Code)
		}
	}

	// Test 2: Protected crypto ticker endpoint without JWT token -> 401 Unauthorized
	reqTicker, _ := http.NewRequest(http.MethodGet, "/api/v1/crypto/ticker?symbol=BTCUSDT", nil)
	wTicker := httptest.NewRecorder()
	router.ServeHTTP(wTicker, reqTicker)

	if wTicker.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 Unauthorized for protected endpoint without JWT, got %d", wTicker.Code)
	}

	// Test 3: Protected TCBS account cash endpoint without JWT token -> 401 Unauthorized
	reqTCBSCash, _ := http.NewRequest(http.MethodGet, "/api/v1/tcbs/account/cash?account_no=123456", nil)
	wTCBSCash := httptest.NewRecorder()
	router.ServeHTTP(wTCBSCash, reqTCBSCash)

	if wTCBSCash.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 Unauthorized for TCBS cash endpoint without JWT, got %d", wTCBSCash.Code)
	}
}

func TestSymbolValidation(t *testing.T) {
	if !constants.IsValidBinanceSymbol("BTCUSDT") {
		t.Errorf("Expected BTCUSDT to be a valid Binance symbol")
	}
	if !constants.IsValidTCBSSymbol("VCB") {
		t.Errorf("Expected VCB to be a valid TCBS symbol")
	}
	if !constants.IsValidTCBSSymbol("VNINDEX") {
		t.Errorf("Expected VNINDEX to be a valid TCBS index")
	}
}
