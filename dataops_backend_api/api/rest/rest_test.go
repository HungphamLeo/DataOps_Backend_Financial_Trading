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
	cryptoaccount "dataops-backend-api/services/crypto_account"
	"dataops-backend-api/services/portfolio"
	"dataops-backend-api/services/risk"
	"dataops-backend-api/services/tcbs"
	tcbsaccount "dataops-backend-api/services/tcbs_account"
	"dataops-backend-api/services/vnstock"

	goredis "github.com/go-redis/redis/v8"
)

// MockRedis implement redis.Service
type mockRedis struct {
	store map[string]string
}

func newMockRedis() *mockRedis {
	return &mockRedis{store: make(map[string]string)}
}

func (m *mockRedis) Get(key string) string                          { return m.store[key] }
func (m *mockRedis) Set(key string, val interface{}, ttl time.Duration) bool {
	m.store[key] = val.(string)
	return true
}
func (m *mockRedis) Delete(key string) bool                         { delete(m.store, key); return true }
func (m *mockRedis) Client() *goredis.Client                        { return nil }
func (m *mockRedis) Ping() bool                                     { return true }
func (m *mockRedis) Exists(key string) bool                         { _, ok := m.store[key]; return ok }
func (m *mockRedis) Expire(key string, ttl time.Duration) bool      { return true }
func (m *mockRedis) RateLimit(key string, limit int, window time.Duration) (bool, error) {
	return true, nil
}
func (m *mockRedis) GetOrSet(key string, ttl time.Duration, fn func() (string, error)) (string, error) {
	if val, ok := m.store[key]; ok {
		return val, nil
	}
	val, err := fn()
	if err != nil {
		return "", err
	}
	m.store[key] = val
	return val, nil
}
func (m *mockRedis) InvalidatePrefix(prefix string) error {
	for k := range m.store {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			delete(m.store, k)
		}
	}
	return nil
}

func TestNewAPI_RouteRegistration(t *testing.T) {
	logger, closeLogger, _ := logging.NewLogger(logging.Config{
		Service: "test",
		Level:   logging.INFO,
	})
	defer closeLogger()

	cfg := &configuration.Config{}
	cfg.App.Mode = "dev"
	cfg.Authentication.Secret = "12345678901234567890123456789012"

	mockR := newMockRedis()

	cryptoAccSvc := cryptoaccount.NewService(cfg, mockR, logger)
	tcbsAccSvc := tcbsaccount.NewService(cfg, mockR, logger)
	portfolioSvc := portfolio.NewService(cfg, mockR, cryptoAccSvc, tcbsAccSvc, logger)
	riskSvc := risk.NewService(cfg, portfolioSvc, nil, logger)
	svc := &services.Services{
		AuthSvc:          authentication.NewService(nil, cfg, mockR, logger),
		CryptoSvc:        analytics.NewCryptoService(cfg, mockR, nil, logger),
		CryptoAccountSvc: cryptoAccSvc,
		PortfolioSvc:     portfolioSvc,
		RiskSvc:          riskSvc,
		VNStockSvc:       vnstock.NewService(cfg, logger),
		TCBSSvc:          tcbs.NewService(cfg, mockR, nil, logger),
		TCBSAccountSvc:   tcbsAccSvc,
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

	// Test 4: Protected Portfolio overview endpoint without JWT -> 401 Unauthorized
	reqPortfolio, _ := http.NewRequest(http.MethodGet, "/api/v1/portfolio/overview", nil)
	wPortfolio := httptest.NewRecorder()
	router.ServeHTTP(wPortfolio, reqPortfolio)

	if wPortfolio.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 Unauthorized for Portfolio overview endpoint without JWT, got %d", wPortfolio.Code)
	}

	// Test 5: Protected Risk Report endpoint without JWT -> 401 Unauthorized
	reqRisk, _ := http.NewRequest(http.MethodGet, "/api/v1/portfolio/risk-report?account_no=123456", nil)
	wRisk := httptest.NewRecorder()
	router.ServeHTTP(wRisk, reqRisk)

	if wRisk.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 Unauthorized for Risk Report endpoint without JWT, got %d", wRisk.Code)
	}
}

func TestDefaultPort(t *testing.T) {
	if constants.DefaultHTTPPort != 8080 {
		t.Errorf("Expected DefaultHTTPPort to be 8080, got %d", constants.DefaultHTTPPort)
	}
}
