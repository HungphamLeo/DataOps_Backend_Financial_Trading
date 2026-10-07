// client.go — TCBS iFlash Open API HTTP Client
//
// Thay thế VPSClient bằng client đầy đủ cho TCBS iFlash Open API v1.0.0.
// Bao gồm: JWT auth, auto token management, retry pattern, tất cả market data endpoints.
//
// Reference: openapi-v1.0.0.json — TCBS iFlash Open API
// Base URL:  https://openapi.tcbs.com.vn
// Contact:   openapi@tcbs.com.vn
//
// Auth flow:
//  1. POST /gaia/v1/oauth2/openapi/token với apiKey + OTP (Smart OTP từ TCInvest)
//  2. Nhận JWT token → gắn vào Authorization: Bearer <token>
//  3. Token tự động mark stale sau TokenRefreshThreshold giây
//
// Retry pattern:
//   - Mỗi HTTP call có retry loop tối đa DefaultRetryMax lần
//   - Delay tăng dần: attempt × 1s
//   - HTTPError 4xx không retry (client error — không tự khỏi)
//   - HTTPError 5xx + network error → retry
//
// Design khác biệt so với VPSClient:
//   - VPS: JSON-RPC qua POST, session = string từ login response
//   - TCBS: REST, auth = JWT Bearer header
//   - TCBS public market endpoints (tickerCommons, tickerSnaps) không cần auth

package tcbsclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	tcbsconst "dataops-stream-producer/pkg/sources/tcbs/constants"
)

// Client quản lý TCBS HTTP session với JWT token
type Client struct {
	httpClient *http.Client
	baseURL    string
	apiKey     string

	// JWT token management — thread-safe
	jwtToken    string
	tokenObtainedAt time.Time
	tokenMu     sync.RWMutex
}

// NewClient tạo TCBS client mới.
// apiKey: API key từ TCInvest app — có thể trống nếu chỉ dùng public endpoints.
// Dùng SetToken() để inject JWT token nếu đã có sẵn.
func NewClient(apiKey string) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: tcbsconst.DefaultTimeout * time.Second,
		},
		baseURL: tcbsconst.BaseURL,
		apiKey:  apiKey,
	}
}

// NewClientWithURL tạo client với custom base URL (dùng cho testing / staging).
func NewClientWithURL(apiKey, baseURL string) *Client {
	c := NewClient(apiKey)
	c.baseURL = baseURL
	return c
}

// SetToken gắn JWT token đã có sẵn (e.g., từ env var TCBS_JWT_TOKEN).
// Dùng khi token được quản lý bên ngoài và không cần gọi Authenticate().
func (c *Client) SetToken(token string) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	c.jwtToken = token
	c.tokenObtainedAt = time.Now()
	log.Printf("[TCBSClient] JWT token set manually")
}

// Authenticate lấy JWT token từ TCBS bằng apiKey + Smart OTP.
//
// QUAN TRỌNG:
//   - OTP có TTL ngắn (~30s) và chỉ dùng được 1 lần.
//   - Gọi ngay trước khi bắt đầu session mới.
//   - Trong production dùng SetToken() nếu token được cấp từ bên ngoài (CI/CD).
//
// Returns: JWT token string, nil nếu thành công.
func (c *Client) Authenticate(otp string) (string, error) {
	if c.apiKey == "" {
		return "", fmt.Errorf("TCBSClient: apiKey is empty — set TCBS_API_KEY env var")
	}

	reqBody := TokenRequest{
		APIKey: c.apiKey,
		OTP:    otp,
	}

	respBytes, err := c.doPost(string(tcbsconst.EndpointToken), reqBody, false)
	if err != nil {
		return "", fmt.Errorf("authenticate: %w", err)
	}

	var tokenResp TokenResponse
	if err := json.Unmarshal(respBytes, &tokenResp); err != nil {
		return "", fmt.Errorf("parse token response: %w", err)
	}

	if tokenResp.Token == "" {
		return "", fmt.Errorf("TCBS auth response missing 'token' field")
	}

	c.tokenMu.Lock()
	c.jwtToken = tokenResp.Token
	c.tokenObtainedAt = time.Now()
	c.tokenMu.Unlock()

	log.Printf("[TCBSClient] Authentication successful — token obtained")
	return tokenResp.Token, nil
}

// IsTokenValid kiểm tra token còn hạn không (dựa trên threshold, không decode JWT).
func (c *Client) IsTokenValid() bool {
	c.tokenMu.RLock()
	defer c.tokenMu.RUnlock()
	if c.jwtToken == "" {
		return false
	}
	elapsed := time.Since(c.tokenObtainedAt)
	return elapsed < tcbsconst.TokenRefreshThreshold*time.Second
}

// ─── Market Data — Public Endpoints ──────────────────────────────────────────

// GetTickerCommons lấy bảng giá cổ phiếu với orderbook 3 cấp (bid/offer × 3).
//
// Tương đương VPS GetStockData nhưng phong phú hơn: có bid/offer 3 cấp, OHLC, foreign room.
//
// KHÔNG dùng tickers và indexCode cùng lúc (theo spec).
// Nếu cả hai đều rỗng/nil → trả về lỗi.
//
// tickers: danh sách mã, e.g. ["VCB","VHM"] — nil để dùng indexCode.
// indexCode: TCBSIndexCode, e.g. IndexCodeHOSE. 0 = không dùng.
func (c *Client) GetTickerCommons(tickers []string, indexCode tcbsconst.TCBSIndexCode) (*TickerCommonsResponse, error) {
	if len(tickers) == 0 && indexCode == 0 {
		return nil, fmt.Errorf("GetTickerCommons: phải truyền tickers hoặc indexCode")
	}
	if len(tickers) > 0 && indexCode != 0 {
		return nil, fmt.Errorf("GetTickerCommons: không dùng tickers và indexCode cùng lúc")
	}

	params := url.Values{}
	if len(tickers) > 0 {
		tickerCSV := ""
		for i, t := range tickers {
			if i > 0 {
				tickerCSV += ","
			}
			tickerCSV += t
		}
		params.Set("tickers", tickerCSV)
	} else {
		params.Set("index", strconv.Itoa(int(indexCode)))
	}

	// tickerCommons yêu cầu bearerAuth theo global security scheme trong openapi-v1.0.0.json.
	// Không có operation-level security override → dùng requireAuth: true.
	respBytes, err := c.doGetWithRetry(string(tcbsconst.EndpointTickerCommons), params, true)
	if err != nil {
		return nil, fmt.Errorf("GetTickerCommons: %w", err)
	}

	var result TickerCommonsResponse
	if err := json.Unmarshal(respBytes, &result); err != nil {
		return nil, fmt.Errorf("GetTickerCommons parse: %w", err)
	}

	log.Printf("[TCBSClient] GetTickerCommons — %d tickers returned, tradingDate=%s",
		len(result.Data), result.TradingDate)
	return &result, nil
}

// GetTickerSnaps lấy foreign room + lightweight market snapshot.
//
// indexCode: TCBSIndexCode để filter theo basket. 0 = toàn thị trường.
func (c *Client) GetTickerSnaps(indexCode tcbsconst.TCBSIndexCode) (*TickerSnapsResponse, error) {
	params := url.Values{}
	if indexCode != 0 {
		params.Set("index", strconv.Itoa(int(indexCode)))
	}

	respBytes, err := c.doGetWithRetry(string(tcbsconst.EndpointTickerSnaps), params, true)
	if err != nil {
		return nil, fmt.Errorf("GetTickerSnaps: %w", err)
	}

	var result TickerSnapsResponse
	if err := json.Unmarshal(respBytes, &result); err != nil {
		return nil, fmt.Errorf("GetTickerSnaps parse: %w", err)
	}

	log.Printf("[TCBSClient] GetTickerSnaps — %d records, tradingDate=%s",
		len(result.Data), result.TradingDate)
	return &result, nil
}

// GetDerivatives lấy bảng giá thị trường phái sinh (VN30F).
func (c *Client) GetDerivatives() (*DerivativesResponse, error) {
	respBytes, err := c.doGetWithRetry(string(tcbsconst.EndpointDerivatives), nil, true)
	if err != nil {
		return nil, fmt.Errorf("GetDerivatives: %w", err)
	}

	var result DerivativesResponse
	if err := json.Unmarshal(respBytes, &result); err != nil {
		return nil, fmt.Errorf("GetDerivatives parse: %w", err)
	}

	log.Printf("[TCBSClient] GetDerivatives — %d contracts returned", len(result.Data))
	return &result, nil
}

// ─── Intraday Analytics ───────────────────────────────────────────────────────

// GetIntradayBSA lấy Buy/Sell Analysis cho 1 mã cổ phiếu.
//
// ticker: mã CK, e.g. "TCB".
// investorType: loại nhà đầu tư — dùng InvestorType* constants.
func (c *Client) GetIntradayBSA(ticker string, investorType tcbsconst.TCBSInvestorType) (*BSAResponse, error) {
	if ticker == "" {
		return nil, fmt.Errorf("GetIntradayBSA: ticker is empty")
	}

	path := fmt.Sprintf(string(tcbsconst.EndpointIntradayBSA), ticker)
	params := url.Values{}
	params.Set("type", string(investorType))

	respBytes, err := c.doGetWithRetry(path, params, false)
	if err != nil {
		return nil, fmt.Errorf("GetIntradayBSA ticker=%s: %w", ticker, err)
	}

	var result BSAResponse
	if err := json.Unmarshal(respBytes, &result); err != nil {
		return nil, fmt.Errorf("GetIntradayBSA parse ticker=%s: %w", ticker, err)
	}

	log.Printf("[TCBSClient] GetIntradayBSA ticker=%s investorType=%s — %d records",
		ticker, investorType, len(result.Data))
	return &result, nil
}

// GetIntradayHistory lấy lịch sử khớp lệnh intraday (tick-by-tick) — 1 trang.
//
// ticker:    mã CK, e.g. "VCB".
// page:      số trang, bắt đầu từ 0.
// size:      số records mỗi trang (max 100).
// headIndex: head index cho reverse paging (-1 = mới nhất).
func (c *Client) GetIntradayHistory(ticker string, page, size, headIndex int) (*IntradayHistoryResponse, error) {
	if ticker == "" {
		return nil, fmt.Errorf("GetIntradayHistory: ticker is empty")
	}
	if size > tcbsconst.IntraHistoryMaxSize {
		size = tcbsconst.IntraHistoryMaxSize
	}

	path := fmt.Sprintf(string(tcbsconst.EndpointIntradayHistory), ticker)
	params := url.Values{}
	params.Set("page", strconv.Itoa(page))
	params.Set("size", strconv.Itoa(size))
	params.Set("headIndex", strconv.Itoa(headIndex))

	respBytes, err := c.doGetWithRetry(path, params, false)
	if err != nil {
		return nil, fmt.Errorf("GetIntradayHistory ticker=%s page=%d: %w", ticker, page, err)
	}

	var result IntradayHistoryResponse
	if err := json.Unmarshal(respBytes, &result); err != nil {
		return nil, fmt.Errorf("GetIntradayHistory parse ticker=%s: %w", ticker, err)
	}

	return &result, nil
}

// GetIntradayHistoryAllPages lấy toàn bộ trang lịch sử khớp lệnh intraday.
//
// Tự động paginate cho đến khi data rỗng hoặc đạt maxPages.
// Dừng sớm nếu một trang trả về ít hơn size records.
func (c *Client) GetIntradayHistoryAllPages(ticker string, maxPages int) ([]IntradayTickItem, error) {
	if ticker == "" {
		return nil, fmt.Errorf("GetIntradayHistoryAllPages: ticker is empty")
	}

	all := make([]IntradayTickItem, 0)
	headIndex := -1

	for page := 0; page < maxPages; page++ {
		resp, err := c.GetIntradayHistory(ticker, page, tcbsconst.IntraHistoryMaxSize, headIndex)
		if err != nil {
			log.Printf("[TCBSClient] GetIntradayHistoryAllPages ticker=%s page=%d: %v — stopping", ticker, page, err)
			break
		}
		if len(resp.Data) == 0 {
			break
		}

		all = append(all, resp.Data...)
		headIndex = int(resp.HeadIndex)

		// Dừng nếu trang này ít hơn size (đã hết data)
		if len(resp.Data) < tcbsconst.IntraHistoryMaxSize {
			break
		}
	}

	log.Printf("[TCBSClient] GetIntradayHistoryAllPages ticker=%s — %d total ticks", ticker, len(all))
	return all, nil
}

// ─── Internal HTTP helpers ────────────────────────────────────────────────────

// doGetWithRetry thực hiện GET request với retry loop.
// requireAuth=true → gắn Authorization: Bearer header.
// Retry với delay tăng dần cho 5xx và network errors.
// 4xx không retry — là client error cần fix.
func (c *Client) doGetWithRetry(path string, params url.Values, requireAuth bool) ([]byte, error) {
	fullURL := c.baseURL + path
	if len(params) > 0 {
		fullURL += "?" + params.Encode()
	}

	var lastErr error
	for attempt := 0; attempt < tcbsconst.DefaultRetryMax; attempt++ {
		if attempt > 0 {
			delay := time.Duration(attempt) * time.Second
			log.Printf("[TCBSClient] Retry %d/%d GET %s (delay=%s): %v",
				attempt, tcbsconst.DefaultRetryMax, path, delay, lastErr)
			time.Sleep(delay)
		}

		req, err := http.NewRequest(http.MethodGet, fullURL, nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}

		req.Header.Set("Accept", "application/json")
		if requireAuth {
			token, err := c.getToken()
			if err != nil {
				return nil, err
			}
			req.Header.Set("Authorization", "Bearer "+token)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("HTTP GET %s: %w", path, err)
			continue // network error → retry
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()

		if readErr != nil {
			lastErr = fmt.Errorf("read response body: %w", readErr)
			continue
		}

		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			// 4xx: client error — không retry
			return nil, fmt.Errorf("TCBS API %d for %s: %s", resp.StatusCode, path, string(body))
		}
		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("TCBS API %d for %s: %s", resp.StatusCode, path, string(body))
			continue // 5xx → retry
		}

		return body, nil
	}

	return nil, fmt.Errorf("GET %s after %d retries: %w", path, tcbsconst.DefaultRetryMax, lastErr)
}

// doPost thực hiện POST request (dùng cho auth endpoint).
// requireAuth=false vì auth endpoint không cần token.
func (c *Client) doPost(path string, body interface{}, requireAuth bool) ([]byte, error) {
	reqBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, c.baseURL+path, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("build POST request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if requireAuth {
		token, err := c.getToken()
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP POST %s: %w", path, err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read POST response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("TCBS API %d for POST %s: %s", resp.StatusCode, path, string(respBytes))
	}

	return respBytes, nil
}

// getToken lấy JWT token hiện tại thread-safe.
// Trả về error nếu token chưa được set — caller phải gọi Authenticate() hoặc SetToken() trước.
func (c *Client) getToken() (string, error) {
	c.tokenMu.RLock()
	defer c.tokenMu.RUnlock()
	if c.jwtToken == "" {
		return "", fmt.Errorf("TCBSClient: JWT token chưa được set. Gọi Authenticate(otp) hoặc SetToken(token) trước")
	}
	return c.jwtToken, nil
}
