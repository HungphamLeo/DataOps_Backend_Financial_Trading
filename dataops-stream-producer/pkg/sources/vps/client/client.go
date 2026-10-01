// REUSE: ~95% copied from trading-bot-finance-service/pkg/sources/vps/vps.go
// LEARN: HTTP Client pattern trong Go — thuần stdlib, không cần third-party
//
// VPS Securities là sàn chứng khoán Việt Nam cung cấp API JSON-RPC qua HTTP POST
// Không có WebSocket public → phải poll định kỳ
//
// API flow:
//   1. Login → nhận sid (session ID)
//   2. Dùng sid để gọi các API khác (GetStockData, NewOrder, ListOrder...)
//   3. Session expires sau ~30 phút → cần refresh tự động
//
// Điểm khác biệt vs finance-service: bỏ order execution, chỉ giữ market data fetch
// vì dataops-stream-producer chỉ cần READ market data, không cần WRITE orders

package vpsclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// Client quản lý VPS HTTP session
// LEARN: Struct với fields — encapsulation, ẩn implementation detail
type Client struct {
	httpClient *http.Client
	sessionID  string    // VPS session ID, hết hạn sau ~30 phút
	sessionAt  time.Time // thời điểm session được tạo, để detect expiry
	username   string
	password   string
}

// NewClient tạo VPS client mới
// LEARN: Constructor nhận credentials từ ngoài (Dependency Injection)
//
//	Không hardcode credentials trong client — tốt cho security
func NewClient(username, password string) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: 15 * time.Second, // timeout để tránh hang khi VPS API chậm
		},
		username: username,
		password: password,
	}
}

// Login xác thực với VPS, lưu session ID
// REUSE: 95% từ vps.go Start() — đổi tên cho rõ nghĩa hơn
// LEARN: Retry loop — pattern phổ biến cho network calls:
//
//	for i := 0; i < maxRetry; i++ → thử lại nếu fail, delay giữa retries
func (c *Client) Login() error {
	args := LoginRequest{
		Group: "L",
		Data: &LoginDataRequest{
			Type:     "string",
			Cmd:      VPS_LOGIN,
			Username: c.username,
			Password: c.password,
		},
	}

	req, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("marshal login request: %w", err)
	}

	var lastErr error
	for i := 0; i < 3; i++ {
		if i > 0 {
			time.Sleep(time.Second * time.Duration(i)) // tăng delay sau mỗi retry
		}

		respBytes, err := c.callURL(VPS_URL_LOGIN, req)
		if err != nil {
			lastErr = err
			continue
		}

		result := &LoginResponse{}
		if err := json.Unmarshal(respBytes, result); err != nil {
			lastErr = fmt.Errorf("unmarshal login response: %w", err)
			continue
		}

		if result.RC != VPS_RC_SUCCESS {
			lastErr = fmt.Errorf("VPS login failed rc=%d rs=%s", result.RC, result.RS)
			continue
		}

		c.sessionID = result.Data.Sid
		c.sessionAt = time.Now()
		log.Printf("[VPSClient] Login OK user=%s sid=%s", c.username, c.sessionID[:8]+"***")
		return nil
	}

	return fmt.Errorf("VPS login after 3 retries: %w", lastErr)
}

// EnsureSession kiểm tra session còn hợp lệ không, tự động refresh nếu cần
// LEARN: Session management pattern — kiểm tra trước khi dùng, không đợi đến khi lỗi
//
//	25 phút = 30 phút (VPS timeout) - 5 phút (buffer)
func (c *Client) EnsureSession() error {
	if c.sessionID == "" || time.Since(c.sessionAt) > 25*time.Minute {
		log.Printf("[VPSClient] Session expired or missing, re-logging in...")
		return c.Login()
	}
	return nil
}

// GetStockData lấy market data cho danh sách mã chứng khoán
// LEARN: Variadic input ([]string) → batch request, giảm số lần gọi API
//
//	VPS API nhận tối đa ~200 symbols mỗi request
func (c *Client) GetStockData(symbols []string) ([]*StockDataResponse, error) {
	if err := c.EnsureSession(); err != nil {
		return nil, fmt.Errorf("ensure session: %w", err)
	}

	args := StockDataRequest{
		Group:   "B",
		Session: c.sessionID,
		Data: &StockDataRequestData{
			Type:    "cursor",
			Cmd:     VPS_GET_STOCK_DATA,
			Symbols: symbols,
		},
	}

	req, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("marshal stock data request: %w", err)
	}

	var lastErr error
	for i := 0; i < 3; i++ {
		if i > 0 {
			time.Sleep(2 * time.Second)
		}

		respBytes, err := c.call(req)
		if err != nil {
			// Nếu lỗi có thể là session expired → thử login lại
			if loginErr := c.Login(); loginErr == nil {
				args.Session = c.sessionID
				req, _ = json.Marshal(args)
			}
			lastErr = err
			continue
		}

		result := &StockDataListResponse{}
		if err := json.Unmarshal(respBytes, result); err != nil {
			lastErr = fmt.Errorf("unmarshal stock data: %w", err)
			continue
		}

		if result.RC != VPS_RC_SUCCESS {
			lastErr = fmt.Errorf("VPS stock data rc=%d rs=%s", result.RC, result.RS)
			continue
		}

		return result.Data, nil
	}

	return nil, fmt.Errorf("GetStockData after 3 retries: %w", lastErr)
}

// GetAllStockList lấy toàn bộ danh sách mã chứng khoán theo sàn
func (c *Client) GetAllStockList(exchange string) ([]*StockListItem, error) {
	if err := c.EnsureSession(); err != nil {
		return nil, err
	}

	args := StockListRequest{
		Group:   "B",
		Session: c.sessionID,
		Data: &StockListRequestData{
			Type:     "cursor",
			Cmd:      VPS_GET_STOCK_LIST,
			Exchange: exchange,
		},
	}

	req, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	respBytes, err := c.call(req)
	if err != nil {
		return nil, err
	}

	result := &StockListResponse{}
	if err := json.Unmarshal(respBytes, result); err != nil {
		return nil, fmt.Errorf("unmarshal stock list: %w", err)
	}

	return result.Data, nil
}

// call gọi API endpoint chính của VPS (smartone)
// REUSE: 100% từ vps.go Call()
func (c *Client) call(reqBody []byte) ([]byte, error) {
	return c.callURL(VPS_URL, reqBody)
}

// callURL là HTTP POST helper — reuse cho cả login và data endpoints
// LEARN: io.ReadAll đọc toàn bộ response body vào []byte
//
//	resp.Body.Close() bắt buộc để tránh connection leak
func (c *Client) callURL(url string, reqBody []byte) ([]byte, error) {
	body := bytes.NewBuffer(reqBody)

	resp, err := c.httpClient.Post(url, "application/json", body)
	if err != nil {
		return nil, fmt.Errorf("HTTP POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	return respBytes, nil
}
