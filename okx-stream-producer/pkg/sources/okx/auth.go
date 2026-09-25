package okx

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"time"
)

// Authenticator quản lý tạo Header và Signature cho WebSocket/REST của OKX
type Authenticator struct {
	APIKey     string
	SecretKey  string
	Passphrase string
}

func NewAuthenticator(apiKey, secretKey, passphrase string) *Authenticator {
	return &Authenticator{
		APIKey:     apiKey,
		SecretKey:  secretKey,
		Passphrase: passphrase,
	}
}

// GenerateWSSignature tạo payload login xác thực qua WebSocket private channel của OKX
// OKX WS login format: {"op": "login", "args": [{"apiKey": "...", "passphrase": "...", "timestamp": "...", "sign": "..."}]}
func (a *Authenticator) GenerateWSSignature() (timestamp string, sign string) {
	ts := fmt.Sprintf("%d", time.Now().UTC().Unix())
	message := ts + "GET" + "/users/self/verify"
	mac := hmac.New(sha256.New, []byte(a.SecretKey))
	mac.Write([]byte(message))
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return ts, signature
}

// GenerateRESTHeaders tạo chữ ký HMAC SHA256 cho REST API request
func (a *Authenticator) GenerateRESTHeaders(method, requestPath, body string) (timestamp, sign string) {
	ts := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	message := ts + method + requestPath + body
	mac := hmac.New(sha256.New, []byte(a.SecretKey))
	mac.Write([]byte(message))
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return ts, signature
}
