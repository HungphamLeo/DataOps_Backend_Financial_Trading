package binance

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// Authenticator quản lý tạo chữ ký HMAC-SHA256 cho Binance REST/WebSocket private channels
type Authenticator struct {
	APIKey    string
	SecretKey string
}

func NewAuthenticator(apiKey, secretKey string) *Authenticator {
	return &Authenticator{
		APIKey:    apiKey,
		SecretKey: secretKey,
	}
}

// SignQueryString tạo chữ ký HMAC-SHA256 cho Binance REST API request.
// Binance sign format: HMAC-SHA256(secret, queryString) → hex string
// Gắn vào query: &signature=<hex>
func (a *Authenticator) SignQueryString(queryString string) string {
	mac := hmac.New(sha256.New, []byte(a.SecretKey))
	mac.Write([]byte(queryString))
	return hex.EncodeToString(mac.Sum(nil))
}

// GenerateListenKey tạo timestamp signed payload cho WebSocket user data stream login.
// Binance User Data Stream dùng listenKey (POST /api/v3/userDataStream) thay vì sign trực tiếp.
func (a *Authenticator) GenerateTimestampSignature(payload string) (timestamp string, sign string) {
	ts := fmt.Sprintf("%d", time.Now().UTC().UnixMilli())
	message := payload + "timestamp=" + ts
	mac := hmac.New(sha256.New, []byte(a.SecretKey))
	mac.Write([]byte(message))
	return ts, hex.EncodeToString(mac.Sum(nil))
}
