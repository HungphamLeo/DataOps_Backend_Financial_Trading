// consumer.go — TCBS WSS Derivative Consumer
//
// Kết nối WebSocket đến TCBS /api/v1/derivatives/contracts để nhận real-time
// price updates cho phái sinh VN30F (VN30F2410, VN30F2411...).
//
// Chỉ start khi TCBS_WSS_SYMBOLS không rỗng — cổ phiếu cơ sở KHÔNG có WSS.
// Pattern: goroutine per connection (1 connection đủ cho tất cả symbols),
// auto-reconnect với exponential backoff, JWT Bearer auth qua query param.
//
// Reference: openapi-v1.0.0.json — /api/v1/derivatives/contracts
// Base WSS URL: wss://openapi.tcbs.com.vn/api/v1/derivatives/contracts
package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"dataops-stream-producer/pkg/configuration"
	"dataops-stream-producer/pkg/producer"
	"dataops-stream-producer/redis"
	"dataops-stream-producer/utils"
)

const (
	// wssBaseURL là WebSocket URL của TCBS derivatives contracts endpoint.
	// Auth qua query param token= (JWT Bearer không hỗ trợ qua WS header trực tiếp).
	wssBaseURL = "wss://openapi.tcbs.com.vn/api/v1/derivatives/contracts"

	// readDeadline phải > heartbeat interval của TCBS server (ước tính 60s).
	readDeadline = 3 * time.Minute

	// maxReconnectDelay là backoff tối đa giữa các lần reconnect.
	maxReconnectDelay = 60 * time.Second
)

// TCBSWSSConsumer kết nối WebSocket TCBS và stream derivative price updates → Kafka.
type TCBSWSSConsumer struct {
	cfg      *configuration.Config
	producer producer.Producer
	redis    redis.Service
	wg       sync.WaitGroup
}

func NewConsumer(cfg *configuration.Config, prod producer.Producer, redisSvc redis.Service) *TCBSWSSConsumer {
	return &TCBSWSSConsumer{
		cfg:      cfg,
		producer: prod,
		redis:    redisSvc,
	}
}

// Start khởi động WSS consumer goroutine.
// Chỉ gọi khi cfg.TCBS.WssSymbols không rỗng — caller phải guard trước.
// Block cho đến khi ctx bị cancel.
func (c *TCBSWSSConsumer) Start(ctx context.Context) {
	log.Printf("%s [TCBSWSConsumer] Starting for symbols: %v", utils.GetTimeNow(), c.cfg.TCBS.WssSymbols)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.runWithReconnect(ctx)
	}()

	c.wg.Wait()
}

// runWithReconnect giữ kết nối WSS alive với exponential backoff khi disconnect.
func (c *TCBSWSSConsumer) runWithReconnect(ctx context.Context) {
	delay := 5 * time.Second

	for {
		select {
		case <-ctx.Done():
			log.Printf("%s [TCBSWSConsumer] stopped", utils.GetTimeNow())
			return
		default:
		}

		if err := c.connectAndStream(ctx); err != nil {
			log.Printf("[TCBSWSConsumer] connection error: %v — reconnect in %s", err, delay)
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			// Exponential backoff: 5s → 10s → 20s → ... → 60s (cap)
			delay *= 2
			if delay > maxReconnectDelay {
				delay = maxReconnectDelay
			}
		} else {
			// Reset backoff sau khi kết nối lại thành công
			delay = 5 * time.Second
		}
	}
}

// connectAndStream mở WebSocket đến TCBS, subscribe symbols và xử lý messages.
// JWT token được gửi qua query param ?token=<jwt> vì WS protocol không hỗ trợ
// Authorization header sau handshake.
func (c *TCBSWSSConsumer) connectAndStream(ctx context.Context) error {
	token := c.cfg.TCBS.JWTToken
	if token == "" {
		return fmt.Errorf("TCBS_JWT_TOKEN is empty — cannot connect to WSS derivatives")
	}

	wsURL := fmt.Sprintf("%s?token=%s", wssBaseURL, token)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial error: %w", err)
	}
	defer conn.Close()

	// Trả Pong khi nhận Ping để giữ kết nối alive (giống BinanceProducer pattern).
	conn.SetPingHandler(func(data string) error {
		if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
			return err
		}
		return conn.WriteMessage(websocket.PongMessage, []byte(data))
	})

	log.Printf("%s [TCBSWSConsumer] connected — streaming %d derivative symbols",
		utils.GetTimeNow(), len(c.cfg.TCBS.WssSymbols))

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err := conn.SetReadDeadline(time.Now().Add(readDeadline)); err != nil {
			return fmt.Errorf("set read deadline: %w", err)
		}

		_, msgBytes, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read error: %w", err)
		}

		c.handleMessage(msgBytes)
	}
}

// TCBSDerivWSMessage là message nhận từ TCBS WSS /api/v1/derivatives/contracts.
// TCBS push JSON array hoặc object — parse mềm bằng RawMessage.
type TCBSDerivWSMessage struct {
	Symbol        string  `json:"symbol"`
	CeilPrice     float64 `json:"ceilPrice"`
	FloorPrice    float64 `json:"floorPrice"`
	RefPrice      float64 `json:"refPrice"`
	MatchPrice    float64 `json:"matchPrice"`
	MatchQtty     float64 `json:"matchQtty"`     // API dùng "matchQtty" (không phải matchQty)
	BidPrice01    float64 `json:"bidPrice01"`
	BidQtty01     float64 `json:"bidQtty01"`
	OfferPrice01  float64 `json:"offerPrice01"`
	OfferQtty01   float64 `json:"offerQtty01"`
	Change        float64 `json:"change"`
	ChangePercent float64 `json:"changePercent"`
	TotalVolume   float64 `json:"totalVolume"`
}

// handleMessage parse và route WSS message → Kafka + Redis.
// TCBS WSS có thể push: single object hoặc array — thử cả 2 format.
func (c *TCBSWSSConsumer) handleMessage(raw []byte) {
	nowMs := time.Now().UnixMilli()
	ctx := context.Background()

	// Thử parse array trước (TCBS có thể push batch)
	var items []TCBSDerivWSMessage
	if err := json.Unmarshal(raw, &items); err != nil || len(items) == 0 {
		// Fallback: parse single object
		var item TCBSDerivWSMessage
		if err := json.Unmarshal(raw, &item); err != nil || item.Symbol == "" {
			return
		}
		items = []TCBSDerivWSMessage{item}
	}

	for _, item := range items {
		if item.Symbol == "" {
			continue
		}
		// Chỉ xử lý symbols trong WssSymbols config — bỏ qua symbol không cần
		if !c.isWssSymbol(item.Symbol) {
			continue
		}

		event := producer.TCBSDerivativeEvent{
			Symbol:     item.Symbol,
			CeilPrice:  item.CeilPrice,
			FloorPrice: item.FloorPrice,
			RefPrice:   item.RefPrice,
			MatchPrice: item.MatchPrice,
			MatchQty:   item.MatchQtty,
			BidPrice01: item.BidPrice01,
			BidQty01:   item.BidQtty01,
			AskPrice01: item.OfferPrice01,
			AskQty01:   item.OfferQtty01,
			Change:     item.Change,
			ChangePct:  item.ChangePercent,
			TotalVol:   item.TotalVolume,
			TsMs:       nowMs,
			ProducedMs: nowMs,
		}

		if err := c.producer.PublishTCBSDerivative(ctx, item.Symbol, event); err != nil {
			log.Printf("[TCBSWSConsumer] PublishTCBSDerivative %s: %v", item.Symbol, err)
		}

		// Cache latest derivative tick — đối xứng với binance_{sym}_ticker
		cacheKey := fmt.Sprintf("tcbs_%s_ticker", strings.ToLower(item.Symbol))
		c.redis.Set(cacheKey, map[string]interface{}{
			"symbol":      item.Symbol,
			"match_price": item.MatchPrice,
			"match_qty":   item.MatchQtty,
			"total_vol":   item.TotalVolume,
			"change":      item.Change,
			"change_pct":  item.ChangePercent,
			"bid_price1":  item.BidPrice01,
			"bid_qty1":    item.BidQtty01,
			"ask_price1":  item.OfferPrice01,
			"ask_qty1":    item.OfferQtty01,
			"ref_price":   item.RefPrice,
			"ts":          nowMs,
		}, 30*time.Second) // TTL 30s — WSS push liên tục nên không cần lâu
	}
}

// isWssSymbol kiểm tra symbol có trong danh sách WssSymbols không.
func (c *TCBSWSSConsumer) isWssSymbol(symbol string) bool {
	for _, s := range c.cfg.TCBS.WssSymbols {
		if strings.EqualFold(s, symbol) {
			return true
		}
	}
	return false
}
