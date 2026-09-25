// OKX WebSocket Producer — CORE của T2 Stream Ingestion
// REUSE pattern: trading-bot-finance-service/pkg/sources/xtb/client.go (WebSocket)
// REUSE pattern: trading-bot-finance-service/pkg/consumers/router.go (goroutine per pair)
package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"okx-stream-producer/constants"
	"okx-stream-producer/pkg/configuration"
	"okx-stream-producer/redis"
	"okx-stream-producer/utils"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	kafka "github.com/segmentio/kafka-go"
)

// OKXProducer quản lý tất cả WebSocket connections đến OKX
// 1 goroutine per trading pair — pattern từ trading-bot-finance-service
type OKXProducer struct {
	cfg         *configuration.Config
	kafkaWriter *kafka.Writer
	redis       redis.Service
	wg          sync.WaitGroup
}

func NewOKXProducer(cfg *configuration.Config, kafkaWriter *kafka.Writer, redis redis.Service) *OKXProducer {
	return &OKXProducer{
		cfg:         cfg,
		kafkaWriter: kafkaWriter,
		redis:       redis,
	}
}

// Start khởi động goroutine per trading pair
// Pattern: REUSE từ trading-bot-finance-service/pkg/consumers/consumers.go
func (p *OKXProducer) Start(ctx context.Context) error {
	fmt.Printf("%s [OKXProducer] Starting for symbols: %v\n", utils.GetTimeNow(), p.cfg.OKX.Symbols)

	for _, symbol := range p.cfg.OKX.Symbols {
		p.wg.Add(1)
		go func(sym string) {
			defer p.wg.Done()
			p.runPair(ctx, sym) // goroutine per pair
		}(symbol)
	}

	p.wg.Wait()
	return nil
}

// runPair chạy WebSocket stream cho 1 trading pair
// Tự động reconnect khi mất kết nối — REUSE pattern từ xtb/client.go
func (p *OKXProducer) runPair(ctx context.Context, symbol string) {
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("%s [OKXProducer] [%s] stopped\n", utils.GetTimeNow(), symbol)
			return
		default:
			if err := p.connectAndStream(ctx, symbol); err != nil {
				log.Printf("[OKXProducer] [%s] error: %s — reconnect in 5s", symbol, err.Error())
				// Exponential backoff reconnect — REUSE pattern từ queue/queue.go When()
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
				}
			}
		}
	}
}

// connectAndStream mở WebSocket và stream data → Kafka
// REUSE: gorilla/websocket — cùng library với trading-bot-finance-service/pkg/sources/xtb/client.go
func (p *OKXProducer) connectAndStream(ctx context.Context, symbol string) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, p.cfg.OKX.WSURL, nil)
	if err != nil {
		return fmt.Errorf("dial error: %w", err)
	}
	defer conn.Close()

	fmt.Printf("%s [OKXProducer] [%s] connected to OKX WebSocket\n", utils.GetTimeNow(), symbol)

	// Subscribe vào 3 channels: trades, books5 (top 5 levels), funding-rate
	subscribeMsg := map[string]interface{}{
		"op": "subscribe",
		"args": []map[string]string{
			{"channel": "trades", "instId": symbol},
			{"channel": "books5", "instId": symbol},
			{"channel": "funding-rate", "instId": symbol + "-SWAP"},
		},
	}
	if err := conn.WriteJSON(subscribeMsg); err != nil {
		return fmt.Errorf("subscribe error: %w", err)
	}

	// Read loop
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			_, msgBytes, err := conn.ReadMessage()
			if err != nil {
				return fmt.Errorf("read error: %w", err)
			}
			p.handleMessage(symbol, msgBytes)
		}
	}
}

// handleMessage parse message và route đến đúng Kafka topic
// REUSE pattern: trading-bot-finance-service/pkg/consumers/router.go switch case routing
func (p *OKXProducer) handleMessage(symbol string, raw []byte) {
	var msg OKXWSMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}

	// Bỏ qua event messages (subscribe confirm, pong)
	if msg.Event != "" {
		return
	}

	var topic string
	switch msg.Arg.Channel {
	case "trades":
		topic = constants.TopicOKXTradeTicks
	case "books5":
		topic = constants.TopicOKXOrderBookSnapshot
	case "funding-rate":
		topic = constants.TopicOKXFundingRates
	default:
		return
	}

	// Cache timestamp vào Redis để monitor slow provider
	// REUSE: market-maker-background/pkg/background-mm/services/background/asset.go pattern
	cacheKey := fmt.Sprintf("okx_%s_%s", strings.ToLower(symbol), msg.Arg.Channel)
	p.redis.Set(cacheKey, map[string]interface{}{
		"ts": time.Now().UnixMilli(),
	}, 60*time.Second)

	// Publish → Kafka
	kafkaMsg := kafka.Message{
		Topic: topic,
		Key:   []byte(symbol),   // Key = symbol → consistent hashing cho partitioner
		Value: raw,
	}
	if err := p.kafkaWriter.WriteMessages(context.Background(), kafkaMsg); err != nil {
		log.Printf("[OKXProducer] [%s] kafka write error: %s", symbol, err.Error())
	}
}

// OKXWSMessage là structure của message từ OKX WebSocket
type OKXWSMessage struct {
	Arg struct {
		Channel string `json:"channel"`
		InstId  string `json:"instId"`
	} `json:"arg"`
	Event string            `json:"event"`
	Data  []json.RawMessage `json:"data"`
}
