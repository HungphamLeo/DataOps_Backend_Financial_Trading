// Binance WebSocket Producer — CORE của T2 Stream Ingestion
// Pattern: goroutine per trading pair, auto-reconnect on disconnect
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

// BinanceProducer quản lý tất cả WebSocket connections đến Binance
// 1 goroutine per trading pair
type BinanceProducer struct {
	cfg         *configuration.Config
	kafkaWriter *kafka.Writer
	redis       redis.Service
	wg          sync.WaitGroup
}

func NewBinanceProducer(cfg *configuration.Config, kafkaWriter *kafka.Writer, redis redis.Service) *BinanceProducer {
	return &BinanceProducer{
		cfg:         cfg,
		kafkaWriter: kafkaWriter,
		redis:       redis,
	}
}

// Start khởi động goroutine per trading pair
func (p *BinanceProducer) Start(ctx context.Context) error {
	fmt.Printf("%s [BinanceProducer] Starting for symbols: %v\n", utils.GetTimeNow(), p.cfg.Binance.Symbols)

	for _, symbol := range p.cfg.Binance.Symbols {
		p.wg.Add(1)
		go func(sym string) {
			defer p.wg.Done()
			p.runPair(ctx, sym)
		}(symbol)
	}

	p.wg.Wait()
	return nil
}

// runPair chạy WebSocket stream cho 1 trading pair
// Tự động reconnect khi mất kết nối
func (p *BinanceProducer) runPair(ctx context.Context, symbol string) {
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("%s [BinanceProducer] [%s] stopped\n", utils.GetTimeNow(), symbol)
			return
		default:
			if err := p.connectAndStream(ctx, symbol); err != nil {
				log.Printf("[BinanceProducer] [%s] error: %s — reconnect in 5s", symbol, err.Error())
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
// Binance combined stream: wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@depth5
func (p *BinanceProducer) connectAndStream(ctx context.Context, symbol string) error {
	symLower := strings.ToLower(symbol)
	// Combined stream: trade + depth5 (top 5 levels)
	streamPath := fmt.Sprintf("/stream?streams=%s@trade/%s@depth5", symLower, symLower)
	wsURL := p.cfg.Binance.WSURL + streamPath

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial error: %w", err)
	}
	defer conn.Close()

	fmt.Printf("%s [BinanceProducer] [%s] connected to Binance WebSocket\n", utils.GetTimeNow(), symbol)

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
func (p *BinanceProducer) handleMessage(symbol string, raw []byte) {
	var envelope BinanceCombinedStreamMsg
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return
	}

	var topic string
	switch {
	case strings.HasSuffix(envelope.Stream, "@trade"):
		topic = constants.TopicBinanceTradeTicks
	case strings.Contains(envelope.Stream, "@depth"):
		topic = constants.TopicBinanceOrderBookSnapshot
	default:
		return
	}

	// Cache timestamp vào Redis để monitor slow provider
	cacheKey := fmt.Sprintf("binance_%s_%s", strings.ToLower(symbol), envelope.Stream)
	p.redis.Set(cacheKey, map[string]interface{}{
		"ts": time.Now().UnixMilli(),
	}, 60*time.Second)

	// Publish → Kafka
	kafkaMsg := kafka.Message{
		Topic: topic,
		Key:   []byte(symbol),
		Value: raw,
	}
	if err := p.kafkaWriter.WriteMessages(context.Background(), kafkaMsg); err != nil {
		log.Printf("[BinanceProducer] [%s] kafka write error: %s", symbol, err.Error())
	}
}

// BinanceCombinedStreamMsg là wrapper của combined stream message từ Binance
type BinanceCombinedStreamMsg struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}
