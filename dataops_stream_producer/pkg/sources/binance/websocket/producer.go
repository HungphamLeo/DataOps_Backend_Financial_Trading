// Binance WebSocket Producer — CORE của T2 Stream Ingestion
// Pattern: goroutine per trading pair, auto-reconnect on disconnect
package websocket

import (
	"context"
	"dataops-stream-producer/pkg/configuration"
	"dataops-stream-producer/pkg/producer"
	"dataops-stream-producer/redis"
	"dataops-stream-producer/utils"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// BinanceProducer quản lý tất cả WebSocket connections đến Binance
// 1 goroutine per trading pair
type BinanceProducer struct {
	cfg      *configuration.Config
	producer producer.Producer
	redis    redis.Service
	wg       sync.WaitGroup
}

func NewBinanceProducer(cfg *configuration.Config, prod producer.Producer, redis redis.Service) *BinanceProducer {
	return &BinanceProducer{
		cfg:      cfg,
		producer: prod,
		redis:    redis,
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

// handleMessage parse message và route đến đúng Kafka topic thông qua Producer với DLQ + Retry
func (p *BinanceProducer) handleMessage(symbol string, raw []byte) {
	var envelope BinanceCombinedStreamMsg
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return
	}

	// Cache timestamp vào Redis để monitor slow provider
	cacheKey := fmt.Sprintf("binance_%s_%s", strings.ToLower(symbol), envelope.Stream)
	p.redis.Set(cacheKey, map[string]interface{}{
		"ts": time.Now().UnixMilli(),
	}, 60*time.Second)

	ctx := context.Background()

	switch {
	case strings.HasSuffix(envelope.Stream, "@trade"):
		var rawTrade BinanceRawTrade
		if err := json.Unmarshal(envelope.Data, &rawTrade); err != nil {
			log.Printf("[BinanceProducer] [%s] unmarshal trade error: %v", symbol, err)
			return
		}

		price, _ := strconv.ParseFloat(rawTrade.Price, 64)
		size, _ := strconv.ParseFloat(rawTrade.Quantity, 64)
		side := "buy"
		if rawTrade.IsBuyerMaker {
			side = "sell" // Buyer is maker => Seller is taker (aggressive)
		}

		tradeEvent := producer.TradeEvent{
			InstID:     symbol,
			TradeID:    strconv.FormatInt(rawTrade.TradeID, 10),
			Price:      price,
			Size:       size,
			Side:       side,
			TsMs:       rawTrade.TradeTime,
			ProducedMs: time.Now().UnixMilli(),
			InstType:   "SPOT",
		}

		if err := p.producer.PublishTrade(ctx, symbol, tradeEvent); err != nil {
			log.Printf("[BinanceProducer] [%s] publish trade error: %v", symbol, err)
		}

	case strings.Contains(envelope.Stream, "@depth"):
		var rawDepth BinanceRawDepth
		if err := json.Unmarshal(envelope.Data, &rawDepth); err != nil {
			log.Printf("[BinanceProducer] [%s] unmarshal depth error: %v", symbol, err)
			return
		}

		bids := make([]producer.PriceLevel, 0, len(rawDepth.Bids))
		for _, b := range rawDepth.Bids {
			if len(b) >= 2 {
				px, _ := strconv.ParseFloat(b[0], 64)
				qty, _ := strconv.ParseFloat(b[1], 64)
				bids = append(bids, producer.PriceLevel{Price: px, Quantity: qty})
			}
		}

		asks := make([]producer.PriceLevel, 0, len(rawDepth.Asks))
		for _, a := range rawDepth.Asks {
			if len(a) >= 2 {
				px, _ := strconv.ParseFloat(a[0], 64)
				qty, _ := strconv.ParseFloat(a[1], 64)
				asks = append(asks, producer.PriceLevel{Price: px, Quantity: qty})
			}
		}

		checksum := int32(rawDepth.LastUpdateID)
		obEvent := producer.OrderBookEvent{
			InstID:     symbol,
			TsMs:       time.Now().UnixMilli(),
			ProducedMs: time.Now().UnixMilli(),
			Bids:       bids,
			Asks:       asks,
			Checksum:   &checksum,
		}

		if err := p.producer.PublishOrderBook(ctx, symbol, obEvent); err != nil {
			log.Printf("[BinanceProducer] [%s] publish orderbook error: %v", symbol, err)
		}
	}
}

// BinanceCombinedStreamMsg là wrapper của combined stream message từ Binance
type BinanceCombinedStreamMsg struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type BinanceRawTrade struct {
	EventType    string `json:"e"`
	EventTime    int64  `json:"E"`
	Symbol       string `json:"s"`
	TradeID      int64  `json:"t"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	BuyerOrderID int64  `json:"b"`
	SellerOrderID int64 `json:"a"`
	TradeTime    int64  `json:"T"`
	IsBuyerMaker bool   `json:"m"`
}

type BinanceRawDepth struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}
