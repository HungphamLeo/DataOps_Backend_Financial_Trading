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

	// Binance gửi Ping frame mỗi ~3 phút và yêu cầu Pong trong vòng 10 phút.
	// Gorilla WebSocket KHÔNG tự trả Pong — phải set PingHandler tường minh.
	// Thiếu handler này dẫn đến close 1008 (policy violation: Pong timeout).
	conn.SetPingHandler(func(data string) error {
		// Phải set WriteDeadline trước khi ghi để tránh block vô hạn
		if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
			return err
		}
		return conn.WriteMessage(websocket.PongMessage, []byte(data))
	})

	// ReadDeadline reset sau mỗi message để phát hiện kết nối chết.
	// 5 phút > interval Ping của Binance (3 phút) → không false-positive.
	const readDeadline = 5 * time.Minute

	fmt.Printf("%s [BinanceProducer] [%s] connected to Binance WebSocket\n", utils.GetTimeNow(), symbol)

	// Read loop
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := conn.SetReadDeadline(time.Now().Add(readDeadline)); err != nil {
				return fmt.Errorf("set read deadline: %w", err)
			}
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

	// Cache timestamp monitor (latency tracking) — TTL 60s
	monitorKey := fmt.Sprintf("binance_%s_%s", strings.ToLower(symbol), envelope.Stream)
	p.redis.Set(monitorKey, map[string]interface{}{
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

		// Cache latest trade tick — dùng cho real-time dashboard / API query nhanh
		// Key: binance_{SYMBOL}_ticker, TTL 10s (refresh mỗi trade)
		// buyVol/sellVol: tích lũy trong window TTL — reset khi key expire
		symLower := strings.ToLower(symbol)
		tickerKey := fmt.Sprintf("binance_%s_ticker", symLower)
		existing := p.redis.Get(tickerKey)
		var buyVol, sellVol float64
		if existing != "" {
			var prev map[string]interface{}
			if err := json.Unmarshal([]byte(existing), &prev); err == nil {
				if v, ok := prev["buy_vol"].(float64); ok {
					buyVol = v
				}
				if v, ok := prev["sell_vol"].(float64); ok {
					sellVol = v
				}
			}
		}
		if side == "buy" {
			buyVol += size
		} else {
			sellVol += size
		}
		p.redis.Set(tickerKey, map[string]interface{}{
			"symbol":   symbol,
			"price":    price,
			"size":     size,
			"side":     side,
			"buy_vol":  buyVol,
			"sell_vol": sellVol,
			"trade_id": rawTrade.TradeID,
			"ts":       rawTrade.TradeTime,
		}, 10*time.Second)

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
