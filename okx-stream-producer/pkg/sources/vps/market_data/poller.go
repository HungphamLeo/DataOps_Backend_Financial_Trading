// VPS Market Data Poller
// VPS không có WebSocket public → dùng cron poll REST mỗi 1s trong giờ giao dịch
// REUSE: trading-bot-finance-service/pkg/sources/vps/vps.go (Call, CallLogin)
// REUSE: market-maker-background cron pattern
package market_data

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"okx-stream-producer/constants"
	"okx-stream-producer/pkg/configuration"
	"okx-stream-producer/redis"
	"okx-stream-producer/utils"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	kafka "github.com/segmentio/kafka-go"
)

// VPSPoller quản lý session VPS và poll market data định kỳ
type VPSPoller struct {
	cfg         *configuration.Config
	kafkaWriter *kafka.Writer
	redis       redis.Service
	session     string      // VPS session ID sau khi login
	sessionMu   sync.RWMutex
}

func NewPoller(cfg *configuration.Config, kafkaWriter *kafka.Writer, redis redis.Service) *VPSPoller {
	return &VPSPoller{cfg: cfg, kafkaWriter: kafkaWriter, redis: redis}
}

// Start khởi động poller với cron schedule
// REUSE: market-maker-background/app/background-tradebot/server/background.go
func (p *VPSPoller) Start(ctx context.Context) error {
	fmt.Printf("%s [VPSPoller] Starting for symbols: %v\n", utils.GetTimeNow(), p.cfg.VPS.Symbols)

	// Step 1: Login lần đầu
	if err := p.login(); err != nil {
		log.Printf("[VPSPoller] Initial login failed: %s", err.Error())
		// Không return error — sẽ retry qua cron
	}

	// Step 2: Setup cron jobs
	c := cron.New() // REUSE: robfig/cron

	// Refresh session mỗi 25 phút (VPS session expire ~30 phút)
	c.AddFunc("*/25 * * * *", func() {
		if err := p.login(); err != nil {
			log.Printf("[VPSPoller] Session refresh failed: %s", err.Error())
		}
	})

	// Poll market data mỗi 1 phút trong giờ giao dịch sáng (9:00–11:30 T2-T6)
	c.AddFunc("*/1 9-11 * * 1-5", func() { p.pollAll() })

	// Poll market data mỗi 1 phút trong giờ giao dịch chiều (13:00–14:45 T2-T6)
	c.AddFunc("*/1 13-14 * * 1-5", func() { p.pollAll() })

	go c.Start()

	// Block cho đến khi context cancel (server shutdown)
	<-ctx.Done()
	c.Stop()
	fmt.Printf("%s [VPSPoller] Stopped\n", utils.GetTimeNow())
	return nil
}

// login gọi VPS API để lấy session ID
// REUSE: trading-bot-finance-service/pkg/sources/vps/vps.go CallLogin pattern
func (p *VPSPoller) login() error {
	args := map[string]interface{}{
		"group": "L",
		"data": map[string]interface{}{
			"type": "string",
			"cmd":  "Web.sCheckLogin",
			"p1":   p.cfg.VPS.Username,
			"p2":   p.cfg.VPS.Password,
		},
	}

	body, err := p.callVPS(p.cfg.VPS.URLLogin, args)
	if err != nil {
		return err
	}

	var resp VPSLoginResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("parse login response: %w", err)
	}

	p.sessionMu.Lock()
	p.session = resp.Data.Sid
	p.sessionMu.Unlock()

	fmt.Printf("%s [VPSPoller] Login success, session: %s\n", utils.GetTimeNow(), p.session)
	return nil
}

// pollAll poll tất cả symbols và publish → Kafka
func (p *VPSPoller) pollAll() {
	p.sessionMu.RLock()
	sid := p.session
	p.sessionMu.RUnlock()

	if sid == "" {
		log.Printf("[VPSPoller] No session — skip poll")
		return
	}

	for _, symbol := range p.cfg.VPS.Symbols {
		go func(sym string) { // goroutine per symbol — REUSE pattern từ finance-service
			if err := p.pollSymbol(sid, sym); err != nil {
				log.Printf("[VPSPoller] [%s] poll error: %s", sym, err.Error())
			}
		}(symbol)
	}
}

// pollSymbol lấy market snapshot cho 1 symbol và publish → Kafka
func (p *VPSPoller) pollSymbol(session, symbol string) error {
	args := map[string]interface{}{
		"group":   "L",
		"session": session,
		"data": map[string]interface{}{
			"type":   "string",
			"cmd":    "Web.getMarketInfo",
			"symbol": symbol,
		},
	}

	body, err := p.callVPS(p.cfg.VPS.URL, args)
	if err != nil {
		return err
	}

	// Enrich với timestamp trước khi publish
	var snapshot map[string]interface{}
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return err
	}
	snapshot["ts"] = time.Now().UnixMilli()
	snapshot["symbol"] = symbol

	enriched, _ := json.Marshal(snapshot)

	// Publish → Kafka: vps.market.snapshot
	msg := kafka.Message{
		Topic: constants.TopicVPSMarketSnapshot,
		Key:   []byte(symbol),
		Value: enriched,
	}
	if err := p.kafkaWriter.WriteMessages(context.Background(), msg); err != nil {
		return fmt.Errorf("kafka write: %w", err)
	}

	// Cache để monitor slow response — REUSE: market-maker-background CheckProviderSlowResponse
	cacheKey := fmt.Sprintf("vps_%s_ticker", symbol)
	p.redis.Set(cacheKey, map[string]interface{}{"ts": time.Now().UnixMilli()}, 120*time.Second)

	return nil
}

// callVPS là HTTP POST helper — REUSE: trading-bot-finance-service/pkg/sources/vps/vps.go Call()
func (p *VPSPoller) callVPS(url string, args interface{}) ([]byte, error) {
	reqBytes, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("VPS API status: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

// VPSLoginResponse — REUSE: trading-bot-finance-service/pkg/sources/vps/types.go
type VPSLoginResponse struct {
	RC   int    `json:"rc"`
	RS   string `json:"rs"`
	Data struct {
		Sid  string `json:"sid"`
		User string `json:"user"`
		Name string `json:"name"`
	} `json:"data"`
}
