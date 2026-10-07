// poller.go — TCBS Market Data Poller
//
// Thay thế VPSPoller bằng TCBSPoller cho TCBS iFlash Open API.
// TCBS không có WebSocket public → dùng cron poll REST trong giờ giao dịch.
//
// So sánh với VPSPoller:
//   - VPS:  JSON-RPC POST + session login mỗi 25 phút
//   - TCBS: REST GET + JWT Bearer (không cần refresh cron, token long-lived)
//   - TCBS: 3 loại data: market snapshot (tickerCommons), BSA, intraday ticks
//
// Cron schedule (HOSE):
//   Sáng:  */1 9-11 * * 1-5  (9:00–11:59 T2-T6)
//   Chiều: */1 13-14 * * 1-5 (13:00–14:59 T2-T6)
//   BSA + Intraday: chạy 1 lần sau 15:00 để có full-day data
//
// Reference: openapi-v1.0.0.json — TCBS iFlash Open API v1.0.0
package market_data

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/robfig/cron/v3"

	"dataops-stream-producer/pkg/configuration"
	"dataops-stream-producer/pkg/producer"
	tcbsclient "dataops-stream-producer/pkg/sources/tcbs/client"
	tcbsconst "dataops-stream-producer/pkg/sources/tcbs/constants"
	"dataops-stream-producer/redis"
	"dataops-stream-producer/utils"
)

// vietnamLocation là timezone Asia/Ho_Chi_Minh — dùng cho tất cả cron schedule TCBS.
// HOSE giờ giao dịch: 09:00–11:30 và 13:00–14:45 ICT (UTC+7).
// robfig/cron mặc định dùng UTC nên BẮT BUỘC inject location này.
var vietnamLocation = mustLoadLocation("Asia/Ho_Chi_Minh")

func mustLoadLocation(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		// Fallback: UTC+7 fixed offset nếu tzdata không có trong image
		log.Printf("[TCBSPoller] WARNING: cannot load timezone %q (%v) — falling back to UTC+7 fixed offset", name, err)
		return time.FixedZone("ICT", 7*60*60)
	}
	return loc
}

// TCBSPoller quản lý TCBS polling định kỳ trong giờ giao dịch
type TCBSPoller struct {
	cfg      *configuration.Config
	producer producer.Producer
	redis    redis.Service
	client   *tcbsclient.Client
}

// NewPoller tạo TCBSPoller mới.
// JWT token được inject từ cfg.TCBS.JWTToken (env TCBS_JWT_TOKEN).
// Nếu token rỗng → poller sẽ log warning nhưng không crash (public endpoints không cần token).
func NewPoller(cfg *configuration.Config, prod producer.Producer, redisSvc redis.Service) *TCBSPoller {
	client := tcbsclient.NewClientWithURL(cfg.TCBS.APIKey, cfg.TCBS.BaseURL)
	if cfg.TCBS.JWTToken != "" {
		client.SetToken(cfg.TCBS.JWTToken)
	}

	return &TCBSPoller{
		cfg:      cfg,
		producer: prod,
		redis:    redisSvc,
		client:   client,
	}
}

// Start khởi động poller với cron schedule.
// Block cho đến khi ctx bị cancel (graceful shutdown).
func (p *TCBSPoller) Start(ctx context.Context) error {
	fmt.Printf("%s [TCBSPoller] Starting — indexes=%v, rest_symbols=%v\n",
		utils.GetTimeNow(), p.cfg.TCBS.Indexes, p.cfg.TCBS.RestSymbols)

	if len(p.cfg.TCBS.WssSymbols) > 0 {
		// WSS consumer chưa implement — cần TCBSWSSConsumer riêng (goroutine như Binance)
		log.Printf("[TCBSPoller] INFO: TCBS_WSS_SYMBOLS=%v configured but WSS consumer not yet implemented — skipping", p.cfg.TCBS.WssSymbols)
	}

	if p.cfg.TCBS.JWTToken == "" && p.cfg.TCBS.APIKey == "" {
		log.Printf("[TCBSPoller] WARNING: cả TCBS_JWT_TOKEN và TCBS_API_KEY đều rỗng. Public endpoints sẽ work nhưng private endpoints sẽ fail.")
	}

	// BẮT BUỘC inject timezone ICT (UTC+7) vào cron scheduler.
	// Container chạy UTC — nếu không set location, cron "9-11" sẽ fire lúc 09:00 UTC = 16:00 ICT,
	// tức là ngoài giờ giao dịch HOSE hoàn toàn.
	c := cron.New(cron.WithLocation(vietnamLocation))

	// Poll market snapshot mỗi 1 phút trong giờ giao dịch sáng (9:00–11:59 T2-T6)
	if _, err := c.AddFunc("*/1 9-11 * * 1-5", func() { p.pollMarketSnapshot() }); err != nil {
		return fmt.Errorf("[TCBSPoller] add morning cron: %w", err)
	}

	// Poll market snapshot mỗi 1 phút trong giờ giao dịch chiều (13:00–14:59 T2-T6)
	if _, err := c.AddFunc("*/1 13-14 * * 1-5", func() { p.pollMarketSnapshot() }); err != nil {
		return fmt.Errorf("[TCBSPoller] add afternoon cron: %w", err)
	}

	// Poll BSA + Intraday ticks 1 lần sau khi đóng cửa (15:05 T2-T6) để có full-day data
	if _, err := c.AddFunc("5 15 * * 1-5", func() {
		p.pollBSA()
		p.pollIntradayTicks()
	}); err != nil {
		return fmt.Errorf("[TCBSPoller] add EOD cron: %w", err)
	}

	go c.Start()
	fmt.Printf("%s [TCBSPoller] Cron jobs started\n", utils.GetTimeNow())

	<-ctx.Done()
	c.Stop()
	fmt.Printf("%s [TCBSPoller] Stopped\n", utils.GetTimeNow())
	return nil
}

// pollMarketSnapshot lấy full market snapshot cho tất cả configured indexes
// và publish từng ticker lên Kafka topic tcbs.market.snapshot.
func (p *TCBSPoller) pollMarketSnapshot() {
	nowMs := time.Now().UnixMilli()

	for _, indexName := range p.cfg.TCBS.Indexes {
		indexCode, ok := tcbsconst.IndexNameToCode[indexName]
		if !ok {
			log.Printf("[TCBSPoller] Unknown index name '%s' — skip. Check TCBS_INDEXES env var or tcbs_constants.", indexName)
			continue
		}

		result, err := p.client.GetTickerCommons(nil, indexCode)
		if err != nil {
			log.Printf("[TCBSPoller] GetTickerCommons index=%s: %v", indexName, err)
			continue
		}

		publishedCount := 0
		for _, item := range result.Data {
			if item.Symbol == "" {
				continue
			}

			event := producer.TCBSMarketEvent{
				Symbol:          item.Symbol,
				Exchange:        indexName,
				RefPrice:        item.RefPrice,
				CeilPrice:       item.CeilPrice,
				FloorPrice:      item.FloorPrice,
				MatchPrice:      item.MatchPrice,
				MatchQty:        item.MatchQtty,
				BidPrice01:      item.BidPrice01,
				BidPrice02:      item.BidPrice02,
				BidPrice03:      item.BidPrice03,
				BidQty01:        item.BidQtty01,
				BidQty02:        item.BidQtty02,
				BidQty03:        item.BidQtty03,
				OfferPrice01:    item.OfferPrice01,
				OfferPrice02:    item.OfferPrice02,
				OfferPrice03:    item.OfferPrice03,
				OfferQty01:      item.OfferQtty01,
				OfferQty02:      item.OfferQtty02,
				OfferQty03:      item.OfferQtty03,
				Open:            item.Open,
				High:            item.High,
				Low:             item.Low,
				Avg:             item.Avg,
				TotalVol:        item.TotalVol,
				TotalVal:        item.TotalVal,
				BuyForeignQty:   item.BuyForeignQtty,
				SellForeignQty:  item.SellForeignQtty,
				Room:            item.Room,
				Change:          item.Change,
				ChangePct:       item.ChangePercent,
				TradingDate:     result.TradingDate,
				TsMs:            nowMs,
				ProducedMs:      nowMs,
			}

			ctx := context.Background()
			if err := p.producer.PublishTCBSMarket(ctx, item.Symbol, event); err != nil {
				log.Printf("[TCBSPoller] PublishTCBSMarket %s: %v", item.Symbol, err)
				continue
			}
			publishedCount++

			// Cache để monitor latency — REUSE pattern từ VPSPoller
			cacheKey := fmt.Sprintf("tcbs_%s_ticker", item.Symbol)
			p.redis.Set(cacheKey, map[string]interface{}{"ts": nowMs, "index": indexName}, 120*time.Second)
		}

		log.Printf("[TCBSPoller] pollMarketSnapshot index=%s — published %d/%d tickers",
			indexName, publishedCount, len(result.Data))
	}
}

// pollBSA lấy Buy/Sell Analysis cho tất cả RestSymbols và publish lên Kafka.
// Chạy 1 lần/ngày sau đóng cửa để có full-day BSA.
func (p *TCBSPoller) pollBSA() {
	nowMs := time.Now().UnixMilli()
	investorType := p.cfg.TCBS.BSAInvestorType
	if investorType == "" {
		investorType = string(tcbsconst.InvestorTypeAll)
	}

	log.Printf("[TCBSPoller] pollBSA — %d tickers, investorType=%s",
		len(p.cfg.TCBS.RestSymbols), investorType)

	for _, ticker := range p.cfg.TCBS.RestSymbols {
		result, err := p.client.GetIntradayBSA(ticker, tcbsconst.TCBSInvestorType(investorType))
		if err != nil {
			log.Printf("[TCBSPoller] GetIntradayBSA ticker=%s: %v", ticker, err)
			continue
		}

		publishedCount := 0
		ctx := context.Background()
		for _, item := range result.Data {
			event := producer.TCBSBSAEvent{
				Ticker:       ticker,
				InvestorType: investorType,
				BUP:          item.BuyUpPercent,
				SDP:          item.SellDownPercent,
				BSR:          item.BuySellRatio,
				EventTime:    item.Time,
				TsMs:         nowMs,
				ProducedMs:   nowMs,
			}

			if err := p.producer.PublishTCBSBSA(ctx, ticker, event); err != nil {
				log.Printf("[TCBSPoller] PublishTCBSBSA ticker=%s: %v", ticker, err)
				continue
			}
			publishedCount++
		}

		log.Printf("[TCBSPoller] pollBSA ticker=%s — published %d BSA records", ticker, publishedCount)
	}
}

// pollIntradayTicks lấy intraday tick history cho tất cả RestSymbols.
// Chạy 1 lần/ngày sau đóng cửa. maxPages từ cfg để kiểm soát volume.
func (p *TCBSPoller) pollIntradayTicks() {
	nowMs := time.Now().UnixMilli()
	maxPages := p.cfg.TCBS.IntradayMaxPages
	if maxPages <= 0 {
		maxPages = 5
	}

	log.Printf("[TCBSPoller] pollIntradayTicks — %d tickers, maxPages=%d",
		len(p.cfg.TCBS.RestSymbols), maxPages)

	for _, ticker := range p.cfg.TCBS.RestSymbols {
		ticks, err := p.client.GetIntradayHistoryAllPages(ticker, maxPages)
		if err != nil {
			log.Printf("[TCBSPoller] GetIntradayHistoryAllPages ticker=%s: %v", ticker, err)
			continue
		}

		publishedCount := 0
		ctx := context.Background()
		for _, item := range ticks {
			event := producer.TCBSIntradayTickEvent{
				Ticker:      ticker,
				Price:       item.Price,
				Volume:      item.Volume,
				PriceChange: item.PriceChange,
				Action:      item.Action,
				BuyAcc:      item.BuyAcc,
				SellAcc:     item.SellAcc,
				Highlight:   item.Highlight,
				EventTime:   item.Time,
				TsMs:        nowMs,
				ProducedMs:  nowMs,
			}

			if err := p.producer.PublishTCBSIntradayTick(ctx, ticker, event); err != nil {
				log.Printf("[TCBSPoller] PublishTCBSIntradayTick ticker=%s: %v", ticker, err)
				continue
			}
			publishedCount++
		}

		log.Printf("[TCBSPoller] pollIntradayTicks ticker=%s — published %d ticks", ticker, publishedCount)
	}
}
