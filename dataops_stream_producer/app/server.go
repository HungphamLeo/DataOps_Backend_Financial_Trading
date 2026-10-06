// Khởi động toàn bộ hệ thống: config → redis → kafka → workers → graceful shutdown
package app

import (
	"context"
	"dataops-stream-producer/constants"
	"dataops-stream-producer/pkg/configuration"
	"dataops-stream-producer/pkg/producer"
	binanceWS "dataops-stream-producer/pkg/sources/binance/websocket"
	tcbsMarketData "dataops-stream-producer/pkg/sources/tcbs/market_data"
	"dataops-stream-producer/redis"
	"dataops-stream-producer/utils"
	"fmt"
	"log"
	"os/signal"
	"syscall"
)

func NewServer() error {
	fmt.Printf("%s [data-stream-producer] Starting\n", utils.GetTimeNow())

	// 1. Load config từ .env
	cfg, err := configuration.FromEnv()
	if err != nil {
		return fmt.Errorf("[Config] %w", err)
	}

	// 2. Redis
	redisDB0 := redis.NewServer(cfg, cfg.Redis.DB_0)
	if !redisDB0.Ping() {
		return fmt.Errorf("[Redis] connection was down")
	}
	defer redisDB0.Close()

	// 3. Kafka Enterprise Producer (acks=all, idempotence=true, BinancePartitioner, DLQ, Schema Registry)
	if len(cfg.Kafka.Brokers) == 0 {
		return fmt.Errorf("[Config] KAFKA_URL must contain at least one broker address")
	}
	kafkaProd, err := producer.NewProducer(cfg.Kafka.Brokers, cfg.SchemaRegistry.URL)
	if err != nil {
		return fmt.Errorf("[Kafka Producer] %w", err)
	}
	defer kafkaProd.Close()

	// 4. Context với graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 5. Khởi động Binance WebSocket producer (goroutine per trading pair)
	binanceProducer := binanceWS.NewBinanceProducer(cfg, kafkaProd, redisDB0)
	go func() {
		if err := binanceProducer.Start(ctx); err != nil {
			log.Printf("[BinanceProducer] error: %s", err.Error())
		}
	}()

	// 6. Khởi động TCBS market data poller (thay thế VPS)
	// TCBS poll: market snapshot mỗi 1 phút trong giờ giao dịch + BSA/intraday EOD
	tcbsPoller := tcbsMarketData.NewPoller(cfg, kafkaProd, redisDB0)
	go func() {
		if err := tcbsPoller.Start(ctx); err != nil {
			log.Printf("[TCBSPoller] error: %s", err.Error())
		}
	}()

	fmt.Printf("%s [data-stream-producer] Running — topics: %v\n",
		utils.GetTimeNow(),
		[]string{
			constants.TopicBinanceTradeTicks,
			constants.TopicTCBSMarketSnapshot,
			constants.TopicTCBSBSA,
			constants.TopicTCBSIntradayTicks,
		},
	)

	// 7. Block cho đến khi Ctrl+C hoặc SIGTERM
	<-ctx.Done()

	fmt.Printf("%s [data-stream-producer] Shutting down gracefully...\n", utils.GetTimeNow())
	return nil
}
