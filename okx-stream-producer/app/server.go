// REUSE PATTERN: market-maker-background/app/background-tradebot/server/server.go
// Khởi động toàn bộ hệ thống: config → redis → kafka → workers → graceful shutdown
package app

import (
	"context"
	"fmt"
	"log"
	"okx-stream-producer/constants"
	"okx-stream-producer/kafka"
	"okx-stream-producer/pkg/configuration"
	"okx-stream-producer/pkg/sources/okx/websocket"
	"okx-stream-producer/pkg/sources/vps/market_data"
	"okx-stream-producer/redis"
	"okx-stream-producer/utils"
	"os/signal"
	"syscall"
)

func NewServer() error {
	fmt.Printf("%s [okx-stream-producer] Starting\n", utils.GetTimeNow())

	// 1. Load config từ .env — REUSE: market-maker-api/configuration/configuration.go
	cfg, err := configuration.FromEnv()
	if err != nil {
		return fmt.Errorf("[Config] %w", err)
	}

	// 2. Redis — REUSE 100%: trading-bot-api/redis/redis.go
	redisDB0 := redis.NewServer(cfg, cfg.Redis.DB_0)
	if !redisDB0.Ping() {
		return fmt.Errorf("[Redis] connection was down")
	}

	// 3. Kafka writer — REUSE 100%: trading-bot-finance-service/kafka/writer.go
	kafkaWriter := kafka.GetWriter(cfg.Kafka.URL)
	defer kafkaWriter.Close()

	// 4. Context với graceful shutdown — REUSE: trading-bot-api/app/signal.go
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 5. Khởi động OKX WebSocket producer (goroutine per trading pair)
	okxProducer := websocket.NewOKXProducer(cfg, kafkaWriter, redisDB0)
	go func() {
		if err := okxProducer.Start(ctx); err != nil {
			log.Printf("[OKXProducer] error: %s", err.Error())
		}
	}()

	// 6. Khởi động VPS market data poller (cron trong giờ giao dịch)
	vpsPoller := market_data.NewPoller(cfg, kafkaWriter, redisDB0)
	go func() {
		if err := vpsPoller.Start(ctx); err != nil {
			log.Printf("[VPSPoller] error: %s", err.Error())
		}
	}()

	fmt.Printf("%s [okx-stream-producer] Running — topics: %v\n",
		utils.GetTimeNow(),
		[]string{constants.TopicOKXTradeTicks, constants.TopicVPSMarketSnapshot},
	)

	// 7. Block cho đến khi Ctrl+C hoặc SIGTERM
	<-ctx.Done()

	fmt.Printf("%s [okx-stream-producer] Shutting down gracefully...\n", utils.GetTimeNow())
	return nil
}
