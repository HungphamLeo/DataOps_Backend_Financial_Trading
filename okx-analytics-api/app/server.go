// REUSE 75%: trading-bot-api/app/server.go
// Khởi động HTTP server với JWT auth, Redis, DB, gRPC clients
package app

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"okx-analytics-api/api/rest"
	"okx-analytics-api/configuration"
	"okx-analytics-api/middlewares"
	"okx-analytics-api/redis"
	"okx-analytics-api/repository"
	"okx-analytics-api/services"
	"okx-analytics-api/utils"
	"os/signal"
	"syscall"
	"time"
)

const port = 8080

func NewServer() error {
	fmt.Printf("%s [okx-analytics-api] Starting v1.0.0\n", utils.GetTimeNow())

	// 1. Config từ .env
	cfg, err := configuration.FromEnv()
	if err != nil {
		return fmt.Errorf("[Config] %w", err)
	}

	// 2. Database (MySQL) — REUSE: market-maker-api/repository/repository.go
	repo, err := repository.InitializeDatabase(cfg)
	if err != nil {
		return fmt.Errorf("[Database] %w", err)
	}

	// 3. Redis — REUSE 100%: trading-bot-api/redis/redis.go
	redisDB0 := redis.NewServer(cfg, cfg.Redis.DB_0)
	if !redisDB0.Ping() {
		return fmt.Errorf("[Redis] connection was down")
	}

	// 4. Middlewares JWT sử dụng Redis session — REUSE: market-maker-api/middlewares/auth.go
	_ = middlewares.JwtAuthMiddleware // registered in rest.go

	// 5. Services — DI pattern: REUSE market-maker-api/services/services.go
	svc := services.NewServices(repo, cfg, redisDB0)

	// 6. HTTP Server — REUSE: trading-bot-api/app/server.go
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: rest.NewAPI(svc, cfg, redisDB0),
	}

	go func() {
		fmt.Printf("%s [okx-analytics-api] HTTP server running on port %d\n", utils.GetTimeNow(), port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[HTTP] listen error: %s", err)
		}
	}()

	// Block cho đến khi SIGTERM / Ctrl+C
	<-ctx.Done()

	// Graceful shutdown với 5s timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[HTTP] forced shutdown: %s", err)
	}

	fmt.Printf("%s [okx-analytics-api] Stopped\n", utils.GetTimeNow())
	return nil
}
