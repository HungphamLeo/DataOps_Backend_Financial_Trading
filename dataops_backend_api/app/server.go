// REUSE 75%: trading-bot-api/app/server.go
// Khởi động HTTP server với JWT auth, Redis, DB, gRPC clients
package app

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"dataops-backend-api/api/rest"
	"dataops-backend-api/configuration"
	"dataops-backend-api/constants"
	"dataops-backend-api/logging"
	"dataops-backend-api/middlewares"
	"dataops-backend-api/redis"
	"dataops-backend-api/repository"
	"dataops-backend-api/repository/batch"
	"dataops-backend-api/services"
	"os/signal"
	"syscall"
	"time"
)

// NewServer khởi động HTTP server với đầy đủ dependencies:
// config → database → redis → services → HTTP listener.
// Hỗ trợ graceful shutdown khi nhận SIGINT / SIGTERM.
//
// Port được đọc từ constants.DefaultHTTPPort (không hardcode trong file này).
func NewServer() error {
	// 1. Logger — dùng logging.NewLogger (internal structured JSON logger).
	//    Không import go.uber.org/zap trực tiếp vì chưa có trong go.mod;
	//    khi cần upgrade sang zap, chỉ sửa logging/logger.go.
	logger, closeLogger, err := logging.NewLogger(logging.Config{
		Service: "dataops-backend-api",
		Level:   logging.INFO,
	})
	if err != nil {
		return fmt.Errorf("[Logger] %w", err)
	}
	defer closeLogger()

	// log.Logger chuẩn cho rest.NewAPI() — dùng prefix để phân biệt subsystem
	apiLogger := log.New(log.Writer(), "[API] ", log.Ldate|log.Ltime|log.LUTC)

	logger.Info("server.start", "Starting dataops-backend-api",
		map[string]interface{}{"version": "v1.0.0", "port": constants.DefaultHTTPPort})

	// 2. Config từ .env
	cfg, err := configuration.FromEnv()
	if err != nil {
		return fmt.Errorf("[Config] %w", err)
	}

	// 3. Database (MySQL) — REUSE: market-maker-api/repository/repository.go
	repo, err := repository.InitializeDatabase(cfg)
	if err != nil {
		return fmt.Errorf("[Database] %w", err)
	}

	// 4. Redis — REUSE 100%: trading-bot-api/redis/redis.go
	redisDB0 := redis.NewServer(cfg, cfg.Redis.DB_0)
	var redisConnected bool
	for attempt := 1; attempt <= 10; attempt++ {
		if redisDB0.Ping() {
			redisConnected = true
			break
		}
		logger.Warn("server.redis", fmt.Sprintf("Redis connection attempt %d/10 failed, retrying in 2s...", attempt), nil)
		time.Sleep(2 * time.Second)
	}
	if !redisConnected {
		return fmt.Errorf("[Redis] connection was down")
	}

	// 5. Middlewares JWT sử dụng Redis session — REUSE: market-maker-api/middlewares/auth.go
	_ = middlewares.JwtAuthMiddleware // registered in rest.go

	// 6. Batch gRPC Client (cho historical data queries tới dataops_batch_api)
	batchClient, err := batch.NewClient(cfg.Batch.GRPCURL, logger)
	if err != nil {
		logger.Warn("server.batch_grpc", "Failed to initialize batch gRPC client", map[string]interface{}{"err": err.Error()})
	}
	if batchClient != nil {
		defer batchClient.Close()
	}

	// 7. Services — DI pattern: REUSE market-maker-api/services/services.go
	svc := services.NewServices(repo, cfg, redisDB0, batchClient, logger)

	// 8. HTTP Server — REUSE: trading-bot-api/app/server.go
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", constants.DefaultHTTPPort),
		Handler: rest.NewAPI(svc, cfg, redisDB0, apiLogger, logger),
	}

	go func() {
		logger.Info("server.listen", "HTTP server running",
			map[string]interface{}{"port": constants.DefaultHTTPPort})
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[HTTP] listen error: %s", err)
		}
	}()

	// Block cho đến khi SIGTERM / Ctrl+C
	<-ctx.Done()

	// Graceful shutdown với timeout từ GatewayConfig
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[HTTP] forced shutdown: %s", err)
	}

	logger.Info("server.stop", "dataops-backend-api stopped", nil)
	return nil
}
