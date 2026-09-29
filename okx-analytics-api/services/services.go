// Services DI container — REUSE pattern: market-maker-api/services/services.go
// Tất cả services được khởi tạo ở đây và inject vào handlers.
// logger được truyền xuống mọi service để đảm bảo structured logging thống nhất.
package services

import (
	"okx-analytics-api/configuration"
	"okx-analytics-api/logging"
	"okx-analytics-api/redis"
	"okx-analytics-api/repository"
	"okx-analytics-api/services/analytics"
	"okx-analytics-api/services/authentication"
	"okx-analytics-api/services/vnstock"
)

// Services là DI container — inject vào tất cả HTTP handlers
type Services struct {
	AuthSvc    authentication.Service
	CryptoSvc  analytics.CryptoService
	VNStockSvc vnstock.Service
}

// NewServices khởi tạo tất cả services với dependencies được inject.
// logger được share giữa các services — tất cả ghi vào cùng 1 output stream.
// Pattern: REUSE market-maker-api/services/services.go NewServices()
func NewServices(
	repo      *repository.Gorm,
	cfg       *configuration.Config,
	redisSvc  redis.Service,
	logger    *logging.Logger,
) *Services {

	authSvc    := authentication.NewService(repo, cfg, redisSvc, logger)
	cryptoSvc  := analytics.NewCryptoService(cfg, logger)
	vnStockSvc := vnstock.NewService(cfg, logger)

	return &Services{
		AuthSvc:    authSvc,
		CryptoSvc:  cryptoSvc,
		VNStockSvc: vnStockSvc,
	}
}
