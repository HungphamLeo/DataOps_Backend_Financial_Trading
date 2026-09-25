// Services DI container — REUSE pattern: market-maker-api/services/services.go
// Tất cả services được khởi tạo ở đây và inject vào handlers
package services

import (
	"okx-analytics-api/configuration"
	"okx-analytics-api/redis"
	"okx-analytics-api/repository"
	"okx-analytics-api/services/analytics"
	"okx-analytics-api/services/authentication"
	"okx-analytics-api/services/vnstock"
)

// Services là DI container — inject vào tất cả HTTP handlers
type Services struct {
	AuthSvc     authentication.Service
	CryptoSvc   analytics.CryptoService
	VNStockSvc  vnstock.Service
}

// NewServices khởi tạo tất cả services với dependencies được inject
// Pattern: REUSE market-maker-api/services/services.go NewServices()
func NewServices(
	repo    *repository.Gorm,
	cfg     *configuration.Config,
	redisSvc redis.Service,
) *Services {

	authSvc    := authentication.NewService(repo, cfg, redisSvc)
	cryptoSvc  := analytics.NewCryptoService(cfg)
	vnStockSvc := vnstock.NewService(cfg)

	return &Services{
		AuthSvc:    authSvc,
		CryptoSvc:  cryptoSvc,
		VNStockSvc: vnStockSvc,
	}
}
