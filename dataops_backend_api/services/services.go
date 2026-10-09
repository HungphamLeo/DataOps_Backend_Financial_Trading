// Services DI container — REUSE pattern: market-maker-api/services/services.go
// Tất cả services được khởi tạo ở đây và inject vào handlers.
// logger được truyền xuống mọi service để đảm bảo structured logging thống nhất.
package services

import (
	"dataops-backend-api/configuration"
	"dataops-backend-api/logging"
	"dataops-backend-api/redis"
	"dataops-backend-api/repository"
	"dataops-backend-api/repository/batch"
	"dataops-backend-api/services/analytics"
	"dataops-backend-api/services/authentication"
	"dataops-backend-api/services/tcbs"
	tcbsaccount "dataops-backend-api/services/tcbs_account"
	"dataops-backend-api/services/vnstock"
)

// Services là DI container — inject vào tất cả HTTP handlers
type Services struct {
	AuthSvc        authentication.Service
	CryptoSvc      analytics.CryptoService
	VNStockSvc     vnstock.Service
	TCBSSvc        tcbs.Service
	TCBSAccountSvc tcbsaccount.Service
	BatchClient    batch.Client
}

// NewServices khởi tạo tất cả services với dependencies được inject.
func NewServices(
	repo *repository.Gorm,
	cfg *configuration.Config,
	redisSvc redis.Service,
	batchClient batch.Client,
	logger *logging.Logger,
) *Services {
	authSvc := authentication.NewService(repo, cfg, redisSvc, logger)
	cryptoSvc := analytics.NewCryptoService(cfg, redisSvc, batchClient, logger)
	vnStockSvc := vnstock.NewService(cfg, logger)
	tcbsSvc := tcbs.NewService(cfg, redisSvc, batchClient, logger)
	tcbsAccountSvc := tcbsaccount.NewService(cfg, redisSvc, logger)

	return &Services{
		AuthSvc:        authSvc,
		CryptoSvc:      cryptoSvc,
		VNStockSvc:     vnStockSvc,
		TCBSSvc:        tcbsSvc,
		TCBSAccountSvc: tcbsAccountSvc,
		BatchClient:    batchClient,
	}
}
