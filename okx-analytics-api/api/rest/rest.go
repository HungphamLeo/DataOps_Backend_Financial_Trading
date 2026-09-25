// REUSE 80%: market-maker-api/api/rest.go
// Gin router setup với route groups, middleware chain, và handler registration
package rest

import (
	"fmt"
	"okx-analytics-api/configuration"
	"okx-analytics-api/middlewares"
	"okx-analytics-api/redis"
	"okx-analytics-api/services"
	"okx-analytics-api/utils"

	"github.com/gin-gonic/gin"
)

// NewAPI khởi tạo Gin router với toàn bộ routes
// URL Structure:
//   POST /api/v1/login              ← public
//   GET  /api/v1/health             ← public
//   GET  /api/v1/crypto/*           ← JWT required
//   GET  /api/v1/vn-stocks/*        ← JWT required
//   GET  /api/v1/admin/*            ← Admin only
func NewAPI(svc *services.Services, cfg *configuration.Config, redisDB0 redis.Service) *gin.Engine {
	fmt.Printf("%s [API] Starting REST API\n", utils.GetTimeNow())

	if cfg.App.Mode == "prod" {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.Default() // Logger + Recovery middleware tự động

	// REUSE: market-maker-api/middlewares/security.go
	router.Use(middlewares.Secure(cfg))

	// ─── Route Groups ────────────────────────────────────────────
	api := router.Group("/api/v1")

	// Public routes (không cần JWT)
	public := api.Group("")

	// Protected routes (cần JWT)
	protected := api.Group("")
	protected.Use(middlewares.JwtAuthMiddleware(cfg, redisDB0))

	// Admin routes
	admin := api.Group("/admin")
	admin.Use(middlewares.Admin(cfg, redisDB0))

	// ─── Handlers ────────────────────────────────────────────────

	// Auth — REUSE 95%: market-maker-api/api/authentication.go
	authHandler := &AuthHandler{AuthSvc: svc.AuthSvc}
	authHandler.RegisterPublic(public)

	// Crypto Analytics (OKX data)
	cryptoHandler := &CryptoHandler{AnalyticsSvc: svc.CryptoSvc}
	cryptoHandler.Register(protected.Group("/crypto"))

	// VN Stocks Analytics (VPS data)
	vnStockHandler := &VNStockHandler{VNStockSvc: svc.VNStockSvc}
	vnStockHandler.Register(protected.Group("/vn-stocks"))

	// Admin
	adminHandler := &AdminHandler{Cfg: cfg}
	adminHandler.Register(admin)

	fmt.Printf("%s [API] Routes registered\n", utils.GetTimeNow())
	return router
}
