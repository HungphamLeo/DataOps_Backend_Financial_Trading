// REUSE 80%: market-maker-api/api/rest.go
// Gin router setup với route groups, middleware chain, và handler registration
package rest

import (
	"log"
	"dataops-backend-api/configuration"
	"dataops-backend-api/middlewares"
	"dataops-backend-api/redis"
	"dataops-backend-api/services"

	"github.com/gin-gonic/gin"
)

// NewAPI khởi tạo Gin router với toàn bộ routes.
//
// Nhận vào logger chuẩn (*log.Logger) để ghi structured log thay vì fmt.Printf.
//
// URL Structure:
//
//	POST /api/v1/login              ← public
//	GET  /api/v1/health             ← public
//	GET  /api/v1/crypto/*           ← JWT required
//	GET  /api/v1/vn-stocks/*        ← JWT required
//	GET  /api/v1/tcbs/*             ← JWT required
//	GET  /api/v1/admin/*            ← Admin only
func NewAPI(svc *services.Services, cfg *configuration.Config, redisDB0 redis.Service, logger *log.Logger) *gin.Engine {
	logger.Println("[API] Starting REST API")

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
	protected.Use(middlewares.JwtAuthMiddleware(cfg.Authentication.Secret, redisDB0))

	// Admin routes
	admin := api.Group("/admin")
	admin.Use(middlewares.JwtAuthMiddleware(cfg.Authentication.Secret, redisDB0), middlewares.AdminMiddleware())

	// ─── Handlers ────────────────────────────────────────────────

	// Auth — REUSE 95%: market-maker-api/api/authentication.go
	authHandler := &AuthHandler{AuthSvc: svc.AuthSvc}
	authHandler.RegisterPublic(public)

	// Crypto Analytics (Binance data)
	cryptoHandler := &CryptoHandler{AnalyticsSvc: svc.CryptoSvc}
	cryptoHandler.Register(protected.Group("/crypto"))

	// VN Stocks Analytics (VPS data)
	vnStockHandler := &VNStockHandler{VNStockSvc: svc.VNStockSvc}
	vnStockHandler.Register(protected.Group("/vn-stocks"))

	// TCBS iFlash Analytics (TCBS data: market snapshot, orderbook, BSA, intraday ticks)
	tcbsHandler := &TCBSHandler{TCBSSvc: svc.TCBSSvc}
	tcbsHandler.Register(protected.Group("/tcbs"))

	// Admin
	adminHandler := &AdminHandler{Cfg: cfg}
	adminHandler.Register(admin)

	logger.Println("[API] Routes registered")
	return router
}
