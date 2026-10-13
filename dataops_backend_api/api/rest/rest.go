// REUSE 80%: market-maker-api/api/rest.go
// Gin router setup với route groups, middleware chain, và handler registration
package rest

import (
	"log"
	"dataops-backend-api/configuration"
	"dataops-backend-api/logging"
	"dataops-backend-api/middlewares"
	"dataops-backend-api/redis"
	"dataops-backend-api/services"

	"github.com/gin-gonic/gin"
)

// NewAPI khởi tạo Gin router với toàn bộ routes.
//
// Nhận vào logger chuẩn (*log.Logger) để ghi structured log thay vì fmt.Printf.
// structuredLogger dùng cho TraceID + RequestLogger middleware.
//
// URL Structure:
//
//	POST /api/v1/login              ← public
//	GET  /api/v1/health             ← public
//	GET  /api/v1/crypto/*           ← JWT required
//	GET  /api/v1/vn-stocks/*        ← JWT required
//	GET  /api/v1/tcbs/*             ← JWT required
//	GET  /api/v1/admin/*            ← Admin only
func NewAPI(svc *services.Services, cfg *configuration.Config, redisDB0 redis.Service, logger *log.Logger, structuredLogger ...*logging.Logger) *gin.Engine {
	if logger != nil {
		logger.Println("[API] Starting REST API")
	}

	if cfg.App.Mode == "prod" {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()
	router.Use(gin.Recovery())

	// TraceID: extract X-Trace-ID header hoặc tạo mới UUID — phải đứng đầu chain
	router.Use(middlewares.TraceID())

	// RequestLogger: structured JSON log mỗi request với trace_id
	if len(structuredLogger) > 0 && structuredLogger[0] != nil {
		router.Use(middlewares.RequestLogger(structuredLogger[0]))
	}

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

	// Root health check endpoint for Docker container & load balancer probes
	router.GET("/health", authHandler.Health)

	// Crypto Analytics (Binance data)
	cryptoHandler := &CryptoHandler{AnalyticsSvc: svc.CryptoSvc}
	cryptoHandler.Register(protected.Group("/crypto"))

	// VN Stocks Analytics (VPS data)
	vnStockHandler := &VNStockHandler{VNStockSvc: svc.VNStockSvc}
	vnStockHandler.Register(protected.Group("/vn-stocks"))

	// TCBS iFlash Analytics (TCBS data: market snapshot, orderbook, BSA, intraday ticks)
	tcbsHandler := &TCBSHandler{TCBSSvc: svc.TCBSSvc}
	tcbsGroup := protected.Group("/tcbs")
	tcbsHandler.Register(tcbsGroup)

	// TCBS Account & Trading Proxy (Cash, Stock assets, Orders, Derivatives)
	tcbsAccountHandler := &TCBSAccountHandler{AccountSvc: svc.TCBSAccountSvc}
	tcbsAccountHandler.Register(tcbsGroup.Group("/account"), tcbsGroup.Group("/derivative"))
	tcbsAccountHandler.RegisterStockOrder(tcbsGroup.Group("/order"))

	// Admin
	adminHandler := &AdminHandler{Cfg: cfg}
	adminHandler.Register(admin)

	if logger != nil {
		logger.Println("[API] Routes registered")
	}
	return router
}
