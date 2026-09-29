// Package rest chứa tất cả Gin HTTP handlers của okx-analytics-api.
//
// File này định nghĩa AuthHandler (public auth endpoints) và AdminHandler
// (admin-only endpoints). Tất cả business logic được delegate xuống
// authentication.Service — handler chỉ đảm nhiệm: parse request, gọi service,
// trả response.
//
// REUSE 95%: market-maker-api/api/authentication.go
package rest

import (
	"net/http"
	"okx-analytics-api/services/authentication"

	"github.com/gin-gonic/gin"
)

// AuthHandler xử lý các public authentication endpoints.
// Không yêu cầu JWT — đây là các route mà client gọi để lấy token.
//
// Dependencies được inject qua field AuthSvc thay vì hardcode —
// giúp dễ mock khi viết unit tests.
type AuthHandler struct {
	AuthSvc authentication.Service
}

// RegisterPublic đăng ký tất cả public routes vào router group r.
// Tất cả routes ở đây đều KHÔNG qua JwtAuthMiddleware.
//
// Routes:
//   POST /login           → đăng nhập, trả về access_token + refresh_token
//   POST /refresh-token   → đổi refresh_token lấy access_token mới
//   POST /logout          → vô hiệu hoá session trên Redis
//   GET  /health          → health check, không cần auth
func (h *AuthHandler) RegisterPublic(r *gin.RouterGroup) {
	r.POST("/login", h.Login)
	r.POST("/refresh-token", h.RefreshToken)
	r.POST("/logout", h.Logout)
	r.GET("/health", h.Health)
}

// Health trả về {"status":"ok"} — dùng cho load balancer và readiness probe.
// GET /api/v1/health
func (h *AuthHandler) Health(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// Login xác thực username/password và trả về JWT token pair.
//
// Request body:
//
//	{ "username": "alice", "password": "secret" }
//
// Response 200:
//
//	{ "access_token": "eyJ...", "refresh_token": "eyJ..." }
//
// Response 400: credentials không hợp lệ hoặc body thiếu field.
//
// POST /api/v1/login
func (h *AuthHandler) Login(ctx *gin.Context) {
	// ShouldBindJSON parse và validate JSON body theo struct tags.
	// binding:"required" → trả về 400 tự động nếu thiếu field.
	var args struct {
		Username string `json:"username" binding:"required"`
		Password string `json:"password" binding:"required"`
	}

	if err := ctx.ShouldBindJSON(&args); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.AuthSvc.Login(args.Username, args.Password)
	if err != nil {
		// Trả 400 thay vì 401 để tránh leak thông tin (user có tồn tại không).
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, result)
}

// RefreshToken cấp access_token mới từ refresh_token còn hạn.
//
// Request body:
//
//	{ "refresh_token": "eyJ..." }
//
// Response 200:
//
//	{ "access_token": "eyJ..." }
//
// Response 401: refresh_token đã hết hạn hoặc session bị xoá (logout).
//
// POST /api/v1/refresh-token
func (h *AuthHandler) RefreshToken(ctx *gin.Context) {
	var args struct {
		RefreshToken string `json:"refresh_token" binding:"required"`
	}

	if err := ctx.ShouldBindJSON(&args); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.AuthSvc.RefreshToken(args.RefreshToken)
	if err != nil {
		ctx.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, result)
}

// Logout vô hiệu hoá session của user bằng cách xoá khỏi Redis.
// Sau khi logout, access_token cũ vẫn valid đến khi hết hạn (stateless JWT),
// nhưng JwtAuthMiddleware sẽ từ chối vì session key không còn trên Redis.
//
// Request body:
//
//	{ "refresh_token": "eyJ..." }
//
// Response 200:
//
//	{ "message": "logged out" }
//
// POST /api/v1/logout
func (h *AuthHandler) Logout(ctx *gin.Context) {
	var args struct {
		RefreshToken string `json:"refresh_token" binding:"required"`
	}

	if err := ctx.ShouldBindJSON(&args); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := h.AuthSvc.Logout(args.RefreshToken); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "logged out"})
}

// AdminHandler xử lý các admin-only endpoints.
// Tất cả routes trong handler này đều đi qua Admin middleware —
// chỉ user có ROLE_ADMIN mới được phép truy cập.
//
// Cfg giữ dạng interface{} để linh hoạt inject bất kỳ config struct nào
// mà không cần import vòng.
type AdminHandler struct {
	Cfg interface{}
}

// Register đăng ký admin routes vào router group r.
//
// Routes:
//
//	GET /api/v1/admin/health → kiểm tra admin endpoint còn sống không
func (h *AdminHandler) Register(r *gin.RouterGroup) {
	r.GET("/health", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, gin.H{"status": "ok", "role": "admin"})
	})
}
