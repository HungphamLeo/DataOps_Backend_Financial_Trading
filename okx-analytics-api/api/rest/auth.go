// Auth Handler — REUSE 95%: market-maker-api/api/authentication.go
package rest

import (
	"net/http"
	"okx-analytics-api/services/authentication"

	"github.com/gin-gonic/gin"
)

type AuthHandler struct {
	AuthSvc authentication.Service
}

func (h *AuthHandler) RegisterPublic(r *gin.RouterGroup) {
	r.POST("/login", h.Login)
	r.POST("/refresh-token", h.RefreshToken)
	r.POST("/logout", h.Logout)
	r.GET("/health", h.Health)
}

func (h *AuthHandler) Health(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *AuthHandler) Login(ctx *gin.Context) {
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
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, result)
}

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

// AdminHandler cho admin endpoints
type AdminHandler struct {
	Cfg interface{}
}

func (h *AdminHandler) Register(r *gin.RouterGroup) {
	r.GET("/health", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, gin.H{"status": "ok", "role": "admin"})
	})
}
