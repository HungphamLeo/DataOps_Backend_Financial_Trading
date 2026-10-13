package rest

import (
	"net/http"
	"strconv"

	"dataops-backend-api/models"
	"dataops-backend-api/services/portfolio"
	"dataops-backend-api/services/risk"

	"github.com/gin-gonic/gin"
)

// PortfolioHandler xử lý các API endpoint liên quan đến Portfolio, NAV Calculation & Risk Report
type PortfolioHandler struct {
	PortfolioSvc portfolio.Service
	RiskSvc      risk.Service
	PreTradeSvc  risk.PreTradeService
}

// Register đăng ký các routes cho Portfolio & Risk Engine
func (h *PortfolioHandler) Register(r *gin.RouterGroup) {
	r.GET("/overview", h.GetOverview)
	r.GET("/nav", h.GetNAV)
	r.GET("/allocation", h.GetAllocation)
	r.GET("/risk-report", h.GetRiskReport)
	r.POST("/pre-trade-check", h.PreTradeCheck)
}

// PreTradeCheck kiểm tra rủi ro trước khi đặt lệnh (dry-run, không gửi lệnh)
// POST /api/v1/portfolio/pre-trade-check
func (h *PortfolioHandler) PreTradeCheck(ctx *gin.Context) {
	if h.PreTradeSvc == nil {
		ctx.JSON(http.StatusServiceUnavailable, gin.H{"error": "pre-trade risk service unavailable"})
		return
	}
	var req models.PreTradeCheckRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body", "details": err.Error()})
		return
	}
	result, err := h.PreTradeSvc.CheckPreTrade(ctx, &req)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, result)
}

// GetOverview trả về tổng quan danh mục, tài sản, NAV và Unrealized PnL
// GET /api/v1/portfolio/overview?account_no=xxx
func (h *PortfolioHandler) GetOverview(ctx *gin.Context) {
	accountNo := ctx.Query("account_no")
	if h.PortfolioSvc == nil {
		ctx.JSON(http.StatusServiceUnavailable, gin.H{"error": "portfolio service unavailable"})
		return
	}

	data, err := h.PortfolioSvc.GetOverview(ctx, accountNo)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}

// GetNAV trả về tóm tắt NAV thời gian thực
// GET /api/v1/portfolio/nav?account_no=xxx
func (h *PortfolioHandler) GetNAV(ctx *gin.Context) {
	accountNo := ctx.Query("account_no")
	if h.PortfolioSvc == nil {
		ctx.JSON(http.StatusServiceUnavailable, gin.H{"error": "portfolio service unavailable"})
		return
	}

	data, err := h.PortfolioSvc.GetNAV(ctx, accountNo)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}

// GetAllocation trả về cơ cấu phân bổ tỷ trọng danh mục (%)
// GET /api/v1/portfolio/allocation?account_no=xxx
func (h *PortfolioHandler) GetAllocation(ctx *gin.Context) {
	accountNo := ctx.Query("account_no")
	if h.PortfolioSvc == nil {
		ctx.JSON(http.StatusServiceUnavailable, gin.H{"error": "portfolio service unavailable"})
		return
	}

	data, err := h.PortfolioSvc.GetAllocations(ctx, accountNo)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, data)
}

// GetRiskReport tính toán và trả về báo cáo phân tích rủi ro (VaR 95%, Max Drawdown, Sharpe, Beta)
// GET /api/v1/portfolio/risk-report?account_no=xxx&benchmark=VNINDEX&days=252
func (h *PortfolioHandler) GetRiskReport(ctx *gin.Context) {
	accountNo := ctx.Query("account_no")
	benchmark := ctx.DefaultQuery("benchmark", "VNINDEX")
	daysStr := ctx.DefaultQuery("days", "252")
	days, _ := strconv.Atoi(daysStr)
	if days <= 0 {
		days = 252
	}

	if h.RiskSvc == nil {
		ctx.JSON(http.StatusServiceUnavailable, gin.H{"error": "risk service unavailable"})
		return
	}

	report, err := h.RiskSvc.GetRiskReport(ctx, accountNo, benchmark, days)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, report)
}

// enforcePreTrade chạy pre-trade check trước khi proxy lệnh sang sàn.
// Trả về true nếu lệnh được phép đi tiếp; false nếu đã ghi response chặn lệnh.
// Khi svc == nil hoặc enforce == false thì bỏ qua (cho qua).
func enforcePreTrade(ctx *gin.Context, svc risk.PreTradeService, enforce bool, req *models.PreTradeCheckRequest) bool {
	if svc == nil || !enforce {
		return true
	}
	result, err := svc.CheckPreTrade(ctx, req)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return false
	}
	if !result.Allowed {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"error": "pre-trade risk check failed", "check": result})
		return false
	}
	return true
}
