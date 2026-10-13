package rest

import (
	"net/http"

	"dataops-backend-api/models"
	"dataops-backend-api/services/risk"
	tcbsaccount "dataops-backend-api/services/tcbs_account"

	"github.com/gin-gonic/gin"
)

// TCBSAccountHandler xử lý các API endpoint liên quan đến tài khoản & giao dịch TCBS
type TCBSAccountHandler struct {
	AccountSvc     tcbsaccount.Service
	PreTradeSvc    risk.PreTradeService // optional: nil = không kiểm tra pre-trade
	PreTradeEnable bool
}

// Register đăng ký các routes cho TCBS Account & Trading proxy
func (h *TCBSAccountHandler) Register(accountGroup, derivGroup *gin.RouterGroup) {
	// Account Endpoints (/api/v1/tcbs/account/...)
	accountGroup.GET("/cash", h.GetCash)
	accountGroup.GET("/stock-asset", h.GetStockAsset)
	accountGroup.GET("/orders", h.GetOrders)

	// Derivative Endpoints (/api/v1/tcbs/derivative/...)
	derivGroup.GET("/positions", h.GetDerivativePositions)
	derivGroup.POST("/order/place", h.PlaceDerivativeOrder)
	derivGroup.DELETE("/order/:id", h.CancelDerivativeOrder)
}

// RegisterStockOrder đăng ký routes lệnh cổ phiếu cơ sở (/api/v1/tcbs/order)
func (h *TCBSAccountHandler) RegisterStockOrder(orderGroup *gin.RouterGroup) {
	orderGroup.POST("/place", h.PlaceStockOrder)
	orderGroup.DELETE("/:id", h.CancelStockOrder)
	orderGroup.PUT("/:id", h.ModifyStockOrder)
}

// GetCash lấy số dư tiền mặt tài khoản
// GET /api/v1/tcbs/account/cash?account_no=xxx
func (h *TCBSAccountHandler) GetCash(ctx *gin.Context) {
	accountNo := ctx.Query("account_no")
	if accountNo == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "account_no is required"})
		return
	}

	cash, err := h.AccountSvc.GetCashInvestments(ctx, accountNo)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, cash)
}

// GetStockAsset lấy danh mục cổ phiếu nắm giữ
// GET /api/v1/tcbs/account/stock-asset?account_no=xxx
func (h *TCBSAccountHandler) GetStockAsset(ctx *gin.Context) {
	accountNo := ctx.Query("account_no")
	if accountNo == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "account_no is required"})
		return
	}

	assets, err := h.AccountSvc.GetStockAssets(ctx, accountNo)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"account_no": accountNo, "assets": assets})
}

// GetOrders lấy danh sách lệnh trong ngày
// GET /api/v1/tcbs/account/orders?account_no=xxx
func (h *TCBSAccountHandler) GetOrders(ctx *gin.Context) {
	accountNo := ctx.Query("account_no")
	if accountNo == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "account_no is required"})
		return
	}

	orders, err := h.AccountSvc.GetOrders(ctx, accountNo)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"account_no": accountNo, "orders": orders})
}

// PlaceStockOrder đặt lệnh mua/bán cổ phiếu cơ sở
// POST /api/v1/tcbs/order/place
func (h *TCBSAccountHandler) PlaceStockOrder(ctx *gin.Context) {
	var req models.TCBSPlaceStockOrderRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body", "details": err.Error()})
		return
	}

	if !enforcePreTrade(ctx, h.PreTradeSvc, h.PreTradeEnable, &models.PreTradeCheckRequest{
		AccountNo: req.AccountNo, AssetType: "STOCK", Symbol: req.Symbol,
		Side: req.Side, Quantity: req.Quantity, Price: req.Price,
	}) {
		return
	}

	resp, err := h.AccountSvc.PlaceStockOrder(ctx, &req)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, resp)
}

// CancelStockOrder hủy lệnh mua/bán cổ phiếu cơ sở
// DELETE /api/v1/tcbs/order/:id?account_no=xxx
func (h *TCBSAccountHandler) CancelStockOrder(ctx *gin.Context) {
	orderID := ctx.Param("id")
	accountNo := ctx.Query("account_no")
	if orderID == "" || accountNo == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "order id and account_no are required"})
		return
	}

	resp, err := h.AccountSvc.CancelStockOrder(ctx, accountNo, orderID)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, resp)
}

// ModifyStockOrder sửa giá / khối lượng lệnh cổ phiếu cơ sở
// PUT /api/v1/tcbs/order/:id
func (h *TCBSAccountHandler) ModifyStockOrder(ctx *gin.Context) {
	orderID := ctx.Param("id")
	var req models.TCBSModifyStockOrderRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body", "details": err.Error()})
		return
	}
	if req.OrderNo == "" {
		req.OrderNo = orderID
	}

	resp, err := h.AccountSvc.ModifyStockOrder(ctx, &req)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, resp)
}

// GetDerivativePositions lấy danh sách vị thế phái sinh đang mở
// GET /api/v1/tcbs/derivative/positions?account_id=xxx
func (h *TCBSAccountHandler) GetDerivativePositions(ctx *gin.Context) {
	accountID := ctx.Query("account_id")
	if accountID == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "account_id is required"})
		return
	}

	positions, err := h.AccountSvc.GetDerivativePositions(ctx, accountID)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"account_id": accountID, "positions": positions})
}

// PlaceDerivativeOrder đặt lệnh phái sinh VN30F
// POST /api/v1/tcbs/derivative/order/place
func (h *TCBSAccountHandler) PlaceDerivativeOrder(ctx *gin.Context) {
	var req models.TCBSPlaceDerivOrderRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body", "details": err.Error()})
		return
	}

	resp, err := h.AccountSvc.PlaceDerivativeOrder(ctx, &req)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, resp)
}

// CancelDerivativeOrder hủy lệnh phái sinh VN30F
// DELETE /api/v1/tcbs/derivative/order/:id?account_no=xxx
func (h *TCBSAccountHandler) CancelDerivativeOrder(ctx *gin.Context) {
	orderID := ctx.Param("id")
	accountNo := ctx.Query("account_no")
	if orderID == "" || accountNo == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "order id and account_no are required"})
		return
	}

	resp, err := h.AccountSvc.CancelDerivativeOrder(ctx, accountNo, orderID)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, resp)
}
