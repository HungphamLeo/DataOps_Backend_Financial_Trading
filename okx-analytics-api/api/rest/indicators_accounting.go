// LEARN: REST API handlers cho Indicators và Accounting
//
// REUSE: Handler pattern từ trading-bot-api/api/ và market-maker-api/api/
//
// RESTful API design conventions:
//   GET    /api/v1/indicators          → list
//   POST   /api/v1/indicators          → create
//   GET    /api/v1/indicators/:id      → get one
//   PUT    /api/v1/indicators/:id      → update
//   DELETE /api/v1/indicators/:id      → delete
//
// HTTP Status codes:
//   200 OK             → success GET
//   201 Created        → success POST
//   400 Bad Request    → invalid input (user error)
//   401 Unauthorized   → missing/invalid auth
//   403 Forbidden      → no permission
//   404 Not Found      → resource doesn't exist
//   500 Internal Error → server-side bug

package rest

import (
	"net/http"
	"okx-analytics-api/middlewares"
	"okx-analytics-api/services/accounting"
	"okx-analytics-api/services/indicator"
	"strconv"

	"github.com/gin-gonic/gin"
)

// ─── Indicator Handlers ───────────────────────────────────────────────────────

// ListIndicatorGroups GET /api/v1/indicators
func ListIndicatorGroups(svc indicator.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		// LEARN: Lấy user từ context (set bởi JwtAuthMiddleware)
		user := middlewares.GetUserFromContext(c)
		if user == nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}

		groups, err := svc.ListGroups(user.ID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"data":  groups,
			"count": len(groups),
		})
	}
}

// GetIndicatorGroup GET /api/v1/indicators/:id
func GetIndicatorGroup(svc indicator.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		user := middlewares.GetUserFromContext(c)
		if user == nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}

		// LEARN: c.Param — lấy URL path parameter
		//        /api/v1/indicators/42 → c.Param("id") = "42"
		id, err := strconv.ParseUint(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
			return
		}

		group, err := svc.GetGroup(uint(id), user.ID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"data": group})
	}
}

// CreateIndicatorGroup POST /api/v1/indicators
func CreateIndicatorGroup(svc indicator.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		user := middlewares.GetUserFromContext(c)
		if user == nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}

		// LEARN: c.ShouldBindJSON — parse JSON body và validate binding tags
		//        binding:"required" → 400 nếu thiếu field
		var req indicator.CreateGroupRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		group, err := svc.CreateGroup(req, user.ID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusCreated, gin.H{
			"message": "indicator group created",
			"data":    group,
		})
	}
}

// UpdateIndicatorGroup PUT /api/v1/indicators/:id
func UpdateIndicatorGroup(svc indicator.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		user := middlewares.GetUserFromContext(c)
		if user == nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}

		id, err := strconv.ParseUint(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
			return
		}

		var req indicator.UpdateGroupRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		group, err := svc.UpdateGroup(uint(id), req, user.ID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message": "indicator group updated",
			"data":    group,
		})
	}
}

// DeleteIndicatorGroup DELETE /api/v1/indicators/:id
func DeleteIndicatorGroup(svc indicator.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		user := middlewares.GetUserFromContext(c)
		if user == nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}

		id, err := strconv.ParseUint(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
			return
		}

		if err := svc.DeleteGroup(uint(id), user.ID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "deleted"})
	}
}

// ComputeSignal POST /api/v1/indicators/:id/signal
func ComputeSignal(svc indicator.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
			return
		}

		// LEARN: c.Query — lấy URL query string parameter
		//        /api/v1/indicators/1/signal?symbol=BTC-USDT&timeframe=1h
		symbol := c.Query("symbol")
		exchange := c.DefaultQuery("exchange", "OKX")
		timeframe := c.DefaultQuery("timeframe", "1h")

		if symbol == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "symbol required"})
			return
		}

		signal, err := svc.ComputeSignal(uint(id), symbol, exchange, timeframe)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"data": signal})
	}
}

// ─── Accounting Handlers ──────────────────────────────────────────────────────

// GetBalance GET /api/v1/accounting/balance?exchange=OKX&currency=USDT
func GetBalance(svc accounting.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		user := middlewares.GetUserFromContext(c)
		if user == nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}

		exchange := c.DefaultQuery("exchange", "OKX")
		currency := c.DefaultQuery("currency", "USDT")

		balance, err := svc.GetBalance(user.ID, exchange, currency)
		if err != nil {
			// Not found = user chưa có balance record → return zeros
			c.JSON(http.StatusOK, gin.H{
				"data": map[string]interface{}{
					"userId":    user.ID,
					"exchange":  exchange,
					"currency":  currency,
					"available": 0,
					"locked":    0,
					"total":     0,
				},
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{"data": balance})
	}
}

// GetTransactionHistory GET /api/v1/accounting/transactions?page=1&pageSize=20
func GetTransactionHistory(svc accounting.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		user := middlewares.GetUserFromContext(c)
		if user == nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}

		// LEARN: strconv.Atoi cho query params — phải handle error
		//        c.DefaultQuery trả về string, cần parse manually
		page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
		pageSize, _ := strconv.Atoi(c.DefaultQuery("pageSize", "20"))

		if page < 1 {
			page = 1
		}
		if pageSize < 1 || pageSize > 100 {
			pageSize = 20
		}

		txs, total, err := svc.GetTransactionHistory(user.ID, page, pageSize)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// LEARN: Pagination response format — chuẩn cho tất cả list APIs
		c.JSON(http.StatusOK, gin.H{
			"data": txs,
			"pagination": gin.H{
				"page":       page,
				"pageSize":   pageSize,
				"total":      total,
				"totalPages": (total + int64(pageSize) - 1) / int64(pageSize),
			},
		})
	}
}

// GetDailyPnL GET /api/v1/accounting/pnl?exchange=OKX&days=30
func GetDailyPnL(svc accounting.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		user := middlewares.GetUserFromContext(c)
		if user == nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}

		exchange := c.DefaultQuery("exchange", "OKX")
		days, _ := strconv.Atoi(c.DefaultQuery("days", "30"))
		if days < 1 || days > 365 {
			days = 30
		}

		pnl, err := svc.GetDailyPnL(user.ID, exchange, days)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"data":     pnl,
			"exchange": exchange,
			"days":     days,
		})
	}
}
