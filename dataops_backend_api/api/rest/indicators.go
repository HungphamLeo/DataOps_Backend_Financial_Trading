// indicators.go — REST handlers cho Indicator Groups và ComputeSignal.
//
// Scope: Indicator Group CRUD (per-user config) + ComputeSignal (RSI/MA từ InfluxDB).
// Out of scope: Accounting handlers → xem accounting.go (commented out — not in platform scope).
//
// Routes (tất cả đều protected — JWT required):
//   GET    /api/v1/indicators          → list indicator groups của user
//   POST   /api/v1/indicators          → create group
//   GET    /api/v1/indicators/:id      → get one
//   PUT    /api/v1/indicators/:id      → update
//   DELETE /api/v1/indicators/:id      → delete
//   POST   /api/v1/indicators/:id/signal → compute RSI+MA signal
package rest

import (
	"net/http"
	"strconv"

	"dataops-backend-api/constants"
	"dataops-backend-api/middlewares"
	"dataops-backend-api/services/indicator"

	"github.com/gin-gonic/gin"
)

// ─── Indicator Group CRUD ─────────────────────────────────────────────────────

// ListIndicatorGroups GET /api/v1/indicators
func ListIndicatorGroups(svc indicator.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
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

// ─── Signal Computation ───────────────────────────────────────────────────────

// ComputeSignal POST /api/v1/indicators/:id/signal
//
// Tính RSI(14) + MA(20) signal cho 1 instrument dựa trên config của indicator group.
// Data source: InfluxDB close price series (written bởi dataops_processing VWAP job).
//
// Query params:
//
//	symbol    (required) — e.g. "BTCUSDT"
//	exchange  (optional) — default: constants.DefaultCryptoExchange
//	timeframe (optional) — default: constants.TF_1H
func ComputeSignal(svc indicator.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
			return
		}

		symbol := c.Query("symbol")
		exchange := c.DefaultQuery("exchange", constants.DefaultCryptoExchange)
		timeframe := c.DefaultQuery("timeframe", constants.TF_1H)

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
