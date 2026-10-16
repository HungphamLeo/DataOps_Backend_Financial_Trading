// accounting.go — REST handlers cho Accounting (Balance, Transactions, PnL).
//
// STATUS: COMMENTED OUT — out of scope cho DataOps Financial Trading platform.
//
// Lý do: Accounting data (balance, P&L, transaction history) thuộc về trading execution layer,
// không phải data analytics platform. Giữ file để không mất code khi tích hợp
// trading bot sau này.
//
// Để re-enable: uncomment toàn bộ file và đăng ký routes trong rest.go.
//
// Data flow khi active:
//   User → GET /api/v1/accounting/balance  → AccountingService → MySQL
//   User → GET /api/v1/accounting/pnl      → AccountingService → MySQL aggregation

package rest

/*
import (
	"net/http"
	"strconv"

	"dataops-backend-api/constants"
	"dataops-backend-api/middlewares"
	"dataops-backend-api/services/accounting"

	"github.com/gin-gonic/gin"
)

// GetBalance GET /api/v1/accounting/balance?exchange=BINANCE&currency=USDT
func GetBalance(svc accounting.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		user := middlewares.GetUserFromContext(c)
		if user == nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}

		exchange := c.DefaultQuery("exchange", constants.DefaultCryptoExchange)
		currency := c.DefaultQuery("currency", constants.DefaultCurrency)

		balance, err := svc.GetBalance(user.ID, exchange, currency)
		if err != nil {
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

// GetDailyPnL GET /api/v1/accounting/pnl?exchange=BINANCE&days=30
func GetDailyPnL(svc accounting.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		user := middlewares.GetUserFromContext(c)
		if user == nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}

		exchange := c.DefaultQuery("exchange", constants.DefaultCryptoExchange)
		days, _ := strconv.Atoi(c.DefaultQuery("days", "30"))
		if days < 1 || days > constants.MaxPnLDays {
			days = constants.DefaultPnLDays
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
*/
