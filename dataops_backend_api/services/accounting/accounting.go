// LEARN: Accounting Service — quản lý transactions tài chính
//
// REUSE: Patterns từ trading-bot-accounting + trading-bot-api/models/history.go
//
// Event Sourcing pattern (từ "Designing Data-Intensive Applications" Ch.11):
//   - Không update balance trực tiếp
//   - Append-only transaction log → balance = sum of all transactions
//   - Có thể replay lịch sử để audit, debug, rollback
//
// Trong đây dùng simplified version:
//   - AccountTransaction = event log (append-only)
//   - AccountBalance = current state snapshot (để query nhanh)
//   - DailyPnL = daily aggregate (pre-computed cho reporting)

package accounting

import (
	"fmt"
	"okx-analytics-api/constants"
	"okx-analytics-api/logging"
	"okx-analytics-api/models"
	"okx-analytics-api/repository"
	"time"
)

// Service interface cho accounting operations
type Service interface {
	RecordTrade(userID uint, symbol, exchange string, amount, fee float64, refID string) error
	// exchange được truyền tường minh — không hardcode "OKX"
	RecordFundingFee(userID uint, symbol, exchange string, amount float64, refID string) error
	GetBalance(userID uint, exchange, currency string) (*models.AccountBalance, error)
	GetTransactionHistory(userID uint, page, pageSize int) ([]models.AccountTransaction, int64, error)
	GetDailyPnL(userID uint, exchange string, days int) ([]models.DailyPnL, error)
	UpdateDailyPnL(userID uint, exchange string, date time.Time) error
}

type accountingService struct {
	repo   repository.Repository
	logger *logging.Logger
}

func NewService(repo repository.Repository, logger *logging.Logger) Service {
	return &accountingService{repo: repo, logger: logger}
}

// RecordTrade ghi lại 1 trade và cập nhật balance + daily P&L
// LEARN: Orchestration — 1 business operation = nhiều repo calls
//        Service layer biết "sequence", repository chỉ biết "1 operation"
func (s *accountingService) RecordTrade(
	userID uint, symbol, exchange string, amount, fee float64, refID string,
) error {
	// Bước 1: Lấy balance hiện tại
	// currency dùng DefaultCurrency — toàn bộ accounting layer quy về USDT
	bal, _ := s.repo.GetBalance(userID, exchange, constants.DefaultCurrency)
	balanceBefore := 0.0
	if bal != nil {
		balanceBefore = bal.Total
	}

	balanceAfter := balanceBefore + amount - fee

	// Bước 2: Xác định loại transaction (profit hay loss)
	txType := models.TxProfit
	if amount < 0 {
		txType = models.TxLoss
	}

	// Bước 3: Ghi transaction record (append-only, immutable)
	tx := &models.AccountTransaction{
		UserID:        userID,
		Type:          txType,
		Symbol:        symbol,
		Exchange:      exchange,
		Amount:        amount,
		BalanceBefore: balanceBefore,
		BalanceAfter:  balanceAfter,
		Currency:      constants.DefaultCurrency,
		RefID:         refID,
		ExecutedAt:    time.Now(),
	}
	if err := s.repo.RecordTransaction(tx); err != nil {
		s.logger.Error("RecordTrade", "failed to record trade transaction", map[string]interface{}{
			"userID": userID, "symbol": symbol, "exchange": exchange, "refID": refID, "err": err.Error(),
		})
		return fmt.Errorf("record trade transaction: %w", err)
	}

	// Bước 4: Ghi fee record riêng (để phân tích chi phí trading)
	if fee > 0 {
		feeTx := &models.AccountTransaction{
			UserID:        userID,
			Type:          models.TxTradeFee,
			Symbol:        symbol,
			Exchange:      exchange,
			Amount:        -fee, // fee là chi phí → negative
			BalanceBefore: balanceAfter,
			BalanceAfter:  balanceAfter - fee,
			Currency:      constants.DefaultCurrency,
			RefID:         refID + "_fee",
			ExecutedAt:    time.Now(),
		}
		if err := s.repo.RecordTransaction(feeTx); err != nil {
			s.logger.Error("RecordTrade", "failed to record fee transaction", map[string]interface{}{
				"userID": userID, "symbol": symbol, "exchange": exchange, "refID": refID, "err": err.Error(),
			})
			return fmt.Errorf("record fee transaction: %w", err)
		}
		balanceAfter -= fee
	}

	// Bước 5: Update balance snapshot
	newBal := &models.AccountBalance{
		UserID:    userID,
		Exchange:  exchange,
		Currency:  constants.DefaultCurrency,
		Available: balanceAfter,
		Total:     balanceAfter,
		UpdatedAt: time.Now(),
	}
	if err := s.repo.UpsertBalance(newBal); err != nil {
		s.logger.Error("RecordTrade", "failed to update balance", map[string]interface{}{
			"userID": userID, "exchange": exchange, "err": err.Error(),
		})
		return fmt.Errorf("update balance: %w", err)
	}

	s.logger.Info("RecordTrade", "trade recorded", map[string]interface{}{
		"userID": userID, "symbol": symbol, "exchange": exchange,
		"amount": amount, "fee": fee, "balanceAfter": balanceAfter,
	})
	return nil
}

// RecordFundingFee ghi lại funding fee của perpetual futures.
// exchange được truyền vào thay vì hardcode "OKX" — hỗ trợ multi-exchange sau này.
// LEARN: Funding fee khác trade fee: xảy ra mỗi 8 giờ, có thể dương hoặc âm
func (s *accountingService) RecordFundingFee(userID uint, symbol, exchange string, amount float64, refID string) error {
	bal, _ := s.repo.GetBalance(userID, exchange, constants.DefaultCurrency)
	balanceBefore := 0.0
	if bal != nil {
		balanceBefore = bal.Total
	}

	tx := &models.AccountTransaction{
		UserID:        userID,
		Type:          models.TxFundingFee,
		Symbol:        symbol,
		Exchange:      exchange,
		Amount:        amount,
		BalanceBefore: balanceBefore,
		BalanceAfter:  balanceBefore + amount,
		Currency:      constants.DefaultCurrency,
		RefID:         refID,
		ExecutedAt:    time.Now(),
		Note:          exchange + " perpetual funding fee",
	}

	if err := s.repo.RecordTransaction(tx); err != nil {
		s.logger.Error("RecordFundingFee", "failed to record funding fee", map[string]interface{}{
			"userID": userID, "symbol": symbol, "exchange": exchange, "refID": refID, "err": err.Error(),
		})
		return err
	}

	s.logger.Info("RecordFundingFee", "funding fee recorded", map[string]interface{}{
		"userID": userID, "symbol": symbol, "exchange": exchange, "amount": amount,
	})
	return nil
}

func (s *accountingService) GetBalance(userID uint, exchange, currency string) (*models.AccountBalance, error) {
	return s.repo.GetBalance(userID, exchange, currency)
}

func (s *accountingService) GetTransactionHistory(userID uint, page, pageSize int) ([]models.AccountTransaction, int64, error) {
	return s.repo.GetTransactionsByUser(userID, page, pageSize)
}

func (s *accountingService) GetDailyPnL(userID uint, exchange string, days int) ([]models.DailyPnL, error) {
	return s.repo.GetDailyPnL(userID, exchange, days)
}

// UpdateDailyPnL aggregate transactions trong ngày thành DailyPnL record
// LEARN: Materialized view pattern — gọi cuối ngày hoặc qua cron job
func (s *accountingService) UpdateDailyPnL(userID uint, exchange string, date time.Time) error {
	txs, _, err := s.repo.GetTransactionsByUser(userID, 1, 10000) // simplified
	if err != nil {
		s.logger.Error("UpdateDailyPnL", "failed to fetch transactions", map[string]interface{}{
			"userID": userID, "exchange": exchange, "date": date.Format("2006-01-02"), "err": err.Error(),
		})
		return err
	}

	pnl := &models.DailyPnL{
		UserID:   userID,
		Date:     date,
		Exchange: exchange,
	}

	dayStart := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.Add(24 * time.Hour)

	for _, tx := range txs {
		if tx.ExecutedAt.Before(dayStart) || tx.ExecutedAt.After(dayEnd) {
			continue
		}
		switch tx.Type {
		case models.TxProfit:
			pnl.TotalProfit += tx.Amount
			pnl.TradeCount++
		case models.TxLoss:
			pnl.TotalLoss += tx.Amount
			pnl.TradeCount++
		case models.TxTradeFee:
			pnl.TradeFee += (-tx.Amount) // fee là negative → convert positive
		case models.TxFundingFee:
			pnl.FundingFee += (-tx.Amount)
		}
	}

	pnl.NetPnL = pnl.TotalProfit + pnl.TotalLoss - pnl.TradeFee - pnl.FundingFee

	return s.repo.UpsertBalance(&models.AccountBalance{
		UserID:   userID,
		Exchange: exchange,
	})
}
