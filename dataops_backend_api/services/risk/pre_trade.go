package risk

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"dataops-backend-api/configuration"
	"dataops-backend-api/logging"
	"dataops-backend-api/models"
	cryptoaccount "dataops-backend-api/services/crypto_account"
	"dataops-backend-api/services/portfolio"
	tcbsaccount "dataops-backend-api/services/tcbs_account"
)

const defaultMaxAllocationPct = 30.0

// PreTradeService kiểm tra rủi ro trước khi bắn lệnh sang TCBS / Binance.
type PreTradeService interface {
	CheckPreTrade(ctx context.Context, req *models.PreTradeCheckRequest) (*models.PreTradeCheckResult, error)
}

type preTradeService struct {
	cfg          *configuration.Config
	portfolioSvc portfolio.Service
	cryptoAccSvc cryptoaccount.Service
	tcbsAccSvc   tcbsaccount.Service
	logger       *logging.Logger
}

// NewPreTradeService khởi tạo Pre-Trade Risk Check service.
func NewPreTradeService(
	cfg *configuration.Config,
	portfolioSvc portfolio.Service,
	cryptoAccSvc cryptoaccount.Service,
	tcbsAccSvc tcbsaccount.Service,
	logger *logging.Logger,
) PreTradeService {
	return &preTradeService{
		cfg:          cfg,
		portfolioSvc: portfolioSvc,
		cryptoAccSvc: cryptoAccSvc,
		tcbsAccSvc:   tcbsAccSvc,
		logger:       logger,
	}
}

// CheckPreTrade áp dụng các rule: Max Allocation %, sức mua khả dụng (PPSE / Free quote asset)
// và ngưỡng Margin Ratio tối thiểu. Lệnh SELL luôn được cho qua.
func (s *preTradeService) CheckPreTrade(ctx context.Context, req *models.PreTradeCheckRequest) (*models.PreTradeCheckResult, error) {
	assetType := strings.ToUpper(req.AssetType)
	if assetType != "CRYPTO" && assetType != "STOCK" {
		return nil, fmt.Errorf("unsupported asset_type %q (expected CRYPTO or STOCK)", req.AssetType)
	}
	if req.Quantity <= 0 {
		return nil, fmt.Errorf("quantity must be > 0")
	}

	result := &models.PreTradeCheckResult{
		Allowed:      true,
		Violations:   []string{},
		Warnings:     []string{},
		MaxAllocPct:  s.maxAllocationPct(),
		CalculatedAt: time.Now().Unix(),
	}

	if !isBuySide(req.Side) {
		return result, nil
	}

	symbol := strings.ToUpper(req.Symbol)

	// Giá ước tính: giá đặt lệnh, hoặc giá live của vị thế đang nắm giữ (lệnh MARKET).
	overview, err := s.portfolioSvc.GetOverview(ctx, req.AccountNo)
	if err != nil {
		return nil, fmt.Errorf("fetch portfolio overview: %w", err)
	}

	var existingMV, livePrice float64
	for _, a := range overview.Assets {
		if strings.EqualFold(a.Symbol, symbol) {
			existingMV += a.MarketValue
			livePrice = a.LivePrice
		}
	}

	price := req.Price
	if price <= 0 {
		price = livePrice
	}
	if price <= 0 {
		result.Warnings = append(result.Warnings, "cannot estimate order value (no price): allocation and buying power checks skipped")
		return result, nil
	}
	result.OrderValue = round2(req.Quantity * price)

	// 1. Max Allocation % cho một mã
	if overview.TotalNAV > 0 {
		result.PostTradeAllocationPct = round2((existingMV + result.OrderValue) / overview.TotalNAV * 100.0)
		if result.PostTradeAllocationPct > result.MaxAllocPct {
			result.AddViolation(fmt.Sprintf("allocation of %s after trade %.2f%% exceeds max %.2f%%",
				symbol, result.PostTradeAllocationPct, result.MaxAllocPct))
		}
	} else {
		result.Warnings = append(result.Warnings, "portfolio NAV unavailable: allocation check skipped")
	}

	// 2. Sức mua khả dụng + Margin ratio
	switch assetType {
	case "STOCK":
		s.checkStockFunds(ctx, req.AccountNo, result)
	case "CRYPTO":
		s.checkCryptoFunds(ctx, symbol, result)
	}

	return result, nil
}

func (s *preTradeService) checkStockFunds(ctx context.Context, accountNo string, result *models.PreTradeCheckResult) {
	if s.tcbsAccSvc == nil || accountNo == "" {
		result.Warnings = append(result.Warnings, "TCBS account not available: buying power check skipped")
		return
	}
	cash, err := s.tcbsAccSvc.GetCashInvestments(ctx, accountNo)
	if err != nil || cash == nil {
		result.Warnings = append(result.Warnings, "TCBS buying power unavailable: check skipped")
		return
	}
	result.AvailableFunds = cash.PurchasingPower
	if result.OrderValue > cash.PurchasingPower {
		result.AddViolation(fmt.Sprintf("order value %.2f exceeds purchasing power %.2f", result.OrderValue, cash.PurchasingPower))
	}
	if min := s.cfg.Risk.MinMarginRatioPct; min > 0 && cash.MarginRatio > 0 && cash.MarginRatio < min {
		result.AddViolation(fmt.Sprintf("margin ratio %.2f%% below minimum %.2f%%", cash.MarginRatio, min))
	}
}

func (s *preTradeService) checkCryptoFunds(ctx context.Context, symbol string, result *models.PreTradeCheckResult) {
	if s.cryptoAccSvc == nil {
		result.Warnings = append(result.Warnings, "crypto account not available: free balance check skipped")
		return
	}
	if !strings.HasSuffix(symbol, "USDT") {
		result.Warnings = append(result.Warnings, "non-USDT quote asset: free balance check skipped")
		return
	}
	bal, err := s.cryptoAccSvc.GetAccountBalance(ctx)
	if err != nil || bal == nil {
		result.Warnings = append(result.Warnings, "Binance balance unavailable: check skipped")
		return
	}
	for _, b := range bal.Balances {
		if b.Asset == "USDT" {
			result.AvailableFunds = b.Free
			if result.OrderValue > b.Free {
				result.AddViolation(fmt.Sprintf("order value %.2f exceeds free USDT %.2f", result.OrderValue, b.Free))
			}
			return
		}
	}
	result.AvailableFunds = 0
	result.AddViolation(fmt.Sprintf("order value %.2f exceeds free USDT 0.00", result.OrderValue))
}

func (s *preTradeService) maxAllocationPct() float64 {
	if s.cfg != nil && s.cfg.Risk.MaxAllocationPct > 0 {
		return s.cfg.Risk.MaxAllocationPct
	}
	return defaultMaxAllocationPct
}

func isBuySide(side string) bool {
	switch strings.ToUpper(side) {
	case "BUY", "NB", "B":
		return true
	}
	return false
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }
