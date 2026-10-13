package portfolio

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"dataops-backend-api/configuration"
	"dataops-backend-api/logging"
	"dataops-backend-api/models"
	"dataops-backend-api/redis"
	cryptoaccount "dataops-backend-api/services/crypto_account"
	tcbsaccount "dataops-backend-api/services/tcbs_account"
)

// Service định nghĩa interface tính toán Real-time Portfolio & NAV
type Service interface {
	GetOverview(ctx context.Context, accountNo string) (*models.PortfolioOverview, error)
	GetNAV(ctx context.Context, accountNo string) (*models.PortfolioNAVResponse, error)
	GetAllocations(ctx context.Context, accountNo string) (*models.PortfolioAllocationResponse, error)
}

type portfolioService struct {
	cfg          *configuration.Config
	redis        redis.Service
	cryptoAccSvc cryptoaccount.Service
	tcbsAccSvc   tcbsaccount.Service
	logger       *logging.Logger
}

// NewService khởi tạo Portfolio & NAV Engine Service
func NewService(
	cfg *configuration.Config,
	redisSvc redis.Service,
	cryptoAccSvc cryptoaccount.Service,
	tcbsAccSvc tcbsaccount.Service,
	logger *logging.Logger,
) Service {
	return &portfolioService{
		cfg:          cfg,
		redis:        redisSvc,
		cryptoAccSvc: cryptoAccSvc,
		tcbsAccSvc:   tcbsAccSvc,
		logger:       logger,
	}
}

// GetOverview tổng hợp toàn bộ danh mục, tính toán NAV và phân bổ tỷ trọng
func (s *portfolioService) GetOverview(ctx context.Context, accountNo string) (*models.PortfolioOverview, error) {
	cacheKey := fmt.Sprintf("portfolio_overview_%s", accountNo)
	if s.redis != nil {
		cached := s.redis.Get(cacheKey)
		if cached != "" {
			var overview models.PortfolioOverview
			if err := json.Unmarshal([]byte(cached), &overview); err == nil {
				return &overview, nil
			}
		}
	}

	var totalCash float64
	var assets []models.PortfolioAsset
	var totalAssetValue float64
	var totalCostBasis float64

	// 1. Lấy dữ liệu Crypto Balances
	if s.cryptoAccSvc != nil {
		cryptoBal, err := s.cryptoAccSvc.GetAccountBalance(ctx)
		if err == nil && cryptoBal != nil {
			for _, item := range cryptoBal.Balances {
				if item.Asset == "USDT" || item.Asset == "USD" {
					totalCash += item.Total
					continue
				}

				if item.Total <= 0 {
					continue
				}

				// Lấy live price từ Redis
				symbol := fmt.Sprintf("%sUSDT", strings.ToUpper(item.Asset))
				livePrice := s.getLiveCryptoPrice(symbol)
				if livePrice <= 0 {
					// Fallback giá ước lượng trong dev nếu không có stream live
					if item.Asset == "BTC" {
						livePrice = 65000.0
					} else if item.Asset == "ETH" {
						livePrice = 3500.0
					} else {
						livePrice = 1.0
					}
				}

				costPrice := livePrice * 0.95 // Giả định baseline cost basis nếu không có order book history
				marketValue := item.Total * livePrice
				unrealizedPnL := (livePrice - costPrice) * item.Total
				unrealizedPnLPct := 0.0
				if costPrice > 0 {
					unrealizedPnLPct = (livePrice - costPrice) / costPrice * 100.0
				}

				totalAssetValue += marketValue
				totalCostBasis += (item.Total * costPrice)

				assets = append(assets, models.PortfolioAsset{
					Symbol:           symbol,
					AssetType:        "CRYPTO",
					Quantity:         item.Total,
					CostPrice:        costPrice,
					LivePrice:        livePrice,
					MarketValue:      marketValue,
					UnrealizedPnL:    unrealizedPnL,
					UnrealizedPnLPct:  unrealizedPnLPct,
				})
			}
		}
	}

	// 2. Lấy dữ liệu TCBS Cash & Stock Assets nếu có accountNo
	if accountNo != "" && s.tcbsAccSvc != nil {
		// Cash
		tcbsCash, err := s.tcbsAccSvc.GetCashInvestments(ctx, accountNo)
		if err == nil && tcbsCash != nil {
			// TCBS cash (VND) - quy đổi tương đương hoặc cộng dồn theo đơn vị VND
			totalCash += tcbsCash.Cash
		}

		// Stock Assets
		stockAssets, err := s.tcbsAccSvc.GetStockAssets(ctx, accountNo)
		if err == nil && stockAssets != nil {
			for _, sa := range stockAssets {
				if sa.Quantity <= 0 {
					continue
				}

				livePrice := s.getLiveStockPrice(sa.Symbol)
				if livePrice <= 0 {
					livePrice = sa.MarketPrice
					if livePrice <= 0 {
						livePrice = sa.CostPrice
					}
				}

				marketVal := sa.Quantity * livePrice
				costVal := sa.Quantity * sa.CostPrice
				pnl := marketVal - costVal
				pnlPct := 0.0
				if costVal > 0 {
					pnlPct = (marketVal - costVal) / costVal * 100.0
				}

				totalAssetValue += marketVal
				totalCostBasis += costVal

				assets = append(assets, models.PortfolioAsset{
					Symbol:           sa.Symbol,
					AssetType:        "STOCK",
					Quantity:         sa.Quantity,
					CostPrice:        sa.CostPrice,
					LivePrice:        livePrice,
					MarketValue:      marketVal,
					UnrealizedPnL:    pnl,
					UnrealizedPnLPct:  pnlPct,
				})
			}
		}

		// Derivative Positions
		derivPositions, err := s.tcbsAccSvc.GetDerivativePositions(ctx, accountNo)
		if err == nil && derivPositions != nil {
			for _, dp := range derivPositions {
				if dp.NetQty == 0 {
					continue
				}
				assets = append(assets, models.PortfolioAsset{
					Symbol:           dp.Symbol,
					AssetType:        "DERIVATIVE",
					Quantity:         dp.NetQty,
					CostPrice:        dp.AvgPrice,
					LivePrice:        dp.MarketPrice,
					MarketValue:      dp.NetQty * dp.MarketPrice * 100000.0, // hợp đồng VN30F hệ số 100k
					UnrealizedPnL:    dp.FloatingPnL,
					UnrealizedPnLPct:  0.0,
				})
			}
		}
	}

	// 3. Tính Total NAV và Allocation %
	totalNAV := totalCash + totalAssetValue
	totalUnrealizedPnL := totalAssetValue - totalCostBasis
	totalUnrealizedPnLPct := 0.0
	if totalCostBasis > 0 {
		totalUnrealizedPnLPct = totalUnrealizedPnL / totalCostBasis * 100.0
	}

	for i := range assets {
		if totalNAV > 0 {
			assets[i].AllocationPct = (assets[i].MarketValue / totalNAV) * 100.0
			assets[i].AllocationPct = math.Round(assets[i].AllocationPct*100) / 100
		}
	}

	overview := &models.PortfolioOverview{
		TotalNAV:              totalNAV,
		TotalCash:             totalCash,
		TotalAssetValue:       totalAssetValue,
		TotalUnrealizedPnL:    totalUnrealizedPnL,
		TotalUnrealizedPnLPct: totalUnrealizedPnLPct,
		Currency:              "USDT",
		Assets:                assets,
		CalculatedAt:          time.Now().Unix(),
	}

	if s.redis != nil {
		_ = s.redis.Set(cacheKey, overview, 10*time.Second)
	}

	return overview, nil
}

// GetNAV trả về tóm tắt NAV thời gian thực
func (s *portfolioService) GetNAV(ctx context.Context, accountNo string) (*models.PortfolioNAVResponse, error) {
	overview, err := s.GetOverview(ctx, accountNo)
	if err != nil {
		return nil, err
	}
	return &models.PortfolioNAVResponse{
		TotalNAV:     overview.TotalNAV,
		Cash:         overview.TotalCash,
		AssetValue:   overview.TotalAssetValue,
		Currency:     overview.Currency,
		CalculatedAt: overview.CalculatedAt,
	}, nil
}

// GetAllocations trả về cơ cấu phân bổ tỷ trọng danh mục
func (s *portfolioService) GetAllocations(ctx context.Context, accountNo string) (*models.PortfolioAllocationResponse, error) {
	overview, err := s.GetOverview(ctx, accountNo)
	if err != nil {
		return nil, err
	}

	var allocations []models.AssetAllocationItem
	// Thêm allocation cho Cash
	if overview.TotalNAV > 0 && overview.TotalCash > 0 {
		cashPct := math.Round((overview.TotalCash/overview.TotalNAV)*10000) / 100
		allocations = append(allocations, models.AssetAllocationItem{
			Symbol:        "CASH",
			AssetType:     "CASH",
			MarketValue:   overview.TotalCash,
			AllocationPct: cashPct,
		})
	}

	for _, a := range overview.Assets {
		allocations = append(allocations, models.AssetAllocationItem{
			Symbol:        a.Symbol,
			AssetType:     a.AssetType,
			MarketValue:   a.MarketValue,
			AllocationPct: a.AllocationPct,
		})
	}

	return &models.PortfolioAllocationResponse{
		TotalNAV:     overview.TotalNAV,
		Allocations:  allocations,
		CalculatedAt: overview.CalculatedAt,
	}, nil
}

// Helper lấy live price của Crypto từ Redis
func (s *portfolioService) getLiveCryptoPrice(symbol string) float64 {
	if s.redis == nil {
		return 0
	}
	cacheKey := fmt.Sprintf("binance_%s_ticker", strings.ToLower(symbol))
	val := s.redis.Get(cacheKey)
	if val == "" {
		return 0
	}

	var tick struct {
		Price float64 `json:"price"`
		Px    float64 `json:"px"`
	}
	if err := json.Unmarshal([]byte(val), &tick); err == nil {
		if tick.Price > 0 {
			return tick.Price
		}
		if tick.Px > 0 {
			return tick.Px
		}
	}
	return 0
}

// Helper lấy live price của Stock từ Redis
func (s *portfolioService) getLiveStockPrice(symbol string) float64 {
	if s.redis == nil {
		return 0
	}
	cacheKey := fmt.Sprintf("tcbs_%s_ticker", strings.ToLower(symbol))
	val := s.redis.Get(cacheKey)
	if val == "" {
		return 0
	}

	var snap struct {
		MatchPrice float64 `json:"match_price"`
		Price      float64 `json:"price"`
		Close      float64 `json:"close"`
	}
	if err := json.Unmarshal([]byte(val), &snap); err == nil {
		if snap.MatchPrice > 0 {
			return snap.MatchPrice
		}
		if snap.Price > 0 {
			return snap.Price
		}
		if snap.Close > 0 {
			return snap.Close
		}
	}
	return 0
}
