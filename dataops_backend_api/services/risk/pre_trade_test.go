package risk

import (
	"context"
	"testing"

	"dataops-backend-api/configuration"
	"dataops-backend-api/models"
	cryptoaccount "dataops-backend-api/services/crypto_account"
	tcbsaccount "dataops-backend-api/services/tcbs_account"
)

type mockPortfolio struct{ overview *models.PortfolioOverview }

func (m *mockPortfolio) GetOverview(context.Context, string) (*models.PortfolioOverview, error) {
	return m.overview, nil
}
func (m *mockPortfolio) GetNAV(context.Context, string) (*models.PortfolioNAVResponse, error) {
	return nil, nil
}
func (m *mockPortfolio) GetAllocations(context.Context, string) (*models.PortfolioAllocationResponse, error) {
	return nil, nil
}

type mockTCBS struct {
	tcbsaccount.Service
	cash *models.TCBSAccountCash
}

func (m *mockTCBS) GetCashInvestments(context.Context, string) (*models.TCBSAccountCash, error) {
	return m.cash, nil
}

type mockCrypto struct {
	cryptoaccount.Service
	bal *models.BinanceAccountBalanceResponse
}

func (m *mockCrypto) GetAccountBalance(context.Context) (*models.BinanceAccountBalanceResponse, error) {
	return m.bal, nil
}

func newTestSvc(cash *models.TCBSAccountCash, bal *models.BinanceAccountBalanceResponse) PreTradeService {
	cfg := &configuration.Config{}
	cfg.Risk.MaxAllocationPct = 30
	cfg.Risk.MinMarginRatioPct = 50
	pf := &mockPortfolio{overview: &models.PortfolioOverview{
		TotalNAV: 1000,
		Assets: []models.PortfolioAsset{
			{Symbol: "VCB", AssetType: "STOCK", LivePrice: 10, MarketValue: 200},
		},
	}}
	return NewPreTradeService(cfg, pf, &mockCrypto{bal: bal}, &mockTCBS{cash: cash}, nil)
}

func TestPreTrade_StockAllowed(t *testing.T) {
	svc := newTestSvc(&models.TCBSAccountCash{PurchasingPower: 500, MarginRatio: 80}, nil)
	res, err := svc.CheckPreTrade(context.Background(), &models.PreTradeCheckRequest{
		AccountNo: "A1", AssetType: "STOCK", Symbol: "VCB", Side: "NB", Quantity: 5, Price: 10,
	})
	if err != nil || !res.Allowed {
		t.Fatalf("expected allowed, got err=%v res=%+v", err, res)
	}
	if res.PostTradeAllocationPct != 25 {
		t.Errorf("expected 25%% post-trade allocation, got %v", res.PostTradeAllocationPct)
	}
}

func TestPreTrade_StockAllocationExceeded(t *testing.T) {
	svc := newTestSvc(&models.TCBSAccountCash{PurchasingPower: 5000, MarginRatio: 80}, nil)
	res, _ := svc.CheckPreTrade(context.Background(), &models.PreTradeCheckRequest{
		AccountNo: "A1", AssetType: "STOCK", Symbol: "VCB", Side: "NB", Quantity: 20, Price: 10,
	})
	if res.Allowed || len(res.Violations) != 1 {
		t.Fatalf("expected one allocation violation, got %+v", res)
	}
}

func TestPreTrade_StockBuyingPowerAndMargin(t *testing.T) {
	svc := newTestSvc(&models.TCBSAccountCash{PurchasingPower: 10, MarginRatio: 40}, nil)
	res, _ := svc.CheckPreTrade(context.Background(), &models.PreTradeCheckRequest{
		AccountNo: "A1", AssetType: "STOCK", Symbol: "VCB", Side: "NB", Quantity: 5, Price: 10,
	})
	if res.Allowed || len(res.Violations) != 2 {
		t.Fatalf("expected buying power + margin violations, got %+v", res)
	}
}

func TestPreTrade_CryptoFreeUSDT(t *testing.T) {
	bal := &models.BinanceAccountBalanceResponse{Balances: []models.BinanceBalanceItem{{Asset: "USDT", Free: 50, Total: 50}}}
	svc := newTestSvc(nil, bal)
	res, _ := svc.CheckPreTrade(context.Background(), &models.PreTradeCheckRequest{
		AssetType: "CRYPTO", Symbol: "BTCUSDT", Side: "BUY", Quantity: 1, Price: 100,
	})
	if res.Allowed {
		t.Fatalf("expected insufficient free USDT violation, got %+v", res)
	}
}

func TestPreTrade_SellAlwaysAllowed(t *testing.T) {
	svc := newTestSvc(&models.TCBSAccountCash{}, nil)
	res, _ := svc.CheckPreTrade(context.Background(), &models.PreTradeCheckRequest{
		AccountNo: "A1", AssetType: "STOCK", Symbol: "VCB", Side: "NS", Quantity: 5, Price: 10,
	})
	if !res.Allowed {
		t.Fatalf("sell must be allowed, got %+v", res)
	}
}

func TestPreTrade_InvalidInput(t *testing.T) {
	svc := newTestSvc(nil, nil)
	if _, err := svc.CheckPreTrade(context.Background(), &models.PreTradeCheckRequest{AssetType: "FX", Symbol: "X", Side: "BUY", Quantity: 1}); err == nil {
		t.Error("expected error for unsupported asset type")
	}
}
