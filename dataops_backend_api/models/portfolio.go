package models

// ─── Portfolio & NAV Engine Models ───────────────────────────────────────────

// PortfolioAsset đại diện cho 1 tài sản trong danh mục đầu tư
type PortfolioAsset struct {
	Symbol           string  `json:"symbol"`
	AssetType        string  `json:"asset_type"` // "CRYPTO", "STOCK", "DERIVATIVE"
	Quantity         float64 `json:"quantity"`
	CostPrice        float64 `json:"cost_price"`
	LivePrice        float64 `json:"live_price"`
	MarketValue      float64 `json:"market_value"`
	UnrealizedPnL    float64 `json:"unrealized_pnl"`
	UnrealizedPnLPct float64 `json:"unrealized_pnl_pct"`
	AllocationPct    float64 `json:"allocation_pct"`
}

// PortfolioOverview tổng hợp toàn bộ danh mục, NAV và phân bổ tài sản
type PortfolioOverview struct {
	TotalNAV              float64          `json:"total_nav"`
	TotalCash             float64          `json:"total_cash"`
	TotalAssetValue       float64          `json:"total_asset_value"`
	TotalUnrealizedPnL    float64          `json:"total_unrealized_pnl"`
	TotalUnrealizedPnLPct float64          `json:"total_unrealized_pnl_pct"`
	Currency              string           `json:"currency"` // "USDT" / "VND"
	Assets                []PortfolioAsset `json:"assets"`
	CalculatedAt          int64            `json:"calculated_at"`
}

// PortfolioNAVResponse tóm tắt NAV thời gian thực
type PortfolioNAVResponse struct {
	TotalNAV     float64 `json:"total_nav"`
	Cash         float64 `json:"cash"`
	AssetValue   float64 `json:"asset_value"`
	Currency     string  `json:"currency"`
	CalculatedAt int64   `json:"calculated_at"`
}

// AssetAllocationItem phân bổ tài sản theo phần trăm
type AssetAllocationItem struct {
	Symbol        string  `json:"symbol"`
	AssetType     string  `json:"asset_type"`
	MarketValue   float64 `json:"market_value"`
	AllocationPct float64 `json:"allocation_pct"`
}

// PortfolioAllocationResponse trả về cơ cấu phân bổ danh mục
type PortfolioAllocationResponse struct {
	TotalNAV     float64               `json:"total_nav"`
	Allocations  []AssetAllocationItem `json:"allocations"`
	CalculatedAt int64                 `json:"calculated_at"`
}
