package models

// ─── Risk Management Engine Models ───────────────────────────────────────────

// RiskReport là báo cáo phân tích rủi ro toàn diện cho danh mục
type RiskReport struct {
	AccountNo        string  `json:"account_no"`
	TotalNAV         float64 `json:"total_nav"`
	Benchmark        string  `json:"benchmark"`          // "VNINDEX" | "BTC" | "BLENDED"
	Days             int     `json:"days"`               // số ngày quan sát (e.g. 252)
	VaR95Parametric  float64 `json:"var_95_parametric"`   // Value at Risk 95% (parametric)
	VaR95Historical  float64 `json:"var_95_historical"`   // Value at Risk 95% (historical)
	VaR95Pct         float64 `json:"var_95_pct"`          // % NAV có rủi ro lỗ trong 1 ngày
	MaxDrawdown      float64 `json:"max_drawdown"`        // Mức sụt giảm tối đa (giá trị)
	MaxDrawdownPct   float64 `json:"max_drawdown_pct"`    // Mức sụt giảm tối đa (%)
	SharpeRatio      float64 `json:"sharpe_ratio"`        // Tỷ số Sharpe
	VolatilityDaily  float64 `json:"volatility_daily"`    // Độ biến động theo ngày (Standard Deviation)
	VolatilityAnnual float64 `json:"volatility_annual"`   // Độ biến động quy năm (Annualized Volatility)
	AnnualReturn     float64 `json:"annual_return"`       // Lợi nhuận kỳ vọng quy năm
	BenchmarkReturn  float64 `json:"benchmark_return"`    // Lợi nhuận benchmark quy năm
	Beta             float64 `json:"beta"`                // Hệ số Beta so với Benchmark
	Alpha            float64 `json:"alpha"`               // Hệ số Jensen's Alpha
	Correlation      float64 `json:"correlation"`         // Hệ số tương quan Pearson với Benchmark
	RiskLevel        string  `json:"risk_level"`          // "LOW", "MODERATE", "HIGH", "EXTREME"
	CalculatedAt     int64   `json:"calculated_at"`
}

// ─── Pre-Trade Risk Check Models ─────────────────────────────────────────────

// PreTradeCheckRequest là input kiểm tra rủi ro trước khi đặt lệnh
type PreTradeCheckRequest struct {
	AccountNo string  `json:"account_no"`
	AssetType string  `json:"asset_type" binding:"required"` // "CRYPTO" | "STOCK"
	Symbol    string  `json:"symbol" binding:"required"`
	Side      string  `json:"side" binding:"required"` // "BUY" | "SELL" | "NB" | "NS" | "B" | "S"
	Quantity  float64 `json:"quantity" binding:"required"`
	Price     float64 `json:"price,omitempty"` // 0 = lệnh thị trường, dùng giá live của vị thế hiện có
}

// PreTradeCheckResult là kết quả kiểm tra rủi ro trước khi đặt lệnh
type PreTradeCheckResult struct {
	Allowed                bool     `json:"allowed"`
	Violations             []string `json:"violations"`
	Warnings               []string `json:"warnings"`
	OrderValue             float64  `json:"order_value"`
	AvailableFunds         float64  `json:"available_funds"`
	PostTradeAllocationPct float64  `json:"post_trade_allocation_pct"`
	MaxAllocPct            float64  `json:"max_allocation_pct"`
	CalculatedAt           int64    `json:"calculated_at"`
}

// AddViolation ghi nhận một vi phạm và chặn lệnh
func (r *PreTradeCheckResult) AddViolation(msg string) {
	r.Allowed = false
	r.Violations = append(r.Violations, msg)
}

