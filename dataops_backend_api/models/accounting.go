// REUSE: Pattern từ trading-bot-accounting + trading-bot-api/models/history.go
// LEARN: Accounting service — ghi lại mọi giao dịch tài chính để audit
//
// Medallion trong accounting:
//   - Mọi operation tài chính PHẢI có audit trail
//   - Không xóa record → chỉ soft delete (DeletedAt field của GORM)
//   - Immutable history: không UPDATE history records, chỉ INSERT mới
//
// References: "Designing Data-Intensive Applications" Ch.11 — Event Sourcing
//   Thay vì lưu trạng thái hiện tại, lưu sequence of events
//   Trạng thái hiện tại = replay tất cả events

package models

import (
	"time"

	"gorm.io/gorm"
)

// ─── Transaction Models ───────────────────────────────────────────────────────

// TransactionType phân loại các loại giao dịch
// LEARN: type alias trên string — compiler kiểm tra type an toàn
type TransactionType string

const (
	TxDeposit    TransactionType = "deposit"     // nạp tiền vào tài khoản
	TxWithdraw   TransactionType = "withdraw"    // rút tiền
	TxTradeFee   TransactionType = "trade_fee"   // phí giao dịch
	TxProfit     TransactionType = "profit"      // lợi nhuận từ giao dịch
	TxLoss       TransactionType = "loss"        // lỗ từ giao dịch
	TxFundingFee TransactionType = "funding_fee" // funding fee (perpetual futures)
	TxAdjust     TransactionType = "adjust"      // điều chỉnh số dư (admin)
)

// AccountTransaction là ledger entry cho mỗi giao dịch tài chính
// LEARN: gorm.Model embed = tự động thêm ID, CreatedAt, UpdatedAt, DeletedAt
//
//	DeletedAt với soft delete: không xóa DB, chỉ set DeletedAt != null
type AccountTransaction struct {
	gorm.Model
	UserID        uint            `gorm:"index;not null"            json:"userId"`
	Type          TransactionType `gorm:"type:varchar(20);not null" json:"type"`
	Symbol        string          `gorm:"type:varchar(20)"          json:"symbol"`   // e.g. "BTC-USDT"
	Exchange      string          `gorm:"type:varchar(20)"          json:"exchange"` // "OKX","VPS"
	Amount        float64         `gorm:"not null"                  json:"amount"`   // signed: + = credit, - = debit
	BalanceBefore float64         `gorm:"not null"                  json:"balanceBefore"`
	BalanceAfter  float64         `gorm:"not null"                  json:"balanceAfter"`
	Currency      string          `gorm:"type:varchar(10);default:'USDT'" json:"currency"`
	RefID         string          `gorm:"type:varchar(100);index"   json:"refId"`        // OKX trade ID or VPS order no
	GroupJobUUID  string          `gorm:"type:varchar(36);index"    json:"groupJobUuid"` // liên kết đến group job
	Note          string          `gorm:"type:text"                 json:"note"`
	ExecutedAt    time.Time       `gorm:"not null"                  json:"executedAt"`
}

// DailyPnL tổng kết P&L theo ngày cho từng user + exchange
// LEARN: Materialized view pattern — pre-aggregate để truy vấn nhanh
//
//	Không join nhiều bảng mỗi lần query, chỉ đọc từ bảng tổng kết
type DailyPnL struct {
	gorm.Model
	UserID      uint      `gorm:"uniqueIndex:idx_user_date_exchange" json:"userId"`
	Date        time.Time `gorm:"uniqueIndex:idx_user_date_exchange" json:"date"`
	Exchange    string    `gorm:"type:varchar(20);uniqueIndex:idx_user_date_exchange" json:"exchange"`
	TotalProfit float64   `json:"totalProfit"`
	TotalLoss   float64   `json:"totalLoss"`
	TradeFee    float64   `json:"tradeFee"`
	FundingFee  float64   `json:"fundingFee"`
	NetPnL      float64   `json:"netPnL"` // = TotalProfit + TotalLoss - TradeFee - FundingFee
	TradeCount  int       `json:"tradeCount"`
}

// AccountBalance số dư hiện tại của user theo exchange + currency
// LEARN: Snapshot pattern — lưu trạng thái hiện tại để không cần replay history
type AccountBalance struct {
	gorm.Model
	UserID    uint      `gorm:"uniqueIndex:idx_balance" json:"userId"`
	Exchange  string    `gorm:"type:varchar(20);uniqueIndex:idx_balance" json:"exchange"`
	Currency  string    `gorm:"type:varchar(10);uniqueIndex:idx_balance" json:"currency"`
	Available float64   `json:"available"` // số dư có thể dùng được
	Locked    float64   `json:"locked"`    // đang bị khóa bởi lệnh đang mở
	Total     float64   `json:"total"`     // = Available + Locked
	UpdatedAt time.Time `json:"updatedAt"`
}

// ExecutionLog ghi lại mọi action hệ thống (không chỉ financial)
// REUSE: Từ market-maker-api/models/strategies.go ErrorLogger
// LEARN: Audit log pattern — immutable record cho debugging và compliance
type ExecutionLog struct {
	gorm.Model
	Service    string `gorm:"type:varchar(50)"  json:"service"` // "dataops-stream-producer","okx-analytics-api"
	Level      string `gorm:"type:varchar(10)"  json:"level"`   // "INFO","WARN","ERROR"
	Action     string `gorm:"type:varchar(100)" json:"action"`  // "OKX_CONNECT","VPS_SESSION_REFRESH"
	Message    string `gorm:"type:text"         json:"message"`
	UserID     *uint  `gorm:"index"             json:"userId,omitempty"` // nullable — system actions không có user
	StackTrace string `gorm:"type:text"         json:"stackTrace,omitempty"`
}
