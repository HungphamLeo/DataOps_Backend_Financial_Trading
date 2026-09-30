// LEARN: Indicator System — tách 3 tầng rõ ràng
//
// REUSE: 100% từ trading-bot-api/models/indicator.go, indicator_group.go, indicator_parameter.go
//
// Tại sao cần Indicator system trong analytics platform?
//   - User tự định nghĩa technical indicators (MA, RSI, Bollinger Bands...)
//   - Mỗi indicator có parameters (period, source, multiplier...)
//   - Groups cho phép bundle nhiều indicators thành 1 strategy profile
//
// Architecture (3 tầng):
//   Tier 1: Models (data shape + DB schema)        ← file này
//   Tier 2: Repository (CRUD)                      ← repository/indicator.go
//   Tier 3: Service (business logic + validation)  ← services/indicator/indicator.go
//
// "Fundamentals of Software Architecture" (O'Reilly):
//   Separation of concerns — mỗi layer chỉ biết về layer ngay bên dưới nó

package models

import "gorm.io/gorm"

// IndicatorGroup là container cho một nhóm indicators cùng strategy
// VD: "VWAP Strategy" = VWAP indicator + Volume indicator + ATR parameter
// LEARN: has-many relationship: 1 group → nhiều indicators + nhiều parameters
type IndicatorGroup struct {
	gorm.Model
	ShortName  string               `gorm:"type:varchar(50);uniqueIndex" json:"shortName"` // unique key
	Name       string               `gorm:"type:varchar(100);not null"   json:"name"`
	Type       uint                 `gorm:"default:0"                    json:"type"`      // 0=crypto, 1=vnstock
	Indicators []*Indicator         `gorm:"foreignKey:GroupID"           json:"indicators"`
	Parameters []*IndicatorParameter `gorm:"foreignKey:GroupID"          json:"parameters"`
	CreatedBy  uint                 `gorm:"index"                        json:"createdBy"`
}

// Indicator là một technical indicator trong group
// VD: MA (Moving Average), RSI, VWAP, ATR, Bollinger Band
type Indicator struct {
	gorm.Model
	GroupID   uint   `gorm:"index;not null"           json:"groupId"`
	ShortName string `gorm:"type:varchar(50)"         json:"shortName"` // "MA","RSI","VWAP"
	Name      string `gorm:"type:varchar(100)"        json:"name"`
	Formula   string `gorm:"type:text"                json:"formula"`   // math formula hoặc description
	IsActive  bool   `gorm:"default:true"             json:"isActive"`
}

// IndicatorParameter là tham số của 1 indicator trong group
// Tách riêng để dễ override: cùng 1 indicator nhưng params khác nhau
// VD: MA period=20 vs MA period=50 → 2 IndicatorParameter records
type IndicatorParameter struct {
	gorm.Model
	GroupID      uint    `gorm:"index;not null"           json:"groupId"`
	IndicatorID  uint    `gorm:"index"                    json:"indicatorId"` // nullable: param level
	Name         string  `gorm:"type:varchar(100)"        json:"name"`     // "period","source","multiplier"
	DefaultValue string  `gorm:"type:varchar(255)"        json:"defaultValue"` // giá trị mặc định
	CurrentValue string  `gorm:"type:varchar(255)"        json:"currentValue"` // giá trị đang dùng
	DataType     string  `gorm:"type:varchar(20)"         json:"dataType"`     // "int","float","string","bool"
	MinValue     float64 `gorm:"default:0"                json:"minValue"`
	MaxValue     float64 `gorm:"default:0"                json:"maxValue"`  // 0 = unlimited
	Description  string  `gorm:"type:text"                json:"description"`
}

// IndicatorSignal ghi lại kết quả tính toán indicator tại một thời điểm
// LEARN: Separate compute results từ configuration — không mix data + business logic
type IndicatorSignal struct {
	gorm.Model
	GroupID    uint    `gorm:"index" json:"groupId"`
	Symbol     string  `gorm:"type:varchar(20);index" json:"symbol"`
	Exchange   string  `gorm:"type:varchar(20)"       json:"exchange"`
	SignalType string  `gorm:"type:varchar(50)"       json:"signalType"` // "BUY","SELL","HOLD","NEUTRAL"
	Confidence float64 `json:"confidence"` // 0-1, confidence score của signal
	Value      float64 `json:"value"`      // giá trị indicator (e.g. MA=85.5)
	Timeframe  string  `gorm:"type:varchar(10)" json:"timeframe"` // "1m","5m","1h","1d"
}
