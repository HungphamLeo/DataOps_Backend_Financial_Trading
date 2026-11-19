// Package contracts implements the ST14 in-stream data quality contract.
//
// This package validates upstream exchange data before it is published to Kafka.
// Valid records continue to the canonical topic. Invalid records must be routed
// to the contract violations DLQ with a stable reason code.
package contracts

import (
	"fmt"
	"math"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the ST14 data contract loaded from data_contracts.yaml.
type Config struct {
	Trade struct {
		// PriceMustBePositive enables validation that price is finite and > 0.
		PriceMustBePositive bool `yaml:"price_must_be_positive"`

		// QuantityMustBePositive enables validation that quantity is finite and > 0.
		QuantityMustBePositive bool `yaml:"quantity_must_be_positive"`

		// AllowedSides contains the canonical trade side values.
		AllowedSides []string `yaml:"allowed_sides"`

		// MaxClockDriftSeconds is the maximum absolute difference between
		// upstream event time and the ingestion system clock.
		MaxClockDriftSeconds int64 `yaml:"max_clock_drift_seconds"`
	} `yaml:"trade"`

	OrderBook struct {
		// BidMustBeBelowAsk enables the top-of-book bid <= ask sanity check.
		BidMustBeBelowAsk bool `yaml:"bid_must_be_below_ask"`
	} `yaml:"orderbook"`
}

// Violation is a stable machine-readable ST14 violation reason.
//
// Violation values are used in Kafka DLQ headers and Prometheus labels.
// Therefore they must not be changed without updating dashboards and alerts.
type Violation string

const (
	ViolationInvalidPrice         Violation = "invalid_price"
	ViolationInvalidQuantity      Violation = "invalid_quantity"
	ViolationInvalidSide          Violation = "invalid_side"
	ViolationClockDrift           Violation = "timestamp_clock_drift"
	ViolationInvalidOrderBook     Violation = "invalid_orderbook"
	ViolationMalformedNumeric     Violation = "malformed_numeric_value"
	ViolationMissingRequiredField Violation = "missing_required_field"
)

// PricePair contains the minimal fields required for bid/ask validation.
type PricePair struct {
	Price    float64
	Quantity float64
}

// LoadConfig reads and validates an ST14 YAML data contract.
//
// Parameters:
//   - path: absolute or relative path to data_contracts.yaml.
//
// Returns:
//   - *Config: parsed contract configuration.
//   - error: returned when the file cannot be read or the configuration is invalid.
//
// Validation rules:
//   - max_clock_drift_seconds must be greater than zero.
//   - allowed_sides is optional but, when present, must contain at least one side.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ST14 data contract %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse ST14 data contract %q: %w", path, err)
	}

	if cfg.Trade.MaxClockDriftSeconds <= 0 {
		return nil, fmt.Errorf(
			"trade.max_clock_drift_seconds must be positive",
		)
	}

	if len(cfg.Trade.AllowedSides) > 0 {
		for _, side := range cfg.Trade.AllowedSides {
			if side == "" {
				return nil, fmt.Errorf(
					"trade.allowed_sides contains an empty side",
				)
			}
		}
	}

	return &cfg, nil
}

// ValidateTrade applies zero-trust validation to a trade event.
//
// Parameters:
//   - price: upstream trade price.
//   - quantity: upstream trade quantity.
//   - side: canonical side value, either "buy" or "sell".
//   - eventTimeMS: upstream event timestamp in Unix milliseconds.
//
// Returns:
//   - []Violation: zero or more stable violation reason codes. An empty slice
//     means that the trade passed all enabled validation rules.
//
// The clock drift check uses absolute difference:
//
//	|systemTime - eventTime| <= maxClockDriftSeconds
//
// Both future timestamps and stale timestamps are rejected.
func (c *Config) ValidateTrade(
	price float64,
	quantity float64,
	side string,
	eventTimeMS int64,
) []Violation {
	violations := make([]Violation, 0, 4)

	if c.Trade.PriceMustBePositive &&
		(math.IsNaN(price) || math.IsInf(price, 0) || price <= 0) {
		violations = append(violations, ViolationInvalidPrice)
	}

	if c.Trade.QuantityMustBePositive &&
		(math.IsNaN(quantity) || math.IsInf(quantity, 0) || quantity <= 0) {
		violations = append(violations, ViolationInvalidQuantity)
	}

	if len(c.Trade.AllowedSides) > 0 {
		allowed := false
		for _, allowedSide := range c.Trade.AllowedSides {
			if side == allowedSide {
				allowed = true
				break
			}
		}

		if !allowed {
			violations = append(violations, ViolationInvalidSide)
		}
	}

	if eventTimeMS <= 0 {
		violations = append(violations, ViolationMissingRequiredField)
	} else {
		driftMS := time.Now().UnixMilli() - eventTimeMS
		if driftMS < 0 {
			driftMS = -driftMS
		}

		maxDriftMS := c.Trade.MaxClockDriftSeconds * 1000
		if driftMS > maxDriftMS {
			violations = append(violations, ViolationClockDrift)
		}
	}

	return violations
}

// ValidateOrderBook applies orderbook sanity checks at ingestion time.
//
// Parameters:
//   - bids: bid levels ordered from highest to lowest price.
//   - asks: ask levels ordered from lowest to highest price.
//
// Returns:
//   - []Violation: ViolationInvalidOrderBook if:
//   - the snapshot contains no bid or ask level;
//   - the highest bid is greater than the lowest ask;
//   - enabled validation fails due to malformed values.
//
// The rule guarantees:
//
//	highest_bid <= lowest_ask
//
// It rejects crossed or empty top-of-book snapshots.
func (c *Config) ValidateOrderBook(
	bids []PricePair,
	asks []PricePair,
) []Violation {
	if !c.OrderBook.BidMustBeBelowAsk {
		return nil
	}

	if len(bids) == 0 || len(asks) == 0 {
		return []Violation{ViolationInvalidOrderBook}
	}

	highestBid := bids[0].Price
	lowestAsk := asks[0].Price

	if math.IsNaN(highestBid) ||
		math.IsNaN(lowestAsk) ||
		math.IsInf(highestBid, 0) ||
		math.IsInf(lowestAsk, 0) ||
		highestBid <= 0 ||
		lowestAsk <= 0 {
		return []Violation{ViolationInvalidOrderBook}
	}

	if highestBid > lowestAsk {
		return []Violation{ViolationInvalidOrderBook}
	}

	for _, bid := range bids {
		if bid.Quantity <= 0 ||
			math.IsNaN(bid.Quantity) ||
			math.IsInf(bid.Quantity, 0) {
			return []Violation{ViolationInvalidOrderBook}
		}
	}

	for _, ask := range asks {
		if ask.Quantity <= 0 ||
			math.IsNaN(ask.Quantity) ||
			math.IsInf(ask.Quantity, 0) {
			return []Violation{ViolationInvalidOrderBook}
		}
	}

	return nil
}

// cd dataops_stream_producer
// go get gopkg.in/yaml.v3@v3.0.1
