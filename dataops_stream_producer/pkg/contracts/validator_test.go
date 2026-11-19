package contracts

import (
	"testing"
	"time"
)

func validConfig() *Config {
	var cfg Config
	cfg.Trade.PriceMustBePositive = true
	cfg.Trade.QuantityMustBePositive = true
	cfg.Trade.AllowedSides = []string{"buy", "sell"}
	cfg.Trade.MaxClockDriftSeconds = 5
	cfg.OrderBook.BidMustBeBelowAsk = true
	return &cfg
}

func TestValidateTradeRejectsInvalidPrice(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	violations := cfg.ValidateTrade(
		-1,
		1,
		"buy",
		time.Now().UnixMilli(),
	)

	for _, violation := range violations {
		if violation == ViolationInvalidPrice {
			return
		}
	}

	t.Fatal("expected invalid_price violation")
}

func TestValidateTradeRejectsZeroQuantity(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	violations := cfg.ValidateTrade(
		100,
		0,
		"buy",
		time.Now().UnixMilli(),
	)

	for _, violation := range violations {
		if violation == ViolationInvalidQuantity {
			return
		}
	}

	t.Fatal("expected invalid_quantity violation")
}

func TestValidateTradeRejectsClockDrift(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	oldEventTime := time.Now().Add(-10 * time.Second).UnixMilli()

	violations := cfg.ValidateTrade(
		100,
		1,
		"buy",
		oldEventTime,
	)

	for _, violation := range violations {
		if violation == ViolationClockDrift {
			return
		}
	}

	t.Fatal("expected timestamp_clock_drift violation")
}

func TestValidateTradeAcceptsValidTrade(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	violations := cfg.ValidateTrade(
		65000,
		0.01,
		"buy",
		time.Now().UnixMilli(),
	)

	if len(violations) != 0 {
		t.Fatalf("expected valid trade, got violations=%v", violations)
	}
}

func TestValidateOrderBookRejectsCrossedBook(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	violations := cfg.ValidateOrderBook(
		[]PricePair{{Price: 101, Quantity: 10}},
		[]PricePair{{Price: 100, Quantity: 10}},
	)

	if len(violations) != 1 ||
		violations[0] != ViolationInvalidOrderBook {
		t.Fatalf("expected invalid_orderbook violation, got=%v", violations)
	}
}

func TestValidateOrderBookAcceptsValidBook(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	violations := cfg.ValidateOrderBook(
		[]PricePair{{Price: 100, Quantity: 10}},
		[]PricePair{{Price: 101, Quantity: 10}},
	)

	if len(violations) != 0 {
		t.Fatalf("expected valid orderbook, got=%v", violations)
	}
}

// cd dataops_stream_producer
// go test ./pkg/contracts
// go test ./pkg/contracts
