// Package contracts exports ST14 validation metrics for Prometheus.
package contracts

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ContractViolationTotal counts upstream records rejected by the ST14 guard.
//
// Labels:
//   - reason: stable machine-readable violation code.
//   - source: upstream source, for example "binance_trade" or "binance_orderbook".
var ContractViolationTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "data_contract_violations_total",
		Help: "Total upstream records rejected by ST14 data contract validation",
	},
	[]string{"reason", "source"},
)

// RecordViolation increments the ST14 Prometheus counter.
//
// Parameters:
//   - reason: stable ST14 violation code.
//   - source: upstream source identifier.
//
// The function is safe to call concurrently from WebSocket goroutines.
func RecordViolation(reason, source string) {
	ContractViolationTotal.WithLabelValues(reason, source).Inc()
}
