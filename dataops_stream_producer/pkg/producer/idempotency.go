package producer

import (
    "crypto/sha256"
    "encoding/hex"
    "fmt"
)

// BuildIdempotencyKey creates a deterministic ST13 identity for a trade event.
//
// The key is derived only from immutable upstream fields, not Kafka offsets,
// producer session IDs or wall-clock timestamps. Replays of the same upstream
// trade therefore produce the same key across producer restarts.
//
// Parameters:
//   - source: upstream exchange name, for example "binance" or "tcbs"
//   - symbol: normalized trading symbol, for example "BTCUSDT"
//   - tradeID: exchange trade identifier
//   - tsMs: upstream event time in milliseconds
//
// Returns:
//   A lowercase hexadecimal SHA-256 string.
//
// It intentionally returns a hash rather than a raw concatenation to avoid
// ambiguous or unsafe key composition.
func BuildIdempotencyKey(source, symbol, tradeID string, tsMs int64) string {
    input := fmt.Sprintf("%s|%s|%s|%d", source, symbol, tradeID, tsMs)
    sum := sha256.Sum256([]byte(input))
    return hex.EncodeToString(sum[:])
}

// ApplyTradeIdempotencyKey attaches the ST13 key before publishing a trade.
//
// This function is pure apart from mutating the provided event.
//
// Parameters:
//   - event: pointer to the trade event to complete
//
// Precondition:
//   event.TradeID must identify the upstream trade uniquely. An empty trade ID
//   should only be accepted for sources that define another stable dedup identity.
func ApplyTradeIdempotencyKey(event *TradeEvent, source string) {
    event.IdempotencyKey = BuildIdempotencyKey(
        source,
        event.InstID,
        event.TradeID,
        event.TsMs,
    )
}