// Kafka topic constants — dùng chung giữa producer và consumer
package constants

const (
	// Binance topics (canonical, dùng trong websocket producer và config)
	TopicBinanceTradeTicks        = "binance.trade.ticks"
	TopicBinanceOrderBookSnapshot = "binance.orderbook.snapshot"
	TopicBinanceLiquidation       = "binance.liquidation.events"
	TopicBinanceFundingRates      = "binance.funding.rates"
	TopicBinanceOHLCVBatch        = "binance.ohlcv.batch"

	// Aliases dùng trong Avro producer (sarama-based)
	TopicBinanceTrades       = TopicBinanceTradeTicks
	TopicBinanceOrderBook    = TopicBinanceOrderBookSnapshot
	TopicBinanceLiquidations = TopicBinanceLiquidation

	// VPS topics
	TopicVPSMarketSnapshot = "vps.market.snapshot"
	TopicVPSMarket         = TopicVPSMarketSnapshot // alias cho producer.go
	TopicVPSOrderMatched   = "vps.order.matched"

	// Alert topics (output của Spark streaming)
	TopicAlertFundingAnomaly = "binance.alerts.funding-anomaly"
	TopicAlertLiquidation    = "binance.alerts.liquidation"
	TopicAlertVPSUnusualVol  = "vps.alerts.unusual-volume"

	// Dead Letter Queue topics
	TopicDLQ                   = "platform.dlq"
	TopicBinanceTradesDLQ      = "binance.trade.ticks.dlq"
	TopicBinanceOrderBookDLQ   = "binance.orderbook.snapshot.dlq"
	TopicBinanceLiquidationsDLQ = "binance.liquidation.events.dlq"
	TopicVPSMarketDLQ          = "vps.market.snapshot.dlq"

	// Consumer group IDs
	GroupSparkStreaming = "spark-streaming-group"
	GroupAlerts        = "alert-group"
	GroupInfluxDB      = "influxdb-group"
	GroupAnalyticsAPI  = "api-query-group"
)
