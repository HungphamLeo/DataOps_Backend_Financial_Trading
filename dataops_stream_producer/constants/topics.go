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

	// TCBS topics — thay thế VPS cho streaming ingestion
	TopicTCBSMarketSnapshot  = "tcbs.market.snapshot"  // tickerCommons full snapshot
	TopicTCBSBSA             = "tcbs.bsa.events"        // Buy/Sell Analysis per ticker
	TopicTCBSIntradayTicks   = "tcbs.intraday.ticks"    // tick-by-tick intraday history

	// Aliases cho producer.go
	TopicTCBSMarket   = TopicTCBSMarketSnapshot
	TopicTCBSBSAAlias = TopicTCBSBSA

	// Alert topics (output của Spark streaming)
	TopicAlertFundingAnomaly  = "binance.alerts.funding-anomaly"
	TopicAlertLiquidation     = "binance.alerts.liquidation"
	TopicAlertTCBSBSAAnomaly  = "tcbs.alerts.bsa-anomaly"

	// Dead Letter Queue topics
	TopicDLQ                    = "platform.dlq"
	TopicBinanceTradesDLQ       = "binance.trade.ticks.dlq"
	TopicBinanceOrderBookDLQ    = "binance.orderbook.snapshot.dlq"
	TopicBinanceLiquidationsDLQ = "binance.liquidation.events.dlq"
	TopicTCBSMarketDLQ          = "tcbs.market.snapshot.dlq"
	TopicTCBSBSADLQ             = "tcbs.bsa.events.dlq"
	TopicTCBSIntradayDLQ        = "tcbs.intraday.ticks.dlq"

	// Consumer group IDs
	GroupSparkStreaming = "spark-streaming-group"
	GroupAlerts        = "alert-group"
	GroupInfluxDB      = "influxdb-group"
	GroupAnalyticsAPI  = "api-query-group"
)
