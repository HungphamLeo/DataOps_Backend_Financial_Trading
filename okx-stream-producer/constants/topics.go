// Kafka topic constants — dùng chung giữa producer và consumer
package constants

const (
	// OKX topics
	TopicOKXTradeTicks        = "okx.trade.ticks"
	TopicOKXOrderBookSnapshot = "okx.orderbook.snapshot"
	TopicOKXLiquidation       = "okx.liquidation.events"
	TopicOKXFundingRates      = "okx.funding.rates"
	TopicOKXOHLCVBatch        = "okx.ohlcv.batch"

	// VPS topics
	TopicVPSMarketSnapshot = "vps.market.snapshot"
	TopicVPSOrderMatched   = "vps.order.matched"

	// Alert topics (output của Spark streaming)
	TopicAlertFundingAnomaly  = "okx.alerts.funding-anomaly"
	TopicAlertLiquidation     = "okx.alerts.liquidation"
	TopicAlertVPSUnusualVol   = "vps.alerts.unusual-volume"

	// Dead Letter Queue
	TopicDLQ = "platform.dlq"

	// Consumer group IDs
	GroupSparkStreaming = "spark-streaming-group"
	GroupAlerts        = "alert-group"
	GroupInfluxDB      = "influxdb-group"
	GroupAnalyticsAPI  = "api-query-group"
)
