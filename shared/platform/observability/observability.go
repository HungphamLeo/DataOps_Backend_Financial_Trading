// Package observability cung cấp các công cụ monitoring và logging chuẩn
// cho toàn bộ platform (dataops-backend-api, dataops-stream-producer).
//
// Bao gồm:
//   - Structured logging qua go.uber.org/zap (JSON format, configurable level)
//   - Prometheus metrics: HTTP requests, Kafka producer/consumer, pipeline latency
//   - HTTP metrics middleware: tự động record latency và status codes
//
// Usage:
//
//	logger := observability.InitLogger("dataops-backend-api", "production", "info")
//	metrics := observability.InitMetrics("dataops")
//	router.Use(gin.WrapH(observability.HTTPMetricsMiddleware(...)))
//
// InitLogger và InitMetrics dùng sync.Once — an toàn khi gọi nhiều lần,
// chỉ khởi tạo 1 lần duy nhất trong vòng đời của process.
package observability

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	globalLogger *zap.Logger
	loggerOnce   sync.Once
)

// InitLogger khởi tạo logger có cấu trúc chuẩn production
func InitLogger(serviceName, env, levelStr string) *zap.Logger {
	loggerOnce.Do(func() {
		var zapLevel zapcore.Level
		if err := zapLevel.UnmarshalText([]byte(levelStr)); err != nil {
			zapLevel = zapcore.InfoLevel
		}

		encoderConfig := zap.NewProductionEncoderConfig()
		encoderConfig.TimeKey = "timestamp"
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		encoderConfig.EncodeDuration = zapcore.MillisDurationEncoder

		core := zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderConfig),
			zapcore.AddSync(zapcore.Lock(zapcore.AddSync(zap.NewProductionEncoderConfig()))), // standard writer
			zapLevel,
		)

		if env == "development" {
			devConfig := zap.NewDevelopmentConfig()
			devConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
			if devLogger, err := devConfig.Build(); err == nil {
				globalLogger = devLogger.With(zap.String("service", serviceName))
				return
			}
		}

		prodConfig := zap.NewProductionConfig()
		prodConfig.Level = zap.NewAtomicLevelAt(zapLevel)
		if l, err := prodConfig.Build(); err == nil {
			globalLogger = l.With(zap.String("service", serviceName))
		} else {
			globalLogger = zap.NewNop()
		}
	})
	return globalLogger
}

// GetLogger trả về logger toàn cục
func GetLogger() *zap.Logger {
	if globalLogger == nil {
		return InitLogger("unknown-service", "development", "info")
	}
	return globalLogger
}

// MetricsRegistry quản lý metrics Prometheus chuẩn cho Microservice & Ingestion Pipeline
type MetricsRegistry struct {
	HttpRequestsTotal     *prometheus.CounterVec
	HttpRequestDuration   *prometheus.HistogramVec
	KafkaMessagesProduced *prometheus.CounterVec
	KafkaMessagesConsumed *prometheus.CounterVec
	KafkaProducerErrors   *prometheus.CounterVec
	KafkaConsumerLagGauge *prometheus.GaugeVec
	PipelineProcessTime   *prometheus.HistogramVec
	ExchangeLatencyGauge  *prometheus.GaugeVec
}

var (
	metrics     *MetricsRegistry
	metricsOnce sync.Once
)

// InitMetrics khởi tạo hệ thống Prometheus metrics
func InitMetrics(namespace string) *MetricsRegistry {
	metricsOnce.Do(func() {
		m := &MetricsRegistry{
			HttpRequestsTotal: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Namespace: namespace,
					Name:      "http_requests_total",
					Help:      "Total number of HTTP requests processed",
				},
				[]string{"method", "path", "status"},
			),
			HttpRequestDuration: prometheus.NewHistogramVec(
				prometheus.HistogramOpts{
					Namespace: namespace,
					Name:      "http_request_duration_seconds",
					Help:      "HTTP request latencies in seconds",
					Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
				},
				[]string{"method", "path", "status"},
			),
			KafkaMessagesProduced: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Namespace: namespace,
					Name:      "kafka_messages_produced_total",
					Help:      "Total number of messages produced to Kafka",
				},
				[]string{"topic", "status"},
			),
			KafkaMessagesConsumed: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Namespace: namespace,
					Name:      "kafka_messages_consumed_total",
					Help:      "Total number of messages consumed from Kafka",
				},
				[]string{"topic", "group"},
			),
			KafkaProducerErrors: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Namespace: namespace,
					Name:      "kafka_producer_errors_total",
					Help:      "Total number of errors encountered when producing to Kafka",
				},
				[]string{"topic", "error_type"},
			),
			KafkaConsumerLagGauge: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: namespace,
					Name:      "kafka_consumer_lag_records",
					Help:      "Current consumer group lag in records",
				},
				[]string{"group", "topic", "partition"},
			),
			PipelineProcessTime: prometheus.NewHistogramVec(
				prometheus.HistogramOpts{
					Namespace: namespace,
					Name:      "pipeline_processing_duration_seconds",
					Help:      "Processing duration of streaming/batch jobs in seconds",
					Buckets:   []float64{.001, .005, .01, .05, .1, .5, 1, 5, 30},
				},
				[]string{"pipeline_name", "step"},
			),
			ExchangeLatencyGauge: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: namespace,
					Name:      "exchange_network_latency_ms",
					Help:      "Observed network ping/roundtrip latency to exchange in ms",
				},
				[]string{"exchange", "endpoint"},
			),
		}

		prometheus.MustRegister(
			m.HttpRequestsTotal,
			m.HttpRequestDuration,
			m.KafkaMessagesProduced,
			m.KafkaMessagesConsumed,
			m.KafkaProducerErrors,
			m.KafkaConsumerLagGauge,
			m.PipelineProcessTime,
			m.ExchangeLatencyGauge,
		)

		metrics = m
	})

	return metrics
}

func GetMetrics() *MetricsRegistry {
	if metrics == nil {
		return InitMetrics("dataops_platform")
	}
	return metrics
}

// MetricsHandler trả về standard Prometheus HTTP handler
func MetricsHandler() http.Handler {
	return promhttp.Handler()
}

// Middleware ghi nhận request metrics cho HTTP Gin / Standard
func HTTPMetricsMiddleware(next http.Handler) http.Handler {
	m := GetMetrics()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rw, r)
		duration := time.Since(start).Seconds()

		m.HttpRequestsTotal.WithLabelValues(r.Method, r.URL.Path, http.StatusText(rw.statusCode)).Inc()
		m.HttpRequestDuration.WithLabelValues(r.Method, r.URL.Path, http.StatusText(rw.statusCode)).Observe(duration)
	})
}

type statusResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *statusResponseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
