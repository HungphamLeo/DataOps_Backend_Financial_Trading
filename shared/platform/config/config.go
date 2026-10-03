package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// PlatformConfig chứa cấu hình chuẩn enterprise cho toàn hệ thống
type PlatformConfig struct {
	Env         string        `json:"env"`
	ServiceName string        `json:"service_name"`
	Port        string        `json:"port"`
	LogLevel    string        `json:"log_level"`
	
	// Exchange Credentials - Binance
	Binance BinanceConfig `json:"binance"`

	// Exchange Credentials - VPS Securities
	VPS VPSConfig `json:"vps"`

	// Infrastructure
	Kafka    KafkaConfig    `json:"kafka"`
	Redis    RedisConfig    `json:"redis"`
	MySQL    MySQLConfig    `json:"mysql"`
	InfluxDB InfluxDBConfig `json:"influxdb"`
	MinIO    MinIOConfig    `json:"minio"`

	// Security
	JWT JWTConfig `json:"jwt"`
}

type BinanceConfig struct {
	APIKey    string `json:"api_key"`
	SecretKey string `json:"secret_key"`
	BaseURL   string `json:"base_url"`
	WSBaseURL string `json:"ws_base_url"`
	WSURLSpot string `json:"ws_url_spot"`
	WSURLFutures string `json:"ws_url_futures"`
}

type VPSConfig struct {
	BaseURL     string `json:"base_url"`
	WSURL       string `json:"ws_url"`
	AccountNo   string `json:"account_no"`
	ClientID    string `json:"client_id"`
	SecretKey   string `json:"secret_key"`
	AccessToken string `json:"access_token"`
}

type KafkaConfig struct {
	Brokers          []string      `json:"brokers"`
	SchemaRegistryURL string       `json:"schema_registry_url"`
	GroupID          string        `json:"group_id"`
	RequiredAcks     string        `json:"required_acks"` // "all", "1", "0"
	Idempotent       bool          `json:"idempotent"`
	TransactionID    string        `json:"transaction_id"`
	MaxRetry         int           `json:"max_retry"`
	RetryBackoff     time.Duration `json:"retry_backoff"`
	SASLEnabled      bool          `json:"sasl_enabled"`
	SASLUser         string        `json:"sasl_user"`
	SASLPassword     string        `json:"sasl_password"`
	TLSEnabled       bool          `json:"tls_enabled"`
}

type RedisConfig struct {
	Addr         string        `json:"addr"`
	Password     string        `json:"password"`
	DB           int           `json:"db"`
	DialTimeout  time.Duration `json:"dial_timeout"`
	ReadTimeout  time.Duration `json:"read_timeout"`
	WriteTimeout time.Duration `json:"write_timeout"`
	PoolSize     int           `json:"pool_size"`
}

type MySQLConfig struct {
	DSN             string        `json:"dsn"`
	MaxOpenConns    int           `json:"max_open_conns"`
	MaxIdleConns    int           `json:"max_idle_conns"`
	ConnMaxLifetime time.Duration `json:"conn_max_lifetime"`
}

type InfluxDBConfig struct {
	URL    string `json:"url"`
	Token  string `json:"token"`
	Org    string `json:"org"`
	Bucket string `json:"bucket"`
}

type MinIOConfig struct {
	Endpoint        string `json:"endpoint"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	UseSSL          bool   `json:"use_ssl"`
	Bucket          string `json:"bucket"`
}

type JWTConfig struct {
	SecretKey      string        `json:"secret_key"`
	ExpirationTime time.Duration `json:"expiration_time"`
	RefreshTTL     time.Duration `json:"refresh_ttl"`
	Issuer         string        `json:"issuer"`
}

// LoadFromEnv nạp cấu hình từ Environment Variables chuẩn hóa
func LoadFromEnv(serviceName string) (*PlatformConfig, error) {
	cfg := &PlatformConfig{
		Env:         getEnv("APP_ENV", "development"),
		ServiceName: serviceName,
		Port:        getEnv("PORT", "8080"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),

		Binance: BinanceConfig{
			APIKey:       getEnv("BINANCE_API_KEY", ""),
			SecretKey:    getEnv("BINANCE_SECRET_KEY", ""),
			BaseURL:      getEnv("BINANCE_BASE_URL", "https://api.binance.com"),
			WSBaseURL:    getEnv("BINANCE_WS_URL", "wss://stream.binance.com:9443/ws"),
			WSURLSpot:    getEnv("BINANCE_WS_URL_SPOT", "wss://stream.binance.com:9443/stream"),
			WSURLFutures: getEnv("BINANCE_FUTURES_WS_URL", "wss://fstream.binance.com/stream"),
		},

		VPS: VPSConfig{
			BaseURL:     getEnv("VPS_BASE_URL", "https://bgapidatafeed.vps.com.vn"),
			WSURL:       getEnv("VPS_WS_URL", "wss://bgapidatafeed.vps.com.vn/socket.io"),
			AccountNo:   getEnv("VPS_ACCOUNT_NO", ""),
			ClientID:    getEnv("VPS_CLIENT_ID", ""),
			SecretKey:   getEnv("VPS_SECRET_KEY", ""),
			AccessToken: getEnv("VPS_ACCESS_TOKEN", ""),
		},

		Kafka: KafkaConfig{
			Brokers:           strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092,localhost:9093,localhost:9094"), ","),
			SchemaRegistryURL: getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
			GroupID:           getEnv("KAFKA_GROUP_ID", fmt.Sprintf("%s-group", serviceName)),
			RequiredAcks:      getEnv("KAFKA_ACKS", "all"),
			Idempotent:        getEnvAsBool("KAFKA_IDEMPOTENT", true),
			TransactionID:     getEnv("KAFKA_TRANSACTION_ID", fmt.Sprintf("tx-%s", serviceName)),
			MaxRetry:          getEnvAsInt("KAFKA_MAX_RETRY", 5),
			RetryBackoff:      getEnvAsDuration("KAFKA_RETRY_BACKOFF", 100*time.Millisecond),
			SASLEnabled:       getEnvAsBool("KAFKA_SASL_ENABLED", false),
			SASLUser:          getEnv("KAFKA_SASL_USER", ""),
			SASLPassword:      getEnv("KAFKA_SASL_PASSWORD", ""),
			TLSEnabled:        getEnvAsBool("KAFKA_TLS_ENABLED", false),
		},

		Redis: RedisConfig{
			Addr:         getEnv("REDIS_ADDR", "localhost:6379"),
			Password:     getEnv("REDIS_PASSWORD", ""),
			DB:           getEnvAsInt("REDIS_DB", 0),
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
			PoolSize:     getEnvAsInt("REDIS_POOL_SIZE", 50),
		},

		MySQL: MySQLConfig{
			DSN:             getEnv("MYSQL_DSN", "root:rootpass@tcp(localhost:3306)/trading_platform?charset=utf8mb4&parseTime=True&loc=Local"),
			MaxOpenConns:    getEnvAsInt("MYSQL_MAX_OPEN_CONNS", 50),
			MaxIdleConns:    getEnvAsInt("MYSQL_MAX_IDLE_CONNS", 10),
			ConnMaxLifetime: getEnvAsDuration("MYSQL_CONN_MAX_LIFETIME", 1*time.Hour),
		},

		InfluxDB: InfluxDBConfig{
			URL:    getEnv("INFLUXDB_URL", "http://localhost:8086"),
			Token:  getEnv("INFLUXDB_TOKEN", "platform-admin-token-2024"),
			Org:    getEnv("INFLUXDB_ORG", "financial-trading"),
			Bucket: getEnv("INFLUXDB_BUCKET", "market_analytics"),
		},

		MinIO: MinIOConfig{
			Endpoint:        getEnv("MINIO_ENDPOINT", "localhost:9000"),
			AccessKeyID:     getEnv("MINIO_ACCESS_KEY", "minioadmin"),
			SecretAccessKey: getEnv("MINIO_SECRET_KEY", "minioadmin"),
			UseSSL:          getEnvAsBool("MINIO_USE_SSL", false),
			Bucket:          getEnv("MINIO_DELTA_BUCKET", "binance-datalake"),
		},

		JWT: JWTConfig{
			SecretKey:      getEnv("JWT_SECRET", "super-secure-production-jwt-secret-key-32chars"),
			ExpirationTime: getEnvAsDuration("JWT_EXPIRATION", 24*time.Hour),
			RefreshTTL:     getEnvAsDuration("JWT_REFRESH_TTL", 7*24*time.Hour),
			Issuer:         "dataops-financial-platform",
		},
	}

	return cfg, nil
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvAsInt(key string, defaultVal int) int {
	valStr := getEnv(key, "")
	if val, err := strconv.Atoi(valStr); err == nil {
		return val
	}
	return defaultVal
}

func getEnvAsBool(key string, defaultVal bool) bool {
	valStr := getEnv(key, "")
	if val, err := strconv.ParseBool(valStr); err == nil {
		return val
	}
	return defaultVal
}

func getEnvAsDuration(key string, defaultVal time.Duration) time.Duration {
	valStr := getEnv(key, "")
	if d, err := time.ParseDuration(valStr); err == nil {
		return d
	}
	return defaultVal
}
