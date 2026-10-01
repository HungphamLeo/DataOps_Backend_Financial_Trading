// LEARN: Configuration Management — centralized config loading
//
// 12-Factor App Methodology (factor III: Config):
//   "Store config in the environment"
//   - Không hardcode config trong code
//   - Không commit secrets vào git
//   - Different config per environment (dev/staging/prod) via env vars
//
// Config hierarchy (priority cao → thấp):
//   1. Environment variables (highest — production secrets)
//   2. .env file (local development only, gitignored)
//   3. Default values (fallback, safe defaults)
//
// Secret management levels:
//   Level 1: .env files (development) — đủ cho học tập
//   Level 2: Docker secrets (staging)
//   Level 3: AWS Secrets Manager / HashiCorp Vault (production)

package configuration

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// Config là master config struct cho toàn bộ analytics-api
// LEARN: Nested structs cho config — group related settings
type Config struct {
	App        AppConfig
	Database   DatabaseConfig
	Redis      RedisConfig
	Kafka      KafkaConfig
	InfluxDB   InfluxDBConfig
	MinIO      MinIOConfig
	JWT        JWTConfig
	VPS        VPSConfig
	BINANCE    BINANCEConfig
	Monitoring MonitoringConfig
}

type AppConfig struct {
	Name        string // "binance-analytics-api"
	Version     string // "1.0.0"
	Environment string // "development","staging","production"
	Port        int    // HTTP port
	Debug       bool   // Gin debug mode
}

type DatabaseConfig struct {
	Host     string
	Port     int
	Name     string
	User     string
	Password string // SENSITIVE: không bao giờ log
	DSN      string // computed field: "user:password@tcp(host:port)/dbname?..."
}

type RedisConfig struct {
	Address  string // "localhost:6379"
	Password string // SENSITIVE
	DB       int    // 0-15
}

type KafkaConfig struct {
	Brokers        []string // ["kafka1:9092","kafka2:9092","kafka3:9092"]
	ConsumerGroup  string
	SchemaRegistry string // "http://schema-registry:8081"
}

type InfluxDBConfig struct {
	URL    string // "http://influxdb:8086"
	Token  string // SENSITIVE: InfluxDB API token
	Org    string // InfluxDB organization name
	Bucket string // InfluxDB bucket name
}

type MinIOConfig struct {
	Endpoint  string // "http://minio:9000"
	AccessKey string // SENSITIVE
	SecretKey string // SENSITIVE
	Bucket    string // "binance-datalake"
}

type JWTConfig struct {
	Secret     string // SENSITIVE: signing key cho JWT
	TTLMinutes int    // token TTL tính bằng phút (e.g. 60 = 1 hour)
	RefreshTTL int    // refresh token TTL tính bằng ngày (e.g. 7)
}

type VPSConfig struct {
	Username string // SENSITIVE: VPS trading account
	Password string // SENSITIVE
	BaseURL  string // VPS API base URL (override cho staging/prod)
}

type BINANCEConfig struct {
	APIKey       string // SENSITIVE: BINANCE API key (chỉ cần cho private endpoints)
	SecretKey    string // SENSITIVE
	Passphrase   string // SENSITIVE: BINANCE thêm 1 layer auth
	WebSocketURL string // BINANCE public WebSocket URL
}

type MonitoringConfig struct {
	LagAlertThreshold     int64 // Kafka consumer lag alert threshold
	SlowResponseThreshold int   // milliseconds (từ CheckProviderSlowResponse pattern)
	MetricsPort           int   // Prometheus metrics port (nếu có)
}

// Load đọc config từ environment variables + optional .env file
// LEARN: godotenv.Load() — load .env file vào os.Environ()
//
//	Không panic nếu .env không tồn tại (optional trong production)
func Load() (*Config, error) {
	// Load .env nếu có (development)
	// LEARN: Ignore error — production không cần .env (dùng actual env vars)
	if err := godotenv.Load(); err != nil {
		log.Println("[Config] No .env file found, using environment variables")
	}

	dbPort := getEnvInt("DB_PORT", 3306)
	redisDB := getEnvInt("REDIS_DB", 0)
	appPort := getEnvInt("APP_PORT", 8080)

	cfg := &Config{
		App: AppConfig{
			Name:        getEnv("APP_NAME", "binance-analytics-api"),
			Version:     getEnv("APP_VERSION", "1.0.0"),
			Environment: getEnv("APP_ENV", "development"),
			Port:        appPort,
			Debug:       getEnvBool("APP_DEBUG", true),
		},
		Database: DatabaseConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     dbPort,
			Name:     getEnv("DB_NAME", "binance_analytics"),
			User:     getEnv("DB_USER", "root"),
			Password: mustGetEnv("DB_PASSWORD"), // bắt buộc
		},
		Redis: RedisConfig{
			Address:  getEnv("REDIS_ADDRESS", "localhost:6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       redisDB,
		},
		Kafka: KafkaConfig{
			Brokers:        strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
			ConsumerGroup:  getEnv("KAFKA_CONSUMER_GROUP", "BINANCE-analytics-consumer"),
			SchemaRegistry: getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		},
		InfluxDB: InfluxDBConfig{
			URL:    getEnv("INFLUXDB_URL", "http://localhost:8086"),
			Token:  mustGetEnv("INFLUXDB_TOKEN"),
			Org:    getEnv("INFLUXDB_ORG", "binance-vps"),
			Bucket: getEnv("INFLUXDB_BUCKET", "binance-realtime"),
		},
		MinIO: MinIOConfig{
			Endpoint:  getEnv("MINIO_ENDPOINT", "http://localhost:9000"),
			AccessKey: mustGetEnv("MINIO_ACCESS_KEY"),
			SecretKey: mustGetEnv("MINIO_SECRET_KEY"),
			Bucket:    getEnv("MINIO_BUCKET", "binance-datalake"),
		},
		JWT: JWTConfig{
			Secret:     mustGetEnv("JWT_SECRET"),
			TTLMinutes: getEnvInt("JWT_TTL_MINUTES", 60),
			RefreshTTL: getEnvInt("JWT_REFRESH_TTL_DAYS", 7),
		},
		VPS: VPSConfig{
			Username: getEnv("VPS_USERNAME", ""),
			Password: getEnv("VPS_PASSWORD", ""),
			BaseURL:  getEnv("VPS_BASE_URL", "https://bgapidatafeed.vps.com.vn"),
		},
		BINANCE: BINANCEConfig{
			APIKey:       getEnv("BINANCE_API_KEY", ""),
			SecretKey:    getEnv("BINANCE_SECRET_KEY", ""),
			Passphrase:   getEnv("BINANCE_PASSPHRASE", ""),
			WebSocketURL: getEnv("BINANCE_WS_URL", "wss://ws.binance.com:8443/ws/v5/public"),
		},
		Monitoring: MonitoringConfig{
			LagAlertThreshold:     int64(getEnvInt("KAFKA_LAG_ALERT_THRESHOLD", 10000)),
			SlowResponseThreshold: getEnvInt("SLOW_RESPONSE_THRESHOLD_MS", 15000),
			MetricsPort:           getEnvInt("METRICS_PORT", 9090),
		},
	}

	// Compute DSN từ các fields riêng lẻ
	// LEARN: fmt.Sprintf xây dựng DSN — tốt hơn string concatenation (+)
	cfg.Database.DSN = fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.Database.User,
		cfg.Database.Password,
		cfg.Database.Host,
		cfg.Database.Port,
		cfg.Database.Name,
	)

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config validation: %w", err)
	}

	log.Printf("[Config] Loaded: env=%s, db=%s@%s/%s, kafka=%v",
		cfg.App.Environment,
		cfg.Database.User,
		cfg.Database.Host,
		cfg.Database.Name,
		cfg.Kafka.Brokers,
	)

	return cfg, nil
}

// validate kiểm tra config hợp lệ trước khi app chạy (fail fast)
// LEARN: Validate config lúc khởi động → phát hiện lỗi ngay, không đợi đến lúc dùng
func (c *Config) validate() error {
	if c.JWT.Secret == "" {
		return fmt.Errorf("JWT_SECRET is required")
	}
	if len(c.JWT.Secret) < 32 {
		return fmt.Errorf("JWT_SECRET must be at least 32 characters")
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS is required")
	}
	return nil
}

// IsProd kiểm tra có phải production environment không
// LEARN: Helper method trên Config — tránh magic string "production" ở nhiều nơi
func (c *Config) IsProd() bool {
	return c.App.Environment == "production"
}

// ─── Helper functions (private) ───────────────────────────────────────────────

func getEnv(key, defaultValue string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultValue
}

func mustGetEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("[Config] REQUIRED environment variable not set: %s", key)
	}
	return val
}

func getEnvInt(key string, defaultValue int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultValue
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		log.Printf("[Config] Invalid int for %s=%s, using default %d", key, val, defaultValue)
		return defaultValue
	}
	return n
}

func getEnvBool(key string, defaultValue bool) bool {
	val := os.Getenv(key)
	if val == "" {
		return defaultValue
	}
	b, err := strconv.ParseBool(val)
	if err != nil {
		return defaultValue
	}
	return b
}
