// REUSE 70%: market-maker-api/configuration/configuration.go
// Đọc config từ environment variables — 12-Factor App pattern
package configuration

import "dataops-stream-producer/utils"

type Config struct {
	Kafka struct {
		URL string
	}

	Redis struct {
		Address  string
		Password string
		DB_0     int
	}

	Binance struct {
		// WebSocket public URL — không cần API key cho market data
		WSURL string // wss://stream.binance.com:9443/ws
		// Trading pairs cần stream
		Symbols []string // ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
	}

	VPS struct {
		URLLogin string // https://soapi-partner.vps.com.vn/Api/Proxy
		URL      string // https://smartone.vps.com.vn/Api/Proxy
		Username string
		Password string
		// Mã CK cần theo dõi
		Symbols []string // ["VCB", "VHM", "TCB", "VIC", "HPG"]
	}

	SchemaRegistry struct {
		URL string // http://schema-registry:8081
	}

	App struct {
		Mode string // "dev" | "prod"
	}
}

func FromEnv() (*Config, error) {
	cfg := &Config{}

	cfg.Kafka.URL = utils.GetEnvAsString("KAFKA_URL", "localhost:9092")

	cfg.Redis.Address = utils.GetEnvAsString("REDIS_ADDRESS", "localhost:6379")
	cfg.Redis.Password = utils.GetEnvAsString("REDIS_PASSWORD", "")
	cfg.Redis.DB_0, _ = utils.GetEnvAsInt("REDIS_DB_0", 0)

	cfg.Binance.WSURL = utils.GetEnvAsString("BINANCE_WS_URL", "wss://stream.binance.com:9443/ws")
	// Symbols từ env: "BTCUSDT,ETHUSDT,SOLUSDT"
	cfg.Binance.Symbols = splitCSV(utils.GetEnvAsString("BINANCE_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT"))

	cfg.VPS.URLLogin = utils.GetEnvAsString("VPS_URL_LOGIN", "https://soapi-partner.vps.com.vn/Api/Proxy")
	cfg.VPS.URL = utils.GetEnvAsString("VPS_URL", "https://smartone.vps.com.vn/Api/Proxy")
	cfg.VPS.Username = utils.GetEnvAsString("VPS_USERNAME", "")
	cfg.VPS.Password = utils.GetEnvAsString("VPS_PASSWORD", "")
	cfg.VPS.Symbols = splitCSV(utils.GetEnvAsString("VPS_SYMBOLS", "VCB,VHM,TCB,VIC,HPG"))

	cfg.SchemaRegistry.URL = utils.GetEnvAsString("SCHEMA_REGISTRY_URL", "http://localhost:8081")

	cfg.App.Mode = utils.GetEnvAsString("MODE", "dev")

	return cfg, nil
}

// splitCSV tách "BTC-USDT,ETH-USDT" thành ["BTC-USDT", "ETH-USDT"]
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var result []string
	start := 0
	for i, c := range s {
		if c == ',' {
			if i > start {
				result = append(result, s[start:i])
			}
			start = i + 1
		}
	}
	if start < len(s) {
		result = append(result, s[start:])
	}
	return result
}
