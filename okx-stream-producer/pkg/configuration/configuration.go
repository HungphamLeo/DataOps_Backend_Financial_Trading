// REUSE 70%: market-maker-api/configuration/configuration.go
// Đọc config từ environment variables — 12-Factor App pattern
package configuration

import "okx-stream-producer/utils"

type Config struct {
	Kafka struct {
		URL string
	}

	Redis struct {
		Address  string
		Password string
		DB_0     int
	}

	OKX struct {
		// WebSocket public URL — không cần API key cho market data
		WSURL string // wss://ws.okx.com:8443/ws/v5/public
		// Trading pairs cần stream
		Symbols []string // ["BTC-USDT", "ETH-USDT", "SOL-USDT"]
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

	cfg.OKX.WSURL = utils.GetEnvAsString("OKX_WS_URL", "wss://ws.okx.com:8443/ws/v5/public")
	// Symbols từ env: "BTC-USDT,ETH-USDT,SOL-USDT"
	cfg.OKX.Symbols = splitCSV(utils.GetEnvAsString("OKX_SYMBOLS", "BTC-USDT,ETH-USDT,SOL-USDT"))

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
