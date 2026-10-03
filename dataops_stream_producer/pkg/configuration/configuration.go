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

	// TCBS thay thế VPS cho market data streaming
	// Reference: openapi-v1.0.0.json — TCBS iFlash Open API v1.0.0
	TCBS struct {
		BaseURL  string   // https://openapi.tcbs.com.vn
		APIKey   string   // API Key từ TCInvest app (dùng để authenticate lấy JWT)
		JWTToken string   // Pre-obtained JWT token — dùng khi không muốn gọi authenticate()
		// Mã CK cần poll BSA + Intraday (danh sách quan trọng)
		Symbols         []string // ["VCB","VHM","TCB","VIC","HPG"]
		// Index baskets cần poll tickerCommons (market snapshot)
		Indexes         []string // ["HOSE","VN30","HNX"]
		// Loại nhà đầu tư cho BSA poll — xem tcbs_constants TCBSInvestorType
		BSAInvestorType string   // "sheep"|"wolf"|"shark"|"all" (default: "all")
		// Số trang tối đa khi lấy intraday tick history mỗi ticker
		IntradayMaxPages int     // default: 5
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

	// TCBS config
	cfg.TCBS.BaseURL   = utils.GetEnvAsString("TCBS_BASE_URL", "https://openapi.tcbs.com.vn")
	cfg.TCBS.APIKey    = utils.GetEnvAsString("TCBS_API_KEY", "")
	cfg.TCBS.JWTToken  = utils.GetEnvAsString("TCBS_JWT_TOKEN", "")
	// Symbols cho BSA + Intraday ticks — quan trọng nhất
	cfg.TCBS.Symbols   = splitCSV(utils.GetEnvAsString("TCBS_SYMBOLS", "VCB,VHM,TCB,VIC,HPG,MSN,VNM,MBB,CTG,BID"))
	// Index baskets cho market snapshot (tickerCommons)
	cfg.TCBS.Indexes   = splitCSV(utils.GetEnvAsString("TCBS_INDEXES", "HOSE,VN30,HNX"))
	cfg.TCBS.BSAInvestorType = utils.GetEnvAsString("TCBS_BSA_INVESTOR_TYPE", "all")
	intradayMaxPages, _ := utils.GetEnvAsInt("TCBS_INTRADAY_MAX_PAGES", 5)
	cfg.TCBS.IntradayMaxPages = intradayMaxPages

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
