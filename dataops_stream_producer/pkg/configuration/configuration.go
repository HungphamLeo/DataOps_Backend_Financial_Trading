// REUSE 70%: market-maker-api/configuration/configuration.go
// Đọc config từ environment variables — 12-Factor App pattern
package configuration

import (
	"fmt"
	"strings"

	"dataops-stream-producer/utils"
)

type Config struct {
	Kafka struct {
		Brokers []string
	}

	Redis struct {
		Address  string
		Password string
		DB_0     int
	}

	Binance struct {
		// WebSocket public URL — không cần API key cho market data
		WSURL string // wss://stream.binance.com:9443
		// Trading pairs cần stream
		Symbols []string // ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
	}

	// TCBS iFlash Open API — Vietnam Stock Market streaming & analytics
	// Reference: openapi-v1.0.0.json — TCBS iFlash Open API v1.0.0
	//
	// Hai loại data source tách biệt hoàn toàn:
	//
	//  WSS symbols  → /api/v1/derivatives/contracts (WebSocket, real-time push)
	//                 Chỉ dành cho phái sinh VN30F, e.g. VN30F2410,VN30F2411
	//                 Chạy trong TCBSWSSConsumer (goroutine liên tục như Binance)
	//
	//  REST symbols → /nyx/v1/intraday/{ticker}/bsa  (poll EOD sau 15:05)
	//                 /nyx/v1/intraday/{ticker}/his/paging (poll EOD sau 15:05)
	//                 Danh sách mã CK cơ sở quan trọng cần BSA + tick history
	//
	//  Indexes      → /tartarus/v1/tickerCommons (poll mỗi 1 phút giờ giao dịch)
	//                 Toàn bộ index basket, không cần liệt kê từng mã
	TCBS struct {
		BaseURL  string // https://openapi.tcbs.com.vn
		APIKey   string // API Key từ TCInvest app
		JWTToken string // Pre-obtained JWT Bearer token

		// WssSymbols: mã phái sinh cho WebSocket real-time stream
		// Endpoint: GET /api/v1/derivatives/contracts (WSS)
		// Chỉ dùng contract phái sinh: VN30F + tháng đáo hạn, e.g. VN30F2410
		// Env: TCBS_WSS_SYMBOLS
		WssSymbols []string

		// RestSymbols: mã CK cơ sở cho BSA + Intraday tick history (REST, EOD)
		// Endpoint: /nyx/v1/intraday/{ticker}/bsa, /nyx/v1/intraday/{ticker}/his/paging
		// Env: TCBS_REST_SYMBOLS
		RestSymbols []string

		// Indexes: index basket cho market snapshot (tickerCommons, REST poll mỗi 1 phút)
		// Endpoint: /tartarus/v1/tickerCommons?index=X
		// Env: TCBS_INDEXES
		Indexes []string

		// BSAInvestorType: loại nhà đầu tư cho BSA — "sheep"|"wolf"|"shark"|"all"
		// Env: TCBS_BSA_INVESTOR_TYPE
		BSAInvestorType string

		// IntradayMaxPages: số trang tối đa khi paginate intraday tick history
		// Env: TCBS_INTRADAY_MAX_PAGES (default: 5)
		IntradayMaxPages int
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

	cfg.Kafka.Brokers = splitCSV(utils.GetEnvAsString("KAFKA_URL", "localhost:9092"))

	cfg.Redis.Address = utils.GetEnvAsString("REDIS_ADDRESS", "localhost:6379")
	cfg.Redis.Password = utils.GetEnvAsString("REDIS_PASSWORD", "")
	redisDB0, err := utils.GetEnvAsInt("REDIS_DB_0", 0)
	if err != nil {
		return nil, fmt.Errorf("invalid REDIS_DB_0: %w", err)
	}
	cfg.Redis.DB_0 = redisDB0

	cfg.Binance.WSURL = utils.GetEnvAsString("BINANCE_WS_URL", "wss://stream.binance.com:9443")
	// Symbols từ env: "BTCUSDT,ETHUSDT,SOLUSDT"
	cfg.Binance.Symbols = splitCSV(utils.GetEnvAsString("BINANCE_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT"))

	// TCBS config
	cfg.TCBS.BaseURL  = utils.GetEnvAsString("TCBS_BASE_URL", "https://openapi.tcbs.com.vn")
	cfg.TCBS.APIKey   = utils.GetEnvAsString("TCBS_API_KEY", "")
	cfg.TCBS.JWTToken = utils.GetEnvAsString("TCBS_JWT_TOKEN", "")

	// WSS symbols — phái sinh VN30F (WebSocket real-time)
	// Default rỗng: WSS consumer chỉ start khi có symbols được cấu hình
	cfg.TCBS.WssSymbols = splitCSV(utils.GetEnvAsString("TCBS_WSS_SYMBOLS", ""))

	// REST symbols — cổ phiếu cơ sở cho BSA + intraday tick history (EOD poll)
	cfg.TCBS.RestSymbols = splitCSV(utils.GetEnvAsString("TCBS_REST_SYMBOLS",
		"VCB,VHM,TCB,VIC,HPG,MSN,VNM,MBB,CTG,BID,FPT,SSI,VPB,GMD,ACB,CTG,GAS,MWG,SAB,HDB"))

	// Index baskets cho market snapshot (tickerCommons — poll mỗi 1 phút)
	cfg.TCBS.Indexes = splitCSV(utils.GetEnvAsString("TCBS_INDEXES", "HOSE,VN30,HNX"))

	cfg.TCBS.BSAInvestorType = utils.GetEnvAsString("TCBS_BSA_INVESTOR_TYPE", "all")
	intradayMaxPages, err := utils.GetEnvAsInt("TCBS_INTRADAY_MAX_PAGES", 5)
	if err != nil {
		return nil, fmt.Errorf("invalid TCBS_INTRADAY_MAX_PAGES: %w", err)
	}
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
				if value := strings.TrimSpace(s[start:i]); value != "" {
					result = append(result, value)
				}
			}
			start = i + 1
		}
	}
	if start < len(s) {
		if value := strings.TrimSpace(s[start:]); value != "" {
			result = append(result, value)
		}
	}
	return result
}
