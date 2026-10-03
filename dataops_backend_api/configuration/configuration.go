// REUSE 70%: market-maker-api/configuration/configuration.go
package configuration

import "dataops-backend-api/utils"

type Config struct {
	MySQL struct {
		DNS string
	}
	Redis struct {
		Address  string
		Password string
		DB_0     int
	}
	InfluxDB struct {
		URL    string
		Token  string
		Org    string
		Bucket string
	}
	Web struct {
		URL  string
		Mode string
	}
	Authentication struct {
		Secret               string
		SecretRefreshToken   string
		Lifespan             int // access token TTL (hours)
		LifespanRefreshToken int // refresh token TTL (hours)
	}
	App struct {
		Mode string // "dev" | "prod"
	}
	// TCBS iFlash Open API — https://openapi.tcbs.com.vn
	// Auth: POST /gaia/v1/oauth2/openapi/token với apiKey + Smart OTP → JWT Bearer
	TCBS struct {
		BaseURL  string // default: https://openapi.tcbs.com.vn
		APIKey   string // TCBS_API_KEY env var
		JWTToken string // TCBS_JWT_TOKEN — pre-populated hoặc refresh bởi TCBSPoller
	}
}

func FromEnv() (*Config, error) {
	cfg := &Config{}

	cfg.MySQL.DNS = utils.GetEnv("MYSQL_DNS", "root:123456@tcp(localhost:3306)/binance_analytics?parseTime=true")

	cfg.Redis.Address = utils.GetEnv("REDIS_ADDRESS", "localhost:6379")
	cfg.Redis.Password = utils.GetEnv("REDIS_PASSWORD", "")
	cfg.Redis.DB_0 = utils.GetEnvInt("REDIS_DB_0", 0)

	cfg.InfluxDB.URL = utils.GetEnv("INFLUXDB_URL", "http://localhost:8086")
	cfg.InfluxDB.Token = utils.GetEnv("INFLUXDB_TOKEN", "")
	cfg.InfluxDB.Org = utils.GetEnv("INFLUXDB_ORG", "binance-vps-platform")
	cfg.InfluxDB.Bucket = utils.GetEnv("INFLUXDB_BUCKET", "market-data")

	cfg.Web.URL = utils.GetEnv("WEB_URL", "http://localhost:3000")
	cfg.Web.Mode = utils.GetEnv("MODE", "dev")

	cfg.Authentication.Secret = utils.GetEnv("JWT_SECRET", "")
	cfg.Authentication.SecretRefreshToken = utils.GetEnv("JWT_REFRESH_SECRET", "")
	cfg.Authentication.Lifespan = utils.GetEnvInt("JWT_LIFESPAN_HOURS", 24)
	cfg.Authentication.LifespanRefreshToken = utils.GetEnvInt("JWT_REFRESH_LIFESPAN_HOURS", 168) // 7 ngày

	cfg.App.Mode = cfg.Web.Mode

	cfg.TCBS.BaseURL  = utils.GetEnv("TCBS_BASE_URL", "https://openapi.tcbs.com.vn")
	cfg.TCBS.APIKey   = utils.GetEnv("TCBS_API_KEY", "")
	cfg.TCBS.JWTToken = utils.GetEnv("TCBS_JWT_TOKEN", "")

	return cfg, nil
}
