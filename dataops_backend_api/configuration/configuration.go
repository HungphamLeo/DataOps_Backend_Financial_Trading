// REUSE 70%: market-maker-api/configuration/configuration.go
package configuration

import "okx-analytics-api/utils"

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
}

func FromEnv() (*Config, error) {
	cfg := &Config{}

	cfg.MySQL.DNS = utils.GetEnvAsString("MYSQL_DNS", "root:123456@tcp(localhost:3306)/okx_analytics?parseTime=true")

	cfg.Redis.Address = utils.GetEnvAsString("REDIS_ADDRESS", "localhost:6379")
	cfg.Redis.Password = utils.GetEnvAsString("REDIS_PASSWORD", "")
	cfg.Redis.DB_0, _ = utils.GetEnvAsInt("REDIS_DB_0", 0)

	cfg.InfluxDB.URL = utils.GetEnvAsString("INFLUXDB_URL", "http://localhost:8086")
	cfg.InfluxDB.Token = utils.GetEnvAsString("INFLUXDB_TOKEN", "")
	cfg.InfluxDB.Org = utils.GetEnvAsString("INFLUXDB_ORG", "okx-vps-platform")
	cfg.InfluxDB.Bucket = utils.GetEnvAsString("INFLUXDB_BUCKET", "market-data")

	cfg.Web.URL = utils.GetEnvAsString("WEB_URL", "http://localhost:3000")
	cfg.Web.Mode = utils.GetEnvAsString("MODE", "dev")

	cfg.Authentication.Secret = utils.GetEnvAsString("JWT_SECRET", "")
	cfg.Authentication.SecretRefreshToken = utils.GetEnvAsString("JWT_REFRESH_SECRET", "")
	cfg.Authentication.Lifespan, _ = utils.GetEnvAsInt("JWT_LIFESPAN_HOURS", 24)
	cfg.Authentication.LifespanRefreshToken, _ = utils.GetEnvAsInt("JWT_REFRESH_LIFESPAN_HOURS", 168) // 7 ngày

	cfg.App.Mode = cfg.Web.Mode

	return cfg, nil
}
