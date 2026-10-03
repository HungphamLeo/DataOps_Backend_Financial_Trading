# OKX + VPS Platform

> OKX Crypto + VPS Securities VN → Kafka → Delta Lake → Real-time Analytics API

---

## Repos trong platform này

```
okx-vps-platform/
├── okx-stream-producer/   ← Go: WebSocket OKX + VPS poll → Kafka
├── okx-analytics-api/     ← Go: REST API (JWT, Redis, InfluxDB, MySQL)
├── okx-batch-ingestion/   ← Python: OKX/VPS REST → Delta Lake Bronze
└── docker-compose.yml     ← Full infra: Kafka, Redis, MySQL, InfluxDB, Grafana
```

## Chạy nhanh

```bash
# 1. Clone và copy env files
cp okx-stream-producer/.env.example okx-stream-producer/.env
cp okx-analytics-api/.env.example   okx-analytics-api/.env

# 2. Điền VPS_USERNAME, VPS_PASSWORD vào okx-stream-producer/.env

# 3. Khởi động infra
docker-compose up -d kafka1 kafka2 kafka3 redis mysql influxdb

# 4. Chạy stream producer (Go)
cd okx-stream-producer && go run .

# 5. Chạy analytics API (Go)
cd okx-analytics-api && go run .

# 6. Kiểm tra
curl http://localhost:8080/api/v1/health
```

## API Endpoints

```
POST /api/v1/login                                    ← Đăng nhập, lấy JWT
GET  /api/v1/health                                   ← Health check

# Crypto (OKX) — cần JWT header: "Authorization: Bearer <token>"
GET  /api/v1/crypto/vwap?symbol=BTC-USDT&interval=5m
GET  /api/v1/crypto/ohlcv?symbol=BTC-USDT&interval=1h
GET  /api/v1/crypto/orderbook?symbol=BTC-USDT
GET  /api/v1/crypto/funding?symbol=BTC-USDT-SWAP
GET  /api/v1/crypto/liquidations?symbol=BTC-USDT-SWAP

# VN Stocks (VPS) — cần JWT
GET  /api/v1/vn-stocks/market?symbol=VCB
GET  /api/v1/vn-stocks/ohlcv?symbol=VCB&interval=1d
GET  /api/v1/vn-stocks/orderbook?symbol=VCB
GET  /api/v1/vn-stocks/top-movers?exchange=HOSE&limit=10
GET  /api/v1/vn-stocks/market-summary
```

## Reuse từ repos senior

| File mới | Reuse từ | % |
|----------|----------|---|
| `okx-stream-producer/redis/redis.go` | `trading-bot-api/redis/redis.go` | 100% |
| `okx-stream-producer/kafka/writer.go` | `trading-bot-finance-service/kafka/writer.go` | 100% |
| `okx-stream-producer/utils/` | `market-maker-background/utils/` | 100% |
| `okx-stream-producer/pkg/sources/vps/` | `trading-bot-finance-service/pkg/sources/vps/` | 85% |
| `okx-analytics-api/middlewares/auth.go` | `market-maker-api/middlewares/auth.go` | 100% |
| `okx-analytics-api/repository/repository.go` | `market-maker-api/repository/repository.go` | 85% |
| `okx-analytics-api/services/authentication/` | `trading-bot-api/services/authentication/` | 90% |
| `okx-analytics-api/app/server.go` | `trading-bot-api/app/server.go` | 75% |
| `okx-analytics-api/api/rest/rest.go` | `market-maker-api/api/rest.go` | 80% |
