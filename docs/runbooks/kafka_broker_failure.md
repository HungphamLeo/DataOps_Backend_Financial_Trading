# Kafka Broker Failure Runbook

## Trigger

- Một broker chuyển sang trạng thái stopped/unhealthy.
- KRaft election hoặc leader reassignment vượt 30 giây.
- Producer circuit breaker chuyển sang Open.
- Consumer lag tăng liên tục.

## Immediate Actions

1. Kiểm tra trạng thái:

```bash
docker compose -f infra_dataops_finance_trading/docker-compose.yml ps
docker compose -f infra_dataops_finance_trading/docker-compose.yml logs --tail=200 kafka1 kafka2 kafka3


docker compose -f infra_dataops_finance_trading/docker-compose.yml exec kafka2 \
  kafka-broker-api-versions --bootstrap-server kafka2:29092

docker compose -f infra_dataops_finance_trading/docker-compose.yml exec kafka2 \
  kafka-topics --bootstrap-server kafka2:29092 \
  --describe --under-min-isr-partitions

docker compose -f infra_dataops_finance_trading/docker-compose.yml start kafka1
docker compose -f infra_dataops_finance_trading/docker-compose.yml ps kafka1

docker compose -f infra_dataops_finance_trading/docker-compose.yml restart kafka1
docker compose -f infra_dataops_finance_trading/docker-compose.yml logs --tail=300 kafka1


docker compose -f infra_dataops_finance_trading/docker-compose.yml exec kafka2 \
  kafka-console-consumer \
  --bootstrap-server kafka2:29092 \
  --topic binance.trade.ticks \
  --from-beginning \
  --max-messages 10

Recovery
Restart một lần khi Kafka và MinIO đã healthy:
docker compose -f infra_dataops_finance_trading/docker-compose.yml start kafka1 kafka2 kafka3
docker compose -f infra_dataops_finance_trading/docker-compose.yml restart minio
docker compose -f infra_dataops_finance_trading/docker-compose.yml restart dataops-processing


Verify Kafka Lag
docker compose -f infra_dataops_finance_trading/docker-compose.yml exec kafka2 \
  kafka-consumer-groups \
  --bootstrap-server kafka2:29092 \
  --describe --group spark-streaming-group

Chạy Scripts
bash -n scripts/chaos/*.sh
docker compose -f infra_dataops_finance_trading/docker-compose.yml config