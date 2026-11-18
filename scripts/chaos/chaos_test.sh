#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/infra_dataops_finance_trading/docker-compose.yml"

restore() {
  docker compose -f "$COMPOSE_FILE" start kafka1 >/dev/null 2>&1 || true
}
trap restore EXIT

echo "=== ST09 Kafka Broker Chaos Test ==="

docker compose -f "$COMPOSE_FILE" ps kafka1 kafka2 kafka3
docker compose -f "$COMPOSE_FILE" stop kafka1

start_ns="$(date +%s%N)"

for _ in $(seq 1 30); do
  if docker compose -f "$COMPOSE_FILE" exec -T kafka2 \
      kafka-broker-api-versions --bootstrap-server kafka2:29092 >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

elapsed_ms=$(( ($(date +%s%N) - start_ns) / 1000000 ))
echo "[Metric] Failover elapsed: ${elapsed_ms}ms"

if (( elapsed_ms >= 30000)); then
  echo "[FAIL] Failover exceeded 30 seconds"
  exit 1
fi

docker compose -f "$COMPOSE_FILE" exec -T kafka2 \
  kafka-console-producer \
  --bootstrap-server kafka2:29092 \
  --topic binance.trade.ticks <<EOF
{"trade_id":"chaos-test-$(date +%s%N)","price":65000,"quantity":0.1,"ts":$(date +%s%3N)}
EOF

docker compose -f "$COMPOSE_FILE" exec -T kafka2 \
  kafka-topics --bootstrap-server kafka2:29092 \
  --describe --topic binance.trade.ticks

restore
trap - EXIT
echo "[PASS] Kafka retained availability; failover=${elapsed_ms}ms"