#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/infra_dataops_finance_trading/docker-compose.yml"
BROKER="${1:-kafka1}"
MAX_FAILOVER_SECONDS="${MAX_FAILOVER_SECONDS:-30}"

case "$BROKER" in
  kafka1|kafka2|kafka3) ;;
  *)
    echo "Usage: $0 kafka1|kafka2|kafka3"
    exit 2
    ;;
esac

restore() {
  echo "[Recovery] Starting $BROKER"
  docker compose -f "$COMPOSE_FILE" start "$BROKER" >/dev/null 2>&1 || true
}
trap restore EXIT

echo "[Chaos] Stopping broker $BROKER"
docker compose -f "$COMPOSE_FILE" stop "$BROKER"

start_ns="$(date +%s%N)"
alive=false

for _ in $(seq 1 "$MAX_FAILOVER_SECONDS"); do
  if docker compose -f "$COMPOSE_FILE" exec -T kafka2 \
      kafka-broker-api-versions --bootstrap-server kafka2:29092 >/dev/null 2>&1; then
    alive=true
    break
  fi
  sleep 1
done

elapsed=$(( ($(date +%s%N) - start_ns) / 1000000 ))

if [[ "$alive" != "true" ]]; then
  echo "[FAIL] Remaining cluster did not become responsive"
  exit 1
fi

echo "[Validate] Surviving brokers became responsive after ${elapsed}ms"

if (( elapsed >= MAX_FAILOVER_SECONDS * 1000)); then
  echo "[FAIL] Failover exceeded ${MAX_FAILOVER_SECONDS}s"
  exit 1
fi

docker compose -f "$COMPOSE_FILE" exec -T kafka2 \
  kafka-topics --bootstrap-server kafka2:29092 \
  --describe --topic binance.trade.ticks

echo "[PASS] Kafka broker failure did not exceed SLA"