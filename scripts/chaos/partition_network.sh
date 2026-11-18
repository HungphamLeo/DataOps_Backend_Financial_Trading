#!/usr/bin/env bash
set -Eeuo pipefail

NETWORK="${NETWORK:-dataops-internal-net}"
BROKER="${1:-kafka1}"
DURATION="${DURATION:-15}"

case "$BROKER" in
  kafka1|kafka2|kafka3) ;;
  *)
    echo "Usage: $0 kafka1|kafka2|kafka3"
    exit 2
    ;;
esac

restore() {
  docker network connect "$NETWORK" "dataops-$BROKER" >/dev/null 2>&1 || true
}
trap restore EXIT

echo "[Chaos] Disconnecting dataops-$BROKER from $NETWORK"
docker network disconnect "$NETWORK" "dataops-$BROKER"

sleep "$DURATION"

echo "[Recovery] Reconnecting dataops-$BROKER"
restore

for _ in $(seq 1 30); do
  if docker exec "dataops-$BROKER" \
      kafka-broker-api-versions --bootstrap-server "localhost:29092" >/dev/null 2>&1; then
    echo "[PASS] Broker recovered after network partition"
    exit 0
  fi
  sleep 1
done

echo "[FAIL] Broker did not recover"
exit 1