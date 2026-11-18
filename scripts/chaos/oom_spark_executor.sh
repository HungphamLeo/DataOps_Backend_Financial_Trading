#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/infra_dataops_finance_trading/docker-compose.yml"
SERVICE="${1:-dataops-processing}"
CHAOS_MEMORY="${CHAOS_MEMORY:-128m}"
RECOVERY_MEMORY="${RECOVERY_MEMORY:-1536m}"

restore() {
  echo "[Recovery] Restoring memory limit"
  docker update --memory="$RECOVERY_MEMORY" "$SERVICE" >/dev/null 2>&1 || true
  docker compose -f "$COMPOSE_FILE" start "$SERVICE" >/dev/null 2>&1 || true
}
trap restore EXIT

echo "[Chaos] Applying $CHAOS_MEMORY limit to $SERVICE"
docker update --memory="$CHAOS_MEMORY" --memory-swap="$CHAOS_MEMORY" "$SERVICE"

docker restart "$SERVICE"

for _ in $(seq 1 60); do
  if docker inspect -f '{{.State.Running}}' "$SERVICE" 2>/dev/null | grep -q true; then
    echo "[PASS] Processing container restarted"
    exit 0
  fi
  sleep 1
done

echo "[FAIL] Processing container did not restart"
exit 1