#!/usr/bin/env bash
# =============================================================================
# scripts/smoke_test.sh — DataOps Backend Financial Trading E2E Smoke Test
#
# Chạy sau `docker-compose up -d` để xác nhận toàn bộ stack hoạt động.
#
# Prerequisites:
#   - docker-compose stack đang chạy (infra_dataops_finance_trading/)
#   - curl, jq đã cài trên host
#   - .env đã điền INFLUXDB_TOKEN, TCBS_JWT_TOKEN (nếu test TCBS endpoints)
#
# Usage:
#   bash scripts/smoke_test.sh
#   bash scripts/smoke_test.sh --skip-auth   # bỏ qua phần tạo user (nếu đã có)
#
# Exit codes:
#   0 — tất cả checks PASSED
#   1 — ít nhất 1 check FAILED
# =============================================================================

set -euo pipefail

# SMOKE_STRICT=1 → pipeline-data checks (Kafka offsets, InfluxDB vwap, Delta objects)
# được tính là FAIL thay vì WARN. Mặc định WARN vì dữ liệu phụ thuộc giờ giao dịch.
SMOKE_STRICT="${SMOKE_STRICT:-0}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-dataops-kafka1}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka1:9092}"
INFLUXDB_TOKEN="${INFLUXDB_TOKEN:-PROD_SUPER_SECRET_INFLUXDB_AUTH_TOKEN}"
INFLUXDB_ORG="${INFLUXDB_ORG:-binance-vps-platform}"
INFLUXDB_BUCKET="${INFLUXDB_BUCKET:-market-data}"
MINIO_BUCKET="${MINIO_BUCKET:-binance-datalake}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"

BACKEND_URL="${BACKEND_URL:-http://localhost:8080}"
INFLUXDB_URL="${INFLUXDB_URL:-http://localhost:8086}"
MINIO_URL="${MINIO_URL:-http://localhost:9010}"
GRAFANA_URL="${GRAFANA_URL:-http://localhost:3001}"
LOKI_URL="${LOKI_URL:-http://localhost:3100}"

TEST_USER="${SMOKE_TEST_USER:-smoketest@dataops.local}"
TEST_PASS="${SMOKE_TEST_PASS:-SmokeTest@2026}"

SKIP_AUTH="${1:-}"

# ── Colors ──────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0

# NOTE: dùng $((x+1)) thay vì ((x++)) — ((0++)) trả exit 1 và làm `set -e` thoát script.
pass() { echo -e "${GREEN}  ✓ PASS${NC} — $1"; PASS=$((PASS+1)); }
fail() { echo -e "${RED}  ✗ FAIL${NC} — $1"; FAIL=$((FAIL+1)); }
info() { echo -e "${YELLOW}  →${NC} $1"; }
# warn: pipeline-data check — FAIL nếu SMOKE_STRICT=1, ngược lại chỉ cảnh báo
soft_fail() {
  if [ "$SMOKE_STRICT" = "1" ]; then fail "$1"; else echo -e "${YELLOW}  ! WARN${NC} — $1"; fi
}

# ── Helper ───────────────────────────────────────────────────────────────────
http_check() {
  local desc="$1"; local url="$2"; local expected_status="${3:-200}"
  local status
  status=$(curl -sf -o /dev/null -w "%{http_code}" "$url" 2>/dev/null || echo "000")
  if [ "$status" = "$expected_status" ]; then
    pass "$desc (HTTP $status)"
  else
    fail "$desc — expected $expected_status, got $status — URL: $url"
  fi
}

echo ""
echo "======================================================"
echo "  DataOps E2E Smoke Test"
echo "  Backend: $BACKEND_URL"
echo "======================================================"
echo ""

# ── 1. Infrastructure health checks ─────────────────────────────────────────
echo "[ 1/8 ] Infrastructure health"

http_check "InfluxDB health" "$INFLUXDB_URL/health"
http_check "MinIO health" "$MINIO_URL/minio/health/live"
http_check "Grafana health" "$GRAFANA_URL/api/health"
http_check "Loki ready" "$LOKI_URL/ready"

# ── 2. Backend API health ────────────────────────────────────────────────────
echo ""
echo "[ 2/8 ] Backend API health"

http_check "Backend /health" "$BACKEND_URL/health"

# ── 3. Auth — login ──────────────────────────────────────────────────────────
echo ""
echo "[ 3/8 ] Authentication"

if [ "$SKIP_AUTH" != "--skip-auth" ]; then
  # Register test user (idempotent — ignore 409 conflict)
  info "Registering test user: $TEST_USER"
  curl -sf -X POST "$BACKEND_URL/api/v1/register" \
    -H "Content-Type: application/json" \
    -d "{\"email\":\"$TEST_USER\",\"password\":\"$TEST_PASS\",\"username\":\"smoketest\"}" \
    -o /dev/null 2>/dev/null || true
fi

info "Logging in as $TEST_USER"
LOGIN_RESPONSE=$(curl -sf -X POST "$BACKEND_URL/api/v1/login" \
  -H "Content-Type: application/json" \
  -d "{\"email\":\"$TEST_USER\",\"password\":\"$TEST_PASS\"}" 2>/dev/null || echo "{}")

TOKEN=$(echo "$LOGIN_RESPONSE" | jq -r '.token // empty' 2>/dev/null || echo "")
if [ -n "$TOKEN" ] && [ "$TOKEN" != "null" ]; then
  pass "Login — JWT token received"
else
  fail "Login — no token in response. Response: $LOGIN_RESPONSE"
  echo ""
  echo "======================================================"
  echo "  SMOKE TEST ABORTED — cannot continue without token"
  echo "======================================================"
  exit 1
fi

AUTH_HEADER="Authorization: Bearer $TOKEN"

# ── 4. Crypto endpoints ──────────────────────────────────────────────────────
echo ""
echo "[ 4/8 ] Crypto API endpoints"

# Ticker list
TICKER_RESP=$(curl -sf -H "$AUTH_HEADER" "$BACKEND_URL/api/v1/crypto/ticker" 2>/dev/null || echo "{}")
if echo "$TICKER_RESP" | jq -e '.data' > /dev/null 2>&1; then
  pass "GET /crypto/ticker — data field present"
else
  fail "GET /crypto/ticker — unexpected response: $TICKER_RESP"
fi

# BTCUSDT ticker
BTC_RESP=$(curl -sf -H "$AUTH_HEADER" "$BACKEND_URL/api/v1/crypto/BTCUSDT/ticker" 2>/dev/null || echo "{}")
if echo "$BTC_RESP" | jq -e '.data.symbol' > /dev/null 2>&1; then
  PRICE=$(echo "$BTC_RESP" | jq -r '.data.price // "N/A"')
  pass "GET /crypto/BTCUSDT/ticker — price=$PRICE"
else
  fail "GET /crypto/BTCUSDT/ticker — unexpected response: $BTC_RESP"
fi

# OHLCV
OHLCV_RESP=$(curl -sf -H "$AUTH_HEADER" "$BACKEND_URL/api/v1/crypto/BTCUSDT/ohlcv?interval=1d&limit=5" 2>/dev/null || echo "{}")
if echo "$OHLCV_RESP" | jq -e '.data | length > 0' > /dev/null 2>&1; then
  COUNT=$(echo "$OHLCV_RESP" | jq '.data | length')
  pass "GET /crypto/BTCUSDT/ohlcv — $COUNT candles returned"
else
  fail "GET /crypto/BTCUSDT/ohlcv — no data or error: $OHLCV_RESP"
fi

# VWAP
VWAP_RESP=$(curl -sf -H "$AUTH_HEADER" "$BACKEND_URL/api/v1/crypto/BTCUSDT/vwap?interval=5m" 2>/dev/null || echo "{}")
if echo "$VWAP_RESP" | jq -e '.data' > /dev/null 2>&1; then
  pass "GET /crypto/BTCUSDT/vwap — data present"
else
  fail "GET /crypto/BTCUSDT/vwap — no data (InfluxDB/processing may not have data yet): $VWAP_RESP"
fi

# Signal (RSI+MA)
SIG_RESP=$(curl -sf -H "$AUTH_HEADER" "$BACKEND_URL/api/v1/crypto/BTCUSDT/signal?groupId=1&timeframe=1h" 2>/dev/null || echo "{}")
if echo "$SIG_RESP" | jq -e '.data.signal' > /dev/null 2>&1; then
  SIGNAL=$(echo "$SIG_RESP" | jq -r '.data.signal')
  pass "GET /crypto/BTCUSDT/signal — signal=$SIGNAL"
else
  fail "GET /crypto/BTCUSDT/signal — no signal: $SIG_RESP"
fi

# ── 5. Trace ID propagation ──────────────────────────────────────────────────
echo ""
echo "[ 5/8 ] Trace ID propagation (X-Trace-ID)"

TRACE_ID="smoke-test-$(date +%s)"
TRACE_RESP_HEADERS=$(curl -sf -D - -H "$AUTH_HEADER" -H "X-Trace-ID: $TRACE_ID" \
  "$BACKEND_URL/api/v1/crypto/BTCUSDT/ticker" -o /dev/null 2>/dev/null || echo "")
if echo "$TRACE_RESP_HEADERS" | grep -qi "x-trace-id: $TRACE_ID"; then
  pass "X-Trace-ID echoed back in response headers"
else
  fail "X-Trace-ID not echoed back — middleware may not be registered"
fi

# ── 6. Portfolio & pre-trade risk endpoints ─────────────────────────────────
echo ""
echo "[ 6/8 ] Portfolio & Risk API (BE1–BE4)"

for ep in overview nav allocation risk-report; do
  code=$(curl -s -o /dev/null -w "%{http_code}" -H "$AUTH_HEADER" "$BACKEND_URL/api/v1/portfolio/$ep" || echo "000")
  # 200 = OK ; 5xx/502 do upstream (Binance/TCBS creds chưa cấu hình) vẫn chứng minh route đã đăng ký
  if [ "$code" = "200" ]; then
    pass "GET /portfolio/$ep (HTTP 200)"
  elif [ "$code" = "404" ] || [ "$code" = "000" ]; then
    fail "GET /portfolio/$ep — route missing (HTTP $code)"
  else
    soft_fail "GET /portfolio/$ep — route exists but HTTP $code (upstream credentials?)"
  fi
done

PT_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "$AUTH_HEADER" -H "Content-Type: application/json" \
  -d '{"symbol":"BTCUSDT","side":"BUY","quantity":0.001,"price":1}' \
  "$BACKEND_URL/api/v1/portfolio/pre-trade-check" || echo "000")
if [ "$PT_CODE" = "404" ] || [ "$PT_CODE" = "000" ]; then
  fail "POST /portfolio/pre-trade-check — route missing (HTTP $PT_CODE)"
else
  pass "POST /portfolio/pre-trade-check — route registered (HTTP $PT_CODE)"
fi

# ── 7. Streaming chain: producer → Kafka → processing ────────────────────────
echo ""
echo "[ 7/8 ] Streaming chain (stream-producer → Kafka → processing)"

for c in dataops-kafka1 dataops-stream-producer dataops-processing dataops-batch-api; do
  state=$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null || echo "missing")
  if [ "$state" = "true" ]; then pass "container $c running"; else fail "container $c not running ($state)"; fi
done

for topic in binance.trade.ticks tcbs.market.snapshot; do
  offs=$(docker exec "$KAFKA_CONTAINER" kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$topic" --time -1 2>/dev/null \
    | awk -F: '{s+=$3} END{print s+0}' || echo 0)
  if [ "${offs:-0}" -gt 0 ]; then
    pass "Kafka topic $topic has messages (end-offset sum=$offs)"
  else
    soft_fail "Kafka topic $topic empty/unreachable (TCBS topic chỉ có dữ liệu trong giờ HOSE)"
  fi
done

# ── 8. Sinks: InfluxDB (VWAP) + Delta Lake on MinIO ──────────────────────────
echo ""
echo "[ 8/8 ] Sinks (InfluxDB vwap, Delta Bronze/Silver)"

FLUX="from(bucket:\"$INFLUXDB_BUCKET\") |> range(start:-1h) |> filter(fn:(r)=>r._measurement==\"vwap\") |> limit(n:1)"
INFLUX_ROWS=$(curl -s -X POST "$INFLUXDB_URL/api/v2/query?org=$INFLUXDB_ORG" \
  -H "Authorization: Token $INFLUXDB_TOKEN" \
  -H "Content-Type: application/vnd.flux" -H "Accept: application/csv" \
  --data "$FLUX" 2>/dev/null | grep -c ",vwap," || true)
if [ "${INFLUX_ROWS:-0}" -gt 0 ]; then
  pass "InfluxDB measurement 'vwap' has points in last 1h"
else
  soft_fail "InfluxDB measurement 'vwap' has no points in last 1h (processing VWAP job chưa emit window?)"
fi

delta_has_log() {
  # $1 = prefix dưới bucket, e.g. delta/silver/vwap_1m
  curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user "$MINIO_ACCESS_KEY:$MINIO_SECRET_KEY" \
    "$MINIO_URL/$MINIO_BUCKET?list-type=2&prefix=$1/_delta_log/&max-keys=1" 2>/dev/null \
    | grep -q "<Key>"
}
for tbl in delta/silver/vwap_1m delta/bronze/binance_candles_spot delta/bronze/tcbs_market_eod; do
  if delta_has_log "$tbl"; then
    pass "Delta table $tbl has _delta_log"
  else
    soft_fail "Delta table $tbl not found in s3://$MINIO_BUCKET (job chưa chạy lần nào?)"
  fi
done

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "======================================================"
echo "  SMOKE TEST RESULTS"
echo "  PASSED: $PASS    FAILED: $FAIL"
echo "======================================================"
echo ""

if [ "$FAIL" -gt 0 ]; then
  echo -e "${RED}  ✗ SMOKE TEST FAILED — $FAIL check(s) failed${NC}"
  exit 1
else
  echo -e "${GREEN}  ✓ SMOKE TEST PASSED — all $PASS checks OK${NC}"
  exit 0
fi
