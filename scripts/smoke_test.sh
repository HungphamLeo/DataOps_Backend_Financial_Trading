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

pass() { echo -e "${GREEN}  ✓ PASS${NC} — $1"; ((PASS++)); }
fail() { echo -e "${RED}  ✗ FAIL${NC} — $1"; ((FAIL++)); }
info() { echo -e "${YELLOW}  →${NC} $1"; }

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
echo "[ 1/5 ] Infrastructure health"

http_check "InfluxDB health" "$INFLUXDB_URL/health"
http_check "MinIO health" "$MINIO_URL/minio/health/live"
http_check "Grafana health" "$GRAFANA_URL/api/health"
http_check "Loki ready" "$LOKI_URL/ready"

# ── 2. Backend API health ────────────────────────────────────────────────────
echo ""
echo "[ 2/5 ] Backend API health"

http_check "Backend /health" "$BACKEND_URL/health"

# ── 3. Auth — login ──────────────────────────────────────────────────────────
echo ""
echo "[ 3/5 ] Authentication"

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
echo "[ 4/5 ] Crypto API endpoints"

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
echo "[ 5/5 ] Trace ID propagation (X-Trace-ID)"

TRACE_ID="smoke-test-$(date +%s)"
TRACE_RESP_HEADERS=$(curl -sf -D - -H "$AUTH_HEADER" -H "X-Trace-ID: $TRACE_ID" \
  "$BACKEND_URL/api/v1/crypto/BTCUSDT/ticker" -o /dev/null 2>/dev/null || echo "")
if echo "$TRACE_RESP_HEADERS" | grep -qi "x-trace-id: $TRACE_ID"; then
  pass "X-Trace-ID echoed back in response headers"
else
  fail "X-Trace-ID not echoed back — middleware may not be registered"
fi

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
