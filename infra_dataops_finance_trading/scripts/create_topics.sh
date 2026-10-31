#!/usr/bin/env bash
# =============================================================================
# create_topics.sh — Khởi tạo Kafka topics cho DataOps Financial Trading Platform
#
# Script này tạo topics với cấu hình production-grade:
#   - replication-factor=3: chịu 1 broker fail, đảm bảo MIN_ISR=2 (W+R>N=3)
#   - partitions=12 cho high-volume topics (binance.trade.ticks, binance.orderbook.snapshot)
#   - partitions=3 cho topic volume thấp, partitions=1 cho DLQ (khớp docker-compose kafka-init)
#   - retention, compression, segment config tối ưu cho từng use case
#
# TẠI SAO KHÔNG DÙNG KAFKA_AUTO_CREATE_TOPICS_ENABLE=true?
#   Auto-create dùng default config: replication=1, partitions=NUM_PARTITIONS(6).
#   Producer gõ nhầm topic name → Kafka tạo ngay topic mới với replication=1.
#   1 broker chết → topic đó mất data vĩnh viễn.
#   Script này: mọi topic đều rf=3, config explicit, không thể "accident create".
#
# TẠI SAO partitions=12 CHO TRADE TOPICS?
#   BinancePartitioner (transactional_producer.go): BTC* → P0, ETH* → P1,
#   các symbol còn lại băm FNV32a vào P2..P(N-1) (với N=12 là P2..P11).
#   Cùng key luôn vào cùng partition → ordering per-symbol được đảm bảo.
#   Lưu ý: altcoin KHÔNG có partition riêng — nhiều symbol có thể trùng partition.
#   Nhiều partition hơn → nhiều consumer đọc song song hơn (Spark tasks).
#
# TÊN TOPIC: phải khớp dataops_stream_producer/constants/topics.go.
#   Topic tạo ở đây mà code không dùng sẽ vô nghĩa; ngược lại topic code cần mà
#   không tạo ở đây thì phải dựa vào auto-create (rf=1) — chính điều script này tránh.
#
# USAGE (bootstrap server là tham số vị trí đầu tiên):
#   # Standalone (từ host machine, kafka broker đang chạy):
#   ./create_topics.sh localhost:9092
#
#   # Trong Docker Compose network (mặc định nếu bỏ trống):
#   ./create_topics.sh kafka1:29092,kafka2:29092,kafka3:29092
#
#   # Dry-run (hiển thị lệnh sẽ chạy, không thực thi):
#   DRY_RUN=1 ./create_topics.sh
#
# CHÚ Ý:
#   Script dùng --if-not-exists → idempotent, safe để chạy lại nhiều lần.
#   Nếu topic đã tồn tại với config khác → dùng kafka-configs --alter để update.
# =============================================================================
set -euo pipefail

# ─── Config ──────────────────────────────────────────────────────────────────
BOOTSTRAP="${1:-kafka1:29092,kafka2:29092,kafka3:29092}"
DRY_RUN="${DRY_RUN:-0}"

# Kafka CLI tool — dùng kafka-topics từ PATH (có sẵn trong cp-kafka image)
KAFKA_TOPICS="${KAFKA_TOPICS_CMD:-kafka-topics}"

# ─── Helper ──────────────────────────────────────────────────────────────────
log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] [create_topics] $*"; }

create_topic() {
    local topic="$1"
    local partitions="$2"
    local replication="$3"
    shift 3
    local configs=("$@")

    local cmd=(
        "$KAFKA_TOPICS"
        --bootstrap-server "$BOOTSTRAP"
        --create --if-not-exists
        --topic "$topic"
        --partitions "$partitions"
        --replication-factor "$replication"
    )
    for cfg in "${configs[@]}"; do
        cmd+=(--config "$cfg")
    done

    log "Creating: $topic (partitions=$partitions, rf=$replication)"
    if [[ "$DRY_RUN" == "1" ]]; then
        echo "  DRY_RUN: ${cmd[*]}"
    else
        "${cmd[@]}" && log "  OK: $topic" || log "  WARN: $topic already exists or error (check manually)"
    fi
}

# ─── Wait for broker quorum ───────────────────────────────────────────────────
log "Waiting for broker quorum at $BOOTSTRAP ..."
until $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP" --list > /dev/null 2>&1; do
    log "  Brokers not ready yet, retrying in 3s..."
    sleep 3
done
log "Broker quorum ready."

# =============================================================================
# HIGH-VOLUME TOPICS — partitions=12
# Binance real-time tick data: ~1000 events/second peak (Flash Crash scenario).
# 12 partitions = 12 Spark executors đọc song song = throughput ~83 events/s/partition.
# retention.ms=7200000 (2h): Spark watermark=30s << 2h → safe replay window.
# compression.type=snappy: low CPU overhead, tốt cho high-frequency streaming.
# segment.ms=1800000 (30m): roll segment mới mỗi 30p → compaction nhanh hơn.
# =============================================================================
log "=== Creating HIGH-VOLUME topics (partitions=12) ==="

# binance.trade.ticks — real-time trade events từ Binance WebSocket
# Key = symbol (BTC, ETH...) → BinancePartitioner đảm bảo per-symbol ordering.
# Consumer: Spark VWAP job (vwap_job.py), volume bars job (volume_bars_job.py).
create_topic "binance.trade.ticks" 6 3 \
    "retention.ms=7200000" \
    "segment.ms=1800000" \
    "compression.type=snappy" \
    "min.insync.replicas=2"

# binance.orderbook.snapshot — order book snapshot từ Binance depth stream
# (tên khớp constants.TopicBinanceOrderBookSnapshot, KHÔNG có "s" ở cuối).
# retention.ms=1800000 (30m): order book stale nhanh → không cần giữ lâu.
create_topic "binance.orderbook.snapshot" 6 3 \
    "retention.ms=1800000" \
    "segment.ms=600000" \
    "compression.type=snappy" \
    "min.insync.replicas=2"

# =============================================================================
# MEDIUM/LOW-VOLUME TOPICS — partitions=3
# Khớp docker-compose kafka-init: volume thấp hơn trade.ticks nhiều.
# =============================================================================
log "=== Creating MEDIUM/LOW-VOLUME topics (partitions=3) ==="

# binance.liquidation.events — forced liquidation (Binance Futures), giữ 24h.
create_topic "binance.liquidation.events" 3 3 \
    "retention.ms=86400000" \
    "compression.type=snappy" \
    "min.insync.replicas=2"

# binance.funding.rates — poll mỗi 8h, throughput rất thấp, giữ 48h.
create_topic "binance.funding.rates" 3 3 \
    "retention.ms=172800000" \
    "min.insync.replicas=2"

# binance.ohlcv.batch — OHLCV từ dataops_batch_api, giữ 7 ngày để replay.
create_topic "binance.ohlcv.batch" 3 3 \
    "retention.ms=604800000" \
    "min.insync.replicas=2"

# =============================================================================
# TCBS TOPICS — partitions=3 (khớp constants/topics.go)
# =============================================================================
log "=== Creating TCBS topics (partitions=3) ==="

create_topic "tcbs.market.snapshot" 3 3 \
    "retention.ms=3600000" \
    "segment.ms=900000" \
    "compression.type=snappy" \
    "min.insync.replicas=2"

create_topic "tcbs.bsa.events" 3 3 \
    "retention.ms=86400000" \
    "min.insync.replicas=2"

create_topic "tcbs.intraday.ticks" 3 3 \
    "retention.ms=86400000" \
    "min.insync.replicas=2"

create_topic "tcbs.derivative.contracts" 3 3 \
    "retention.ms=86400000" \
    "min.insync.replicas=2"

# =============================================================================
# ALERT TOPICS — partitions=3, giữ 24h
# =============================================================================
log "=== Creating ALERT topics (partitions=3) ==="

for t in binance.alerts.funding-anomaly binance.alerts.liquidation tcbs.alerts.bsa-anomaly; do
    create_topic "$t" 3 3 "retention.ms=86400000" "min.insync.replicas=2"
done

# =============================================================================
# DLQ TOPICS — partitions=1 (xử lý tuần tự để debug/audit), giữ 7 ngày.
# compression.type=none: payload lỗi cần đọc được trực tiếp khi điều tra.
# Mỗi topic chính có 1 DLQ tương ứng (xem constants/topics.go và dlq.go).
# =============================================================================
log "=== Creating DLQ topics (partitions=1) ==="

for t in platform.dlq \
         binance.trade.ticks.dlq \
         binance.orderbook.snapshot.dlq \
         binance.liquidation.events.dlq \
         tcbs.market.snapshot.dlq \
         tcbs.bsa.events.dlq \
         tcbs.intraday.ticks.dlq \
         tcbs.derivative.contracts.dlq; do
    create_topic "$t" 1 3 \
        "retention.ms=604800000" \
        "compression.type=none" \
        "min.insync.replicas=2"
done

# =============================================================================
# Summary
# =============================================================================
log "=== Topic creation complete. Listing all topics: ==="
$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP" --list | sort
