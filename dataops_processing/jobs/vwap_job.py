"""
vwap_job.py — Spark Structured Streaming VWAP Job.

Pipeline:
    Kafka (binance.trade.ticks)
        → deserialize JSON payload (Confluent wire-format fallback JSON)
        → withWatermark("event_time", "30 seconds")
        → groupBy(window 5min / 1min slide, symbol)
        → VWAP = sum(price * size) / sum(size)
        → dual sink:
            1. InfluxDB bucket "market-data", measurement "vwap"
               (foreachBatch → influxdb-client Python)
            2. Delta Silver s3://binance-datalake/delta/silver/vwap_1m
               (append mode, partition by symbol)

VWAP formula:
    VWAP = Σ(price_i × size_i) / Σ(size_i)  over window W

InfluxDB schema (khớp với backend GetVWAP Flux query):
    measurement : vwap
    tags        : symbol (e.g. "BTCUSDT"), interval (e.g. "5m")
    fields      : vwap (float64), volume (float64), num_trades (int64)
    _time       : window end timestamp (UTC)

Avro schema (binance_trade_event.avsc):
    instId      → symbol
    price       → double
    size        → double (base currency quantity)
    tsMs        → long (Unix milliseconds, event time)
    side        → enum "buy"|"sell"
    instType    → enum "SPOT"|"FUTURES"|...

Confluent Wire Format (producer.go serializeAvro):
    [0x00][4-byte schema_id BE][avro/json bytes]
    Nếu Schema Registry không khả dụng → producer gửi plain JSON (không có magic byte).
    Job này strip 5-byte header nếu có, fallback JSON nếu không.

Env vars:
    KAFKA_URL               default: localhost:9092
    INFLUXDB_URL            default: http://localhost:8086
    INFLUXDB_TOKEN          default: (required)
    INFLUXDB_ORG            default: binance-vps-platform
    INFLUXDB_BUCKET         default: market-data
    MINIO_ENDPOINT          default: http://minio:9010
    MINIO_ACCESS_KEY        default: minioadmin
    MINIO_SECRET_KEY        default: minioadmin
    DELTA_SILVER_BASE       default: s3://binance-datalake/delta/silver
    SPARK_CHECKPOINT_DIR    default: /tmp/spark-checkpoints/vwap_job

Chạy:
    python -m dataops_processing.jobs.vwap_job
    hoặc spark-submit dataops_processing/jobs/vwap_job.py
"""
from __future__ import annotations

import logging
import os
import struct
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Kafka topic — mirror từ dataops_stream_producer/constants/topics.go
# ---------------------------------------------------------------------------
TOPIC_BINANCE_TRADE_TICKS = "binance.trade.ticks"

# ---------------------------------------------------------------------------
# InfluxDB constants — phải khớp với backend GetVWAP Flux query:
#   filter(fn: (r) => r._measurement == "vwap"
#                  and r.symbol == "{symbol}"
#                  and r.interval == "{interval}")
#   fields: "vwap", "volume"
# ---------------------------------------------------------------------------
INFLUX_MEASUREMENT = "vwap"
INFLUX_TAG_INTERVAL_5M = "5m"
INFLUX_TAG_INTERVAL_1M = "1m"

# ---------------------------------------------------------------------------
# Delta Silver path
# ---------------------------------------------------------------------------
_DELTA_SILVER_BASE = os.environ.get("DELTA_SILVER_BASE", "s3://binance-datalake/delta/silver")
DELTA_SILVER_VWAP = f"{_DELTA_SILVER_BASE}/vwap_1m"


# ---------------------------------------------------------------------------
# Avro / JSON deserialization
# ---------------------------------------------------------------------------

def _strip_confluent_header(raw: bytes) -> bytes:
    """
    Strip Confluent wire-format 5-byte header nếu có (magic byte 0x00).

    Header format: [0x00][schema_id: 4 bytes big-endian][payload]
    Nếu không có magic byte → dữ liệu là plain JSON → trả nguyên.
    """
    if len(raw) > 5 and raw[0] == 0x00:
        return raw[5:]
    return raw


def _parse_trade_payload(raw_bytes: bytes) -> dict | None:
    """
    Parse 1 raw Kafka value bytes → canonical trade dict.

    ── CANONICAL BOUNDARY ──────────────────────────────────────────────────────
    Producer (Go types.go TradeEvent) gửi JSON với field names gốc:
        instId, price, size, side, tsMs, producedAtMs, instType

    Hàm này là ranh giới canonical: normalize về field names theo
    CanonicalTrade trong dataops_protobuf/market-data/market_data.proto:
        instId  → symbol   (canonical field name)
        tsMs    → ts_ms    (canonical field name, unit ms)
        side    → side     (giữ nguyên "buy"/"sell" lowercase từ Avro enum)

    Output dict keys: symbol, price, size, side, ts_ms
    ────────────────────────────────────────────────────────────────────────────

    Returns None nếu parse fail (record bị skip, không crash job).
    """
    import json

    payload = _strip_confluent_header(raw_bytes)
    try:
        data = json.loads(payload)
    except (ValueError, UnicodeDecodeError):
        return None

    # Validate required fields
    inst_id = data.get("instId")
    price   = data.get("price")
    size    = data.get("size")
    ts_ms   = data.get("tsMs")

    if not inst_id or price is None or size is None or ts_ms is None:
        return None
    if size <= 0 or price <= 0:
        return None

    # Normalize: instId → symbol, tsMs → ts_ms (canonical field names)
    return {
        "symbol": inst_id,          # canonical: .proto CanonicalTrade.symbol
        "price":  float(price),
        "size":   float(size),
        "side":   data.get("side", "buy"),  # "buy" | "sell" (Avro TradeSide enum)
        "ts_ms":  int(ts_ms),       # canonical: .proto CanonicalTrade.ts_ms
    }


# ---------------------------------------------------------------------------
# InfluxDB sink
# ---------------------------------------------------------------------------

def _write_to_influxdb(df_pandas, batch_id: int) -> None:
    """
    foreachBatch sink: ghi VWAP rows vào InfluxDB.

    df_pandas columns: symbol, window_start, window_end, interval,
                       vwap, total_volume, num_trades

    InfluxDB point schema (khớp với GetVWAP Flux query):
        measurement = "vwap"
        tag:   symbol   e.g. "BTCUSDT"
        tag:   interval e.g. "5m"
        field: vwap     float
        field: volume   float
        field: num_trades int
        time:  window_end (UTC nanoseconds)
    """
    if df_pandas.empty:
        return

    influx_url    = os.environ.get("INFLUXDB_URL", "http://localhost:8086")
    influx_token  = os.environ.get("INFLUXDB_TOKEN", "")
    influx_org    = os.environ.get("INFLUXDB_ORG", "binance-vps-platform")
    influx_bucket = os.environ.get("INFLUXDB_BUCKET", "market-data")

    try:
        from influxdb_client import InfluxDBClient, WriteOptions
        from influxdb_client.client.write_api import SYNCHRONOUS
    except ImportError as exc:  # pragma: no cover
        logger.error("influxdb-client not installed: %s", exc)
        return

    client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    try:
        from influxdb_client.domain.write_precision import WritePrecision
        records = []
        for _, row in df_pandas.iterrows():
            # window_end là pandas Timestamp — convert sang nanoseconds UTC
            ts_ns = int(row["window_end"].timestamp() * 1_000_000_000)
            point = (
                f"{INFLUX_MEASUREMENT},"
                f"symbol={row['symbol']},"
                f"interval={row['interval']} "
                f"vwap={row['vwap']},"
                f"volume={row['total_volume']},"
                f"num_trades={int(row['num_trades'])}i "
                f"{ts_ns}"
            )
            records.append(point)

        write_api.write(bucket=influx_bucket, org=influx_org, record=records,
                        write_precision=WritePrecision.NANOSECONDS)
        logger.info("batch_id=%d: wrote %d VWAP points to InfluxDB", batch_id, len(records))
    except Exception as exc:
        logger.error("batch_id=%d: InfluxDB write failed: %s", batch_id, exc)
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Spark Structured Streaming job
# ---------------------------------------------------------------------------

def run(spark=None) -> None:
    """
    Khởi động Spark Structured Streaming VWAP job.

    Args:
        spark: SparkSession tuỳ chọn — inject từ ngoài (dùng trong tests).
               Nếu None → tự tạo qua build_spark_session().
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, DoubleType, LongType, TimestampType,
    )

    # correlation_id — sinh 1 UUID mới cho mỗi job run để trace toàn bộ log entries
    correlation_id = str(uuid.uuid4())
    logger.info("vwap_job.start", extra={"correlation_id": correlation_id})

    if spark is None:
        from dataops_processing.config.spark_config import build_spark_session
        spark = build_spark_session("dataops-vwap-job")

    checkpoint_dir = os.environ.get(
        "SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints/vwap_job"
    )
    delta_silver_path = DELTA_SILVER_VWAP

    logger.info("VWAP job starting — topic=%s checkpoint=%s delta=%s",
                TOPIC_BINANCE_TRADE_TICKS, checkpoint_dir, delta_silver_path,
                extra={"correlation_id": correlation_id})

    # ── 1. Đọc từ Kafka ─────────────────────────────────────────────────────
    from dataops_processing.config.spark_config import get_kafka_options

    raw_stream = (
        spark.readStream
        .format("kafka")
        .options(**get_kafka_options(TOPIC_BINANCE_TRADE_TICKS))
        .load()
    )

    # ── 2. Deserialize JSON payload ─────────────────────────────────────────
    # UDF: bytes → struct(symbol, price, size, side, ts_ms)
    trade_schema = StructType([
        StructField("symbol", StringType(),  nullable=False),
        StructField("price",  DoubleType(),  nullable=False),
        StructField("size",   DoubleType(),  nullable=False),
        StructField("side",   StringType(),  nullable=True),
        StructField("ts_ms",  LongType(),    nullable=False),
    ])

    @F.udf(returnType=trade_schema)
    def parse_trade_udf(raw: bytes):
        result = _parse_trade_payload(raw)
        if result is None:
            return None
        return (
            result["symbol"],
            result["price"],
            result["size"],
            result["side"],
            result["ts_ms"],
        )

    parsed = (
        raw_stream
        .select(parse_trade_udf(F.col("value")).alias("trade"))
        .filter(F.col("trade").isNotNull())
        .select(
            F.col("trade.symbol").alias("symbol"),
            F.col("trade.price").alias("price"),
            F.col("trade.size").alias("size"),
            F.col("trade.side").alias("side"),
            # ts_ms (Unix ms) → TimestampType cho watermark
            (F.col("trade.ts_ms") / 1000).cast(TimestampType()).alias("event_time"),
            # price × size = notional value (VWAP numerator component)
            (F.col("trade.price") * F.col("trade.size")).alias("notional"),
        )
    )

    # ── 3. Watermark + Window aggregation ───────────────────────────────────
    # Watermark 30s: drop late events sau 30s so với max event_time đã thấy.
    # Window 5 phút, slide 1 phút → mỗi phút ra 1 bar VWAP của 5 phút gần nhất.
    with_watermark = parsed.withWatermark("event_time", "30 seconds")

    vwap_agg = (
        with_watermark
        .groupBy(
            F.window("event_time", "5 minutes", "1 minute").alias("window"),
            F.col("symbol"),
        )
        .agg(
            # VWAP = Σ(price × size) / Σ(size)
            (F.sum("notional") / F.sum("size")).alias("vwap"),
            F.sum("size").alias("total_volume"),
            F.count("*").alias("num_trades"),
        )
        .select(
            F.col("symbol"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.lit(INFLUX_TAG_INTERVAL_5M).alias("interval"),
            F.col("vwap"),
            F.col("total_volume"),
            F.col("num_trades"),
        )
    )

    # ── 4. Dual sink ─────────────────────────────────────────────────────────

    # Sink A: InfluxDB (foreachBatch — convert to pandas rồi dùng influxdb-client)
    influx_query = (
        vwap_agg
        .writeStream
        .outputMode("append")           # append: chỉ emit window đã đóng (sau watermark)
        .trigger(processingTime="30 seconds")
        .foreachBatch(_write_to_influxdb)
        .option("checkpointLocation", f"{checkpoint_dir}/influxdb")
        .queryName("vwap_to_influxdb")
        .start()
    )

    # Sink B: Delta Silver (append)
    # Partition by symbol để query nhanh theo instrument
    delta_query = (
        vwap_agg
        .writeStream
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_dir}/delta")
        .partitionBy("symbol")
        .start(delta_silver_path)
    )

    logger.info("VWAP job streams started — awaiting termination",
                extra={"correlation_id": correlation_id})

    # Block cho đến khi cả 2 query terminate (Ctrl+C hoặc exception)
    spark.streams.awaitAnyTermination()

    logger.info("VWAP job terminated", extra={"correlation_id": correlation_id})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    logger.info("Starting VWAP job via __main__")

    try:
        run()
    except KeyboardInterrupt:
        logger.info("VWAP job stopped by user")
        sys.exit(0)
