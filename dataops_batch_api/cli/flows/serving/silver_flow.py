"""
silver_flow.py — Silver Transform Flow: PySpark Structured Streaming.

Đây là Stage 3 (T3) trong pipeline:
    Kafka → PySpark → Delta Lake Silver + Kafka Alert Topics

Hai streaming jobs:
    1. VWAP Sliding Window (5m/1m slide, Watermark 30s) → Delta Silver
    2. Funding Rate Z-Score Anomaly (|z| ≥ 2.5) → Kafka alert topic (binance.alerts.funding-anomaly)

Tất cả tham số (topic names, window params, thresholds, checkpoint paths)
được đọc từ exchange_config.yaml qua metadata.py — không hardcode.
"""
from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as _abs,
    avg,
    col,
    from_json,
    stddev,
    struct,
    sum as _sum,
    to_json,
    to_timestamp,
    window,
)
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from cli.flows.serving.metadata import get_kafka_brokers, get_spark_streaming_cfg, setup_logging
from platforms.processing.spark_stack.spark_session import create_spark_session

logger = logging.getLogger(__name__)


# ── Avro/JSON schemas cho Kafka topics ────────────────────────────────────────

TRADE_SCHEMA = StructType([
    StructField("inst_id",  StringType(), False),
    StructField("trade_id", StringType(), False),
    StructField("price",    DoubleType(), False),
    StructField("qty",      DoubleType(), False),
    StructField("side",     StringType(), False),
    StructField("ts",       LongType(),   False),
])

FUNDING_SCHEMA = StructType([
    StructField("inst_id",          StringType(), False),
    StructField("funding_rate",     DoubleType(), False),
    StructField("next_funding_time", LongType(), False),
    StructField("ts",               LongType(),   False),
])


# ─── Job 1: VWAP Sliding Window ───────────────────────────────────────────────

def run_vwap_stream(spark: SparkSession, kafka_brokers: str, cfg: dict):
    """
    Job 1 — VWAP Sliding Window.

    Config (từ exchange_config.yaml section spark_streaming.vwap):
        window_duration: "5 minutes"
        slide_duration:  "1 minute"
        watermark:       "30 seconds"
        checkpoint:      "/tmp/spark_checkpoints/vwap"

    Ghi kết quả vào Delta Lake Silver: delta path đọc từ delta.silver.vwap trong yaml
    hoặc env var DELTA_SILVER_VWAP (env override yaml).

    Args:
        spark: SparkSession đã khởi tạo
        kafka_brokers: bootstrap servers string
        cfg: dict toàn bộ spark_streaming section từ exchange_config.yaml

    Returns:
        StreamingQuery handle

    Raises:
        Exception: nếu không thể start streaming query — log ERROR và re-raise.
    """
    from cli.flows.serving.metadata import get_delta_cfg
    delta_cfg  = get_delta_cfg()

    topic      = cfg.get("kafka_topic_trade_ticks", "okx.trade.ticks")
    window_dur = cfg.get("vwap", {}).get("window_duration", "5 minutes")
    slide_dur  = cfg.get("vwap", {}).get("slide_duration", "1 minute")
    watermark  = cfg.get("vwap", {}).get("watermark", "30 seconds")
    checkpoint = cfg.get("vwap", {}).get("checkpoint", "/tmp/spark_checkpoints/vwap")
    # Env var override > yaml > hardcoded fallback
    delta_path = os.getenv(
            "DELTA_SILVER_VWAP",
            delta_cfg.get("silver", {}).get("vwap", "s3a://binance-datalake/delta/silver/vwap"),
        )

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        raw_df
        .select(from_json(col("value").cast("string"), TRADE_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", to_timestamp(col("ts") / 1000))
    )

    # VWAP = Σ(price × qty) / Σ(qty)
    vwap_df = (
        parsed_df
        .withWatermark("event_time", watermark)
        .groupBy(window("event_time", window_dur, slide_dur), col("inst_id"))
        .agg(
            (_sum(col("price") * col("qty")) / _sum(col("qty"))).alias("vwap"),
            _sum("qty").alias("volume"),
            _sum(col("price") * col("qty")).alias("turnover"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("inst_id").alias("symbol"),
            col("vwap"),
            col("volume"),
            col("turnover"),
        )
    )

    try:
        query = (
            vwap_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .start(delta_path)
        )
        logger.info(
            "[silver_flow:binance_vwap] Started — topic=%s, window=%s/%s, sink=%s",
            topic, window_dur, slide_dur, delta_path,
        )
        return query
    except Exception as exc:
        logger.error(
            "[silver_flow:binance_vwap] Failed to start streaming query: %s", exc, exc_info=True,
        )
        raise


# ─── Job 2: Funding Rate Z-Score Anomaly ──────────────────────────────────────

def run_funding_anomaly_stream(spark: SparkSession, kafka_brokers: str, cfg: dict):
    """
    Job 2 — Rolling Z-Score Funding Rate Anomaly Detection.

    Khi |Z-score| ≥ threshold (mặc định 2.5) → publish cảnh báo vào:
        okx.alerts.funding-anomaly Kafka topic

    Config (từ exchange_config.yaml section spark_streaming.funding_anomaly):
        window_duration:   "1 hour"
        slide_duration:    "10 minutes"
        watermark:         "1 minute"
        z_score_threshold: 2.5
        checkpoint:        "/tmp/spark_checkpoints/funding_anomaly"

    Args:
        spark: SparkSession đã khởi tạo
        kafka_brokers: bootstrap servers string
        cfg: dict từ spark_streaming section

    Returns:
        StreamingQuery handle
    """
    src_topic   = cfg.get("kafka_topic_funding_rates", "okx.funding.rates")
    alert_topic = cfg.get("kafka_topic_alert_funding", "okx.alerts.funding-anomaly")
    fa_cfg      = cfg.get("funding_anomaly", {})
    window_dur  = fa_cfg.get("window_duration", "1 hour")
    slide_dur   = fa_cfg.get("slide_duration", "10 minutes")
    watermark   = fa_cfg.get("watermark", "1 minute")
    threshold   = float(fa_cfg.get("z_score_threshold", 2.5))
    checkpoint  = fa_cfg.get("checkpoint", "/tmp/spark_checkpoints/funding_anomaly")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", src_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    funding_df = (
        raw_df
        .select(from_json(col("value").cast("string"), FUNDING_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", to_timestamp(col("ts") / 1000))
    )

    # Rolling stats trong cửa sổ window_dur / slide_dur
    stats_df = (
        funding_df
        .withWatermark("event_time", watermark)
        .groupBy(window("event_time", window_dur, slide_dur), col("inst_id"))
        .agg(
            avg("funding_rate").alias("mean_funding"),
            stddev("funding_rate").alias("std_funding"),
            _sum("funding_rate").alias("latest_rate"),
        )
    )

    # Z-Score = (X - μ) / σ  → filter anomaly
    anomaly_df = (
        stats_df
        .filter(col("std_funding") > 0.000001)
        .withColumn("z_score", (col("latest_rate") - col("mean_funding")) / col("std_funding"))
        .filter(_abs(col("z_score")) >= threshold)
        .select(
            col("inst_id").alias("key"),
            to_json(struct(
                col("inst_id"),
                col("z_score"),
                col("latest_rate"),
                col("window.start").alias("start_time"),
                col("window.end").alias("end_time"),
            )).alias("value"),
        )
    )

    try:
        query = (
            anomaly_df.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_brokers)
            .option("topic", alert_topic)
            .option("checkpointLocation", checkpoint)
            .start()
        )
        logger.info(
            "[silver_flow:binance_funding_anomaly] Started — z_threshold=%.2f, alert_topic=%s",
            threshold, alert_topic,
        )
        return query
    except Exception as exc:
        logger.error(
            "[silver_flow:binance_funding_anomaly] Failed to start streaming query: %s",
            exc, exc_info=True,
        )
        raise


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    setup_logging()
    kafka_brokers = get_kafka_brokers()
    cfg           = get_spark_streaming_cfg()
    spark         = create_spark_session()

    logger.info("=== Binance Silver Flow (PySpark Structured Streaming) Started ===")
    q1 = run_vwap_stream(spark, kafka_brokers, cfg)
    q2 = run_funding_anomaly_stream(spark, kafka_brokers, cfg)

    spark.streams.awaitAnyTermination()
