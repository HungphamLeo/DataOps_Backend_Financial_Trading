"""
vwap_job.py — Spark Structured Streaming VWAP Job.

Pipeline:
    Kafka (binance.trade.ticks)
        → TradeEvent.from_kafka_bytes()   [canonical deserialization — models/trade.py]
        → withWatermark("event_time", cfg.watermark)
        → groupBy(window(cfg.window_duration / cfg.slide_duration), symbol)
        → VWAP = Σ(price × size) / Σ(size)
        → dual sink:
            1. InfluxDB via _write_to_influxdb (foreachBatch)
            2. Delta Silver s3a://{bucket}/{silver_base}/{delta_table} (append)

Config & Paths đến từ:
    Dependency Injection từ ProcessingOrchestrator (Settings + PathResolver).
    KHÔNG có hardcode nào trong file này.
"""
from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any, Optional

from dataops_processing.core.base_job import BaseProcessingJob
from dataops_processing.core.registry import JobRegistry
from dataops_processing.log.logger_setup import logger_manager

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logger_manager.get_logger(__name__)


# =============================================================================
# InfluxDB sink — foreachBatch pattern (không có native Spark connector)
# =============================================================================

def _write_to_influxdb(df_pandas, batch_id: int) -> None:
    """
    foreachBatch sink: ghi VWAP rows vào InfluxDB bằng influxdb-client.

    df_pandas columns: symbol, window_start, window_end, interval,
                       vwap, total_volume, num_trades

    InfluxDB point schema (phải khớp với GetVWAP Flux query trong backend):
        measurement: "vwap"
        tag:   symbol, interval
        field: vwap (float), volume (float), num_trades (integer)
        time:  window_end (UTC nanoseconds)
    """
    if df_pandas.empty:
        return

    from dataops_processing.config.settings import get_settings
    cfg = get_settings().influxdb

    try:
        from influxdb_client import InfluxDBClient
        from influxdb_client.client.write_api import SYNCHRONOUS
        from influxdb_client.domain.write_precision import WritePrecision
    except ImportError as exc:  # pragma: no cover
        logger.error("influxdb-client not installed: %s", exc, exc_info=True)
        return

    client = InfluxDBClient(url=cfg.url, token=cfg.token, org=cfg.org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    try:
        records = []
        for _, row in df_pandas.iterrows():
            ts_ns = int(row["window_end"].timestamp() * 1_000_000_000)
            records.append(
                f"vwap,"
                f"symbol={row['symbol']},"
                f"interval={row['interval']} "
                f"vwap={row['vwap']},"
                f"volume={row['total_volume']},"
                f"num_trades={int(row['num_trades'])}i "
                f"{ts_ns}"
            )
        write_api.write(
            bucket=cfg.bucket,
            org=cfg.org,
            record=records,
            write_precision=WritePrecision.NANOSECONDS,
        )
        logger.info("batch_id=%d: wrote %d VWAP points to InfluxDB", batch_id, len(records))
    except Exception as exc:
        logger.error("batch_id=%d: InfluxDB write failed — %s", batch_id, exc, exc_info=True)
    finally:
        client.close()


# =============================================================================
# Concrete Strategy: VWAP Streaming Job
# =============================================================================

@JobRegistry.register("vwap")
class VWAPStreamingJob(BaseProcessingJob):
    """
    Job tính toán VWAP real-time dạng streaming từ Kafka ticks sang InfluxDB & Delta Silver.
    """

    @property
    def name(self) -> str:
        return "vwap"

    @property
    def job_type(self) -> str:
        return "streaming"

    def execute(self, spark: Optional["SparkSession"] = None, **kwargs: Any) -> None:
        """Thực thi Spark Structured Streaming pipeline cho VWAP."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            DoubleType, LongType, StringType, StructField, StructType, TimestampType,
        )

        from dataops_processing.config.spark_config import build_spark_session, get_kafka_options
        from dataops_processing.models.trade import TradeEvent

        vwap_cfg = self.settings.vwap_job
        correlation_id = str(uuid.uuid4())
        logger.info("vwap_job.start correlation_id=%s", correlation_id)

        if spark is None:
            spark = build_spark_session("dataops-vwap-job")

        checkpoint_base = self.paths.checkpoint_path("vwap_job")
        delta_path      = self.paths.delta_silver_path(vwap_cfg.delta_table)

        logger.info(
            "VWAP job config — topic=%s window=%s/%s watermark=%s checkpoint=%s delta=%s",
            self.settings.kafka.topic_trade_ticks,
            vwap_cfg.window_duration, vwap_cfg.slide_duration,
            vwap_cfg.watermark, checkpoint_base, delta_path,
        )

        # ── 1. Đọc từ Kafka ───────────────────────────────────────────────────
        raw_stream = (
            spark.readStream
            .format("kafka")
            .options(**get_kafka_options())
            .load()
        )

        # ── 2. Deserialize — UDF gọi TradeEvent.from_kafka_bytes() ───────────
        trade_schema = StructType([
            StructField("symbol", StringType(),  nullable=False),
            StructField("price",  DoubleType(),  nullable=False),
            StructField("size",   DoubleType(),  nullable=False),
            StructField("side",   StringType(),  nullable=True),
            StructField("ts_ms",  LongType(),    nullable=False),
        ])

        @F.udf(returnType=trade_schema)
        def parse_trade_udf(raw: bytes):
            event = TradeEvent.from_kafka_bytes(raw)
            if event is None:
                return None
            return event.to_tuple()

        parsed = (
            raw_stream
            .select(parse_trade_udf(F.col("value")).alias("trade"))
            .filter(F.col("trade").isNotNull())
            .select(
                F.col("trade.symbol").alias("symbol"),
                F.col("trade.price").alias("price"),
                F.col("trade.size").alias("size"),
                F.col("trade.side").alias("side"),
                (F.col("trade.ts_ms") / 1000).cast(TimestampType()).alias("event_time"),
                (F.col("trade.price") * F.col("trade.size")).alias("notional"),
            )
        )

        # ── 3. Watermark + Window aggregation ────────────────────────────────
        with_watermark = parsed.withWatermark("event_time", vwap_cfg.watermark)

        vwap_agg = (
            with_watermark
            .groupBy(
                F.window("event_time", vwap_cfg.window_duration, vwap_cfg.slide_duration).alias("window"),
                F.col("symbol"),
            )
            .agg(
                (F.sum("notional") / F.sum("size")).alias("vwap"),
                F.sum("size").alias("total_volume"),
                F.count("*").alias("num_trades"),
            )
            .select(
                F.col("symbol"),
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.lit(vwap_cfg.influx_interval_tag).alias("interval"),
                F.col("vwap"),
                F.col("total_volume"),
                F.col("num_trades"),
            )
        )

        # ── 4. Dual sink ──────────────────────────────────────────────────────
        # Sink A: InfluxDB
        influx_query = (
            vwap_agg
            .writeStream
            .outputMode(vwap_cfg.output_mode)
            .trigger(processingTime=vwap_cfg.trigger_interval)
            .foreachBatch(_write_to_influxdb)
            .option("checkpointLocation", f"{checkpoint_base}/influxdb")
            .queryName("vwap_to_influxdb")
            .start()
        )

        # Sink B: Delta Silver
        delta_query = (
            vwap_agg
            .writeStream
            .outputMode(vwap_cfg.output_mode)
            .trigger(processingTime=vwap_cfg.trigger_interval)
            .format("delta")
            .option("checkpointLocation", f"{checkpoint_base}/delta")
            .partitionBy("symbol")
            .start(delta_path)
        )

        logger.info("VWAP job streams started — awaiting termination correlation_id=%s", correlation_id)
        spark.streams.awaitAnyTermination()
        logger.info("VWAP job terminated correlation_id=%s", correlation_id)


def run(spark: Optional["SparkSession"] = None) -> None:
    """Backward-compatible functional entry point."""
    from dataops_processing.config.paths import get_path_resolver
    from dataops_processing.config.settings import get_settings
    job = VWAPStreamingJob(settings=get_settings(), paths=get_path_resolver())
    job.run(spark=spark)
