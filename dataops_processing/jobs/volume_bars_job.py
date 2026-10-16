"""
volume_bars_job.py — Simplified Volume Bars Batch Job.

Đọc Delta Bronze (binance_candles_1d) → tính Volume Bars theo threshold →
ghi Delta Silver (volume_bars).

Volume Bars (De Prado simplified):
    - Aggregate theo cumulative volume threshold thay vì thời gian
    - 1 bar = khi cumsum volume vượt ngưỡng N (config per symbol trong YAML)
    - Output: OHLCV per bar với bar_index, open_ts, close_ts

Config & Paths đến từ:
    Dependency Injection từ ProcessingOrchestrator (Settings + PathResolver).
    KHÔNG có hardcode nào trong file này.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional

from dataops_processing.core.base_job import BaseProcessingJob
from dataops_processing.core.registry import JobRegistry
from dataops_processing.log.logger_setup import logger_manager

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logger_manager.get_logger(__name__)


# =============================================================================
# Core algorithm — pure Python (testable mà không cần Spark)
# =============================================================================

def assign_volume_bars(rows: list[dict], threshold: float) -> list[dict]:
    """
    Assign bar index theo cumulative volume threshold.

    Args:
        rows:      List of candle dicts, sorted by ts_open_ms ascending.
                   Required keys: ts_open_ms, ts_close_ms, open, high, low, close, volume.
        threshold: Volume per bar (từ settings.volume_bars_job.thresholds[symbol]).

    Returns:
        List of VolumeBar-compatible dicts — 1 dict per bar với OHLCV aggregated.
        Empty list nếu rows rỗng.

    Algorithm:
        O(n) — duyệt tuần tự, cộng dồn volume.
        Khi cumsum >= threshold → đóng bar hiện tại, reset state cho bar tiếp theo.
        Partial bar cuối được emit dù chưa đủ threshold.
    """
    if not rows:
        return []

    bars: list[dict] = []
    bar_index = 0
    cum_vol   = 0.0
    bar_open  = rows[0]["open"]
    bar_high  = rows[0]["high"]
    bar_low   = rows[0]["low"]
    bar_open_ts  = rows[0]["ts_open_ms"]
    bar_close_ts = rows[0]["ts_close_ms"]
    num_candles  = 0

    for i, row in enumerate(rows):
        cum_vol      += row["volume"]
        bar_high      = max(bar_high, row["high"])
        bar_low       = min(bar_low,  row["low"])
        bar_close     = row["close"]
        bar_close_ts  = row["ts_close_ms"]
        num_candles  += 1

        if cum_vol >= threshold:
            bars.append({
                "bar_index":    bar_index,
                "open":         bar_open,
                "high":         bar_high,
                "low":          bar_low,
                "close":        bar_close,
                "total_volume": cum_vol,
                "num_candles":  num_candles,
                "bar_open_ts":  bar_open_ts,
                "bar_close_ts": bar_close_ts,
            })
            bar_index += 1
            # O(1) reset — dùng index thay vì rows.index(row) O(n)
            if i + 1 < len(rows):
                next_row     = rows[i + 1]
                bar_open     = next_row["open"]
                bar_high     = next_row["high"]
                bar_low      = next_row["low"]
                bar_open_ts  = next_row["ts_open_ms"]
                bar_close_ts = next_row["ts_close_ms"]
            cum_vol     = 0.0
            num_candles = 0

    # Partial bar cuối nếu còn data chưa đóng bar
    if cum_vol > 0 and num_candles > 0:
        bars.append({
            "bar_index":    bar_index,
            "open":         bar_open,
            "high":         bar_high,
            "low":          bar_low,
            "close":        bar_close,
            "total_volume": cum_vol,
            "num_candles":  num_candles,
            "bar_open_ts":  bar_open_ts,
            "bar_close_ts": bar_close_ts,
        })

    return bars


# =============================================================================
# Concrete Strategy: Volume Bars Batch Job
# =============================================================================

@JobRegistry.register("volume-bars")
class VolumeBarsBatchJob(BaseProcessingJob):
    """
    Job tính toán Volume Bars batch từ Delta Bronze sang Delta Silver.
    """

    @property
    def name(self) -> str:
        return "volume-bars"

    @property
    def job_type(self) -> str:
        return "batch"

    def execute(
        self,
        spark: Optional["SparkSession"] = None,
        symbol: Optional[str] = None,
        **kwargs: Any,
    ) -> dict:
        """
        Chạy Volume Bars batch job.

        Args:
            spark: SparkSession tùy chọn.
            symbol: Specific symbol (e.g. 'BTCUSDT'). None = chạy tất cả symbols trong config.

        Returns:
            {"written": int, "symbols": list, "errors": int}
        """
        try:
            from pyspark.sql import functions as F
            from pyspark.sql.types import (
                DoubleType, IntegerType, LongType, StringType,
                StructField, StructType,
            )
        except ImportError as exc:
            logger.error("PySpark or Delta not installed: %s", exc, exc_info=True)
            return {"written": 0, "symbols": [], "errors": 1}

        vb_cfg = self.settings.volume_bars_job
        symbols_to_run = [symbol] if symbol else list(vb_cfg.thresholds.keys())
        if not symbols_to_run:
            logger.warning("volume_bars_job: no symbols configured in processing_config.yaml")
            return {"written": 0, "symbols": [], "errors": 0}

        s = self.settings.spark
        m = self.settings.minio

        created_spark_internally = False
        if spark is None:
            from pyspark.sql import SparkSession
            from delta import configure_spark_with_delta_pip
            builder = (
                SparkSession.builder
                .appName("dataops-volume-bars-job")
                .master(s.master)
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.jars.packages", s.spark_packages_str())
                .config("spark.hadoop.fs.s3a.endpoint",               m.endpoint)
                .config("spark.hadoop.fs.s3a.access.key",             m.access_key)
                .config("spark.hadoop.fs.s3a.secret.key",             m.secret_key)
                .config("spark.hadoop.fs.s3a.path.style.access",      str(m.path_style_access).lower())
                .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(m.ssl_enabled).lower())
                .config("spark.sql.shuffle.partitions",               str(s.shuffle_partitions))
            )

            try:
                spark = configure_spark_with_delta_pip(builder).getOrCreate()
                spark.sparkContext.setLogLevel(s.log_level)
                created_spark_internally = True
            except Exception as exc:
                logger.error("volume_bars_job: SparkSession creation failed: %s", exc, exc_info=True)
                return {"written": 0, "symbols": symbols_to_run, "errors": 1}

        out_schema = StructType([
            StructField("symbol",       StringType(),  False),
            StructField("inst_type",    StringType(),  False),
            StructField("bar_index",    IntegerType(), False),
            StructField("open",         DoubleType(),  False),
            StructField("high",         DoubleType(),  False),
            StructField("low",          DoubleType(),  False),
            StructField("close",        DoubleType(),  False),
            StructField("total_volume", DoubleType(),  False),
            StructField("num_candles",  IntegerType(), False),
            StructField("bar_open_ts",  LongType(),    False),
            StructField("bar_close_ts", LongType(),    False),
            StructField("ingested_at",  StringType(),  False),
        ])

        bronze_path = self.paths.delta_bronze_path(vb_cfg.source_table)
        silver_path = self.paths.delta_silver_path(vb_cfg.delta_table)
        now_iso     = datetime.now(timezone.utc).isoformat()

        total_written = 0
        errors        = 0
        ok_symbols    = []

        for sym in symbols_to_run:
            threshold = vb_cfg.thresholds.get(sym)
            if threshold is None:
                logger.warning("volume_bars_job: no threshold for %s — skip (add to config)", sym)
                continue

            try:
                bronze_df = (
                    spark.read.format("delta")
                    .load(bronze_path)
                    .filter(F.col("symbol") == sym)
                    .filter(F.col("interval") == vb_cfg.source_interval)
                    .orderBy("ts_open_ms")
                    .select("symbol", "inst_type", "ts_open_ms", "ts_close_ms",
                            "open", "high", "low", "close", "volume")
                )

                rows = [r.asDict() for r in bronze_df.collect()]
                if not rows:
                    logger.warning("volume_bars_job: no bronze data for %s", sym)
                    continue

                inst_type = rows[0].get("inst_type", "SPOT")
                bars      = assign_volume_bars(rows, threshold)

                if not bars:
                    logger.warning("volume_bars_job: no bars generated for %s (threshold=%.0f)", sym, threshold)
                    continue

                records = [
                    {"symbol": sym, "inst_type": inst_type, "ingested_at": now_iso, **bar}
                    for bar in bars
                ]

                bar_df = spark.createDataFrame(records, schema=out_schema)
                (
                    bar_df.write.format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", str(vb_cfg.overwrite_schema).lower())
                    .partitionBy("symbol")
                    .save(silver_path)
                )

                total_written += len(records)
                ok_symbols.append(sym)
                logger.info("volume_bars_job: %s → %d bars (threshold=%.0f)", sym, len(bars), threshold)

            except Exception as exc:
                logger.error("volume_bars_job: symbol=%s failed — %s", sym, exc, exc_info=True)
                errors += 1

        if created_spark_internally:
            spark.stop()

        result = {"written": total_written, "symbols": ok_symbols, "errors": errors}
        logger.info("volume_bars_job: DONE %s", result)
        return result


def run(symbol: Optional[str] = None, spark: Optional["SparkSession"] = None) -> dict:
    """Backward-compatible functional entry point."""
    from dataops_processing.config.paths import get_path_resolver
    from dataops_processing.config.settings import get_settings
    job = VolumeBarsBatchJob(settings=get_settings(), paths=get_path_resolver())
    return job.run(spark=spark, symbol=symbol)
