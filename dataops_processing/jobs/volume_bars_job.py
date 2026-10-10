"""
volume_bars_job.py — Simplified Volume Bars Batch Job.

Đọc Delta Bronze (binance_candles_1d) → tính Volume Bars theo threshold →
ghi Delta Silver (volume_bars).

Volume Bars (De Prado simplified):
    - Thay vì aggregate theo time (1h candle), aggregate theo volume threshold
    - 1 bar = khi cumulative volume vượt ngưỡng N (config theo symbol)
    - Output: OHLCV per bar với bar_index, open_ts, close_ts

Ý nghĩa tài chính:
    - Volume bars lọc nhiễu khi market ít hoạt động (bars rộng = thời gian dài nhưng ít trade)
    - Giúp phát hiện pattern tốt hơn time bars cho thuật toán mean-reversion

Input:
    Delta Bronze s3a://binance-datalake/delta/bronze/binance_candles_1d
    Schema: symbol, inst_type, interval, ts_open_ms, ts_close_ms, open, high, low, close, volume

Output:
    Delta Silver s3a://binance-datalake/delta/silver/volume_bars
    Schema: symbol, inst_type, bar_index, open, high, low, close,
            total_volume, num_candles, bar_open_ts, bar_close_ts, ingested_at

Config:
    dataops_processing/config/processing_config.yaml → volume_bars section

Chạy:
    python -m dataops_processing.jobs.volume_bars_job --symbol BTCUSDT
    python -m dataops_processing.jobs.volume_bars_job  # all symbols in config
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import yaml

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_CONFIG_PATH = Path(__file__).parent.parent / "config" / "processing_config.yaml"

_S3A_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "http://minio:9010")
_S3A_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
_S3A_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
_BUCKET         = os.environ.get("DELTA_BUCKET",     "binance-datalake")


def _load_config() -> dict:
    with open(_CONFIG_PATH, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _bronze_path(symbol: str) -> str:
    return f"s3a://{_BUCKET}/delta/bronze/binance_candles_1d"


def _silver_path() -> str:
    return f"s3a://{_BUCKET}/delta/silver/volume_bars"


def _s3a_opts() -> dict:
    return {
        "fs.s3a.endpoint":              _S3A_ENDPOINT,
        "fs.s3a.access.key":            _S3A_ACCESS_KEY,
        "fs.s3a.secret.key":            _S3A_SECRET_KEY,
        "fs.s3a.path.style.access":     "true",
        "fs.s3a.impl":                  "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    }


# ---------------------------------------------------------------------------
# Core algorithm — pure Python (không dùng Spark), testable independently
# ---------------------------------------------------------------------------

def assign_volume_bars(rows: list[dict], threshold: float) -> list[dict]:
    """
    Assign bar index theo cumulative volume threshold.

    Args:
        rows:      List of candle dicts, sorted by ts_open_ms ascending.
                   Required keys: ts_open_ms, ts_close_ms, open, high, low, close, volume.
        threshold: Volume per bar, e.g. 500 for BTCUSDT.

    Returns:
        List of dicts — 1 dict per bar với OHLCV aggregated.
        Empty list nếu rows rỗng.

    Algorithm:
        Duyệt tuần tự, cộng dồn volume.
        Khi cumsum >= threshold → đóng bar hiện tại, bắt đầu bar mới.
        Bar cuối được emit dù chưa đủ threshold (partial bar).
    """
    if not rows:
        return []

    bars: list[dict] = []
    bar_index = 0
    cum_vol = 0.0
    bar_open = rows[0]["open"]
    bar_high = rows[0]["high"]
    bar_low  = rows[0]["low"]
    bar_open_ts  = rows[0]["ts_open_ms"]
    bar_close_ts = rows[0]["ts_close_ms"]
    num_candles = 0

    for i, row in enumerate(rows):
        cum_vol     += row["volume"]
        bar_high     = max(bar_high, row["high"])
        bar_low      = min(bar_low,  row["low"])
        bar_close    = row["close"]
        bar_close_ts = row["ts_close_ms"]
        num_candles += 1

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
            # Reset cho bar tiếp theo — O(1) index thay vì O(n) list.index()
            if i + 1 < len(rows):
                next_row = rows[i + 1]
                bar_open     = next_row["open"]
                bar_high     = next_row["high"]
                bar_low      = next_row["low"]
                bar_open_ts  = next_row["ts_open_ms"]
                bar_close_ts = next_row["ts_close_ms"]
            cum_vol     = 0.0
            num_candles = 0

    # Partial bar cuối nếu còn data
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


# ---------------------------------------------------------------------------
# Spark batch job
# ---------------------------------------------------------------------------

def run(symbol: str | None = None) -> dict:
    """
    Chạy Volume Bars batch job.

    Args:
        symbol: Specific symbol (e.g. "BTCUSDT"), None = chạy tất cả symbols trong config.

    Returns:
        {"written": int, "symbols": list, "errors": int}
    """
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            StructType, StructField,
            StringType, IntegerType, DoubleType, LongType,
        )
        from delta import configure_spark_with_delta_pip
    except ImportError as exc:
        logger.error("PySpark or Delta not installed: %s", exc)
        return {"written": 0, "symbols": [], "errors": 1}

    cfg = _load_config()
    vol_thresholds: dict[str, float] = cfg.get("volume_bars", {})

    symbols_to_run = [symbol] if symbol else list(vol_thresholds.keys())
    if not symbols_to_run:
        logger.warning("volume_bars_job: no symbols configured")
        return {"written": 0, "symbols": [], "errors": 0}

    # Build SparkSession
    builder = (
        SparkSession.builder
        .appName("dataops-volume-bars-job")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    for k, v in _s3a_opts().items():
        builder = builder.config(k, v)

    try:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as exc:
        logger.error("volume_bars_job: SparkSession creation failed: %s", exc)
        return {"written": 0, "symbols": symbols_to_run, "errors": 1}

    # Output schema
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

    now_iso = datetime.now(timezone.utc).isoformat()
    total_written = 0
    errors = 0
    ok_symbols = []

    for sym in symbols_to_run:
        threshold = vol_thresholds.get(sym)
        if threshold is None:
            logger.warning("volume_bars_job: no threshold configured for %s — skip", sym)
            continue

        try:
            bronze_df = (
                spark.read.format("delta")
                .load(_bronze_path(sym))
                .filter(F.col("symbol") == sym)
                .filter(F.col("interval") == "1d")
                .orderBy("ts_open_ms")
                .select("symbol", "inst_type", "ts_open_ms", "ts_close_ms",
                        "open", "high", "low", "close", "volume")
            )

            rows = [r.asDict() for r in bronze_df.collect()]
            if not rows:
                logger.warning("volume_bars_job: no bronze data for %s", sym)
                continue

            inst_type = rows[0].get("inst_type", "SPOT")
            bars = assign_volume_bars(rows, threshold)

            if not bars:
                logger.warning("volume_bars_job: no bars generated for %s", sym)
                continue

            # Enrich với symbol/inst_type/ingested_at
            records = [
                {
                    "symbol":       sym,
                    "inst_type":    inst_type,
                    "ingested_at":  now_iso,
                    **bar,
                }
                for bar in bars
            ]

            bar_df = spark.createDataFrame(records, schema=out_schema)
            (
                bar_df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .partitionBy("symbol")
                .save(_silver_path())
            )

            total_written += len(records)
            ok_symbols.append(sym)
            logger.info("volume_bars_job: %s → %d bars (threshold=%.0f)", sym, len(bars), threshold)

        except Exception as exc:
            logger.error("volume_bars_job: %s failed: %s", sym, exc)
            errors += 1

    spark.stop()
    logger.info("volume_bars_job: DONE written=%d symbols=%s errors=%d",
                total_written, ok_symbols, errors)
    return {"written": total_written, "symbols": ok_symbols, "errors": errors}


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s — %(message)s")

    parser = argparse.ArgumentParser(description="Volume Bars Batch Job")
    parser.add_argument("--symbol", type=str, default=None,
                        help="Symbol to process (e.g. BTCUSDT). Default: all from config.")
    args = parser.parse_args()

    result = run(symbol=args.symbol)
    print(result)
