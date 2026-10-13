"""
indicator_matrix_job.py — Multi-Timeframe Technical Indicator Matrix Batch Job.

Tính toán ma trận chỉ báo kỹ thuật cơ bản:
    - EMA (Exponential Moving Average): 20, 50, 200
    - ATR (Average True Range): 14 (Wilder's smoothing)
    - RSI (Relative Strength Index): 14 (Wilder's smoothed gains/losses)
    - Bollinger Bands: 20 period, 2.0 std (Upper, Middle, Lower, Bandwidth, %B)

Nguồn dữ liệu:
    - Delta Bronze: Binance Candles (1d, 1h) & TCBS Market Snapshot (1d)
Output:
    - Delta Silver / Gold: indicator_matrix (partitioned by symbol, interval)

Thiết kế:
    - Pure Python core algorithms (dễ test, deterministic, không phụ thuộc cụm Spark)
    - PySpark batch execution cho distributed computation và Delta ACID persistence
    - DI config và path resolver qua ProcessingOrchestrator
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from dataops_processing.core.base_job import BaseProcessingJob
from dataops_processing.core.registry import JobRegistry
from dataops_processing.log.logger_setup import logger_manager

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logger_manager.get_logger(__name__)


# =============================================================================
# Core Pure-Python Algorithms (Testable độc lập với pytest không cần Spark)
# =============================================================================

def calculate_ema(closes: List[float], period: int) -> List[float]:
    """
    Tính Exponential Moving Average (EMA) cho chuỗi giá đóng cửa.
    
    Formula:
        Multiplier k = 2 / (period + 1)
        EMA_0 = SMA_initial
        EMA_t = Close_t * k + EMA_{t-1} * (1 - k)
    """
    if not closes or period <= 0:
        return []
    
    n = len(closes)
    ema = [0.0] * n
    if n < period:
        # Nếu không đủ dữ liệu, dùng mean lũy tiến
        running_sum = 0.0
        for i, c in enumerate(closes):
            running_sum += c
            ema[i] = running_sum / (i + 1)
        return ema

    # SMA cho period đầu tiên
    initial_sma = sum(closes[:period]) / period
    for i in range(period - 1):
        ema[i] = sum(closes[:i+1]) / (i + 1)
    ema[period - 1] = initial_sma

    k = 2.0 / (period + 1)
    for i in range(period, n):
        ema[i] = (closes[i] * k) + (ema[i - 1] * (1.0 - k))

    return ema


def calculate_atr(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    period: int = 14,
) -> List[float]:
    """
    Tính Average True Range (ATR) với Wilder's Smoothing.
    
    TR_t = max(High_t - Low_t, |High_t - Close_{t-1}|, |Low_t - Close_{t-1}|)
    ATR_t = (ATR_{t-1} * (period - 1) + TR_t) / period
    """
    n = len(closes)
    if n == 0 or len(highs) != n or len(lows) != n or period <= 0:
        return []

    tr = [0.0] * n
    tr[0] = highs[0] - lows[0]
    for i in range(1, n):
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i - 1])
        lc = abs(lows[i] - closes[i - 1])
        tr[i] = max(hl, hc, lc)

    atr = [0.0] * n
    if n < period:
        cum_tr = 0.0
        for i in range(n):
            cum_tr += tr[i]
            atr[i] = cum_tr / (i + 1)
        return atr

    initial_atr = sum(tr[:period]) / period
    for i in range(period - 1):
        atr[i] = sum(tr[:i+1]) / (i + 1)
    atr[period - 1] = initial_atr

    for i in range(period, n):
        atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period

    return atr


def calculate_rsi(closes: List[float], period: int = 14) -> List[float]:
    """
    Tính Relative Strength Index (RSI) với Wilder's Smoothed Moving Average.
    
    RSI = 100 - (100 / (1 + RS))
    RS = Avg Gain / Avg Loss
    """
    n = len(closes)
    if n == 0 or period <= 0:
        return []

    rsi = [50.0] * n
    if n <= 1:
        return rsi

    gains = [0.0] * n
    losses = [0.0] * n

    for i in range(1, n):
        delta = closes[i] - closes[i - 1]
        if delta > 0:
            gains[i] = delta
        else:
            losses[i] = abs(delta)

    if n <= period:
        avg_gain = sum(gains) / max(n - 1, 1)
        avg_loss = sum(losses) / max(n - 1, 1)
        for i in range(1, n):
            cur_g = sum(gains[1:i+1]) / i
            cur_l = sum(losses[1:i+1]) / i
            if cur_l == 0:
                rsi[i] = 100.0 if cur_g > 0 else 50.0
            else:
                rs = cur_g / cur_l
                rsi[i] = 100.0 - (100.0 / (1.0 + rs))
        return rsi

    avg_gain = sum(gains[1:period+1]) / period
    avg_loss = sum(losses[1:period+1]) / period

    for i in range(1, period):
        cg = sum(gains[1:i+1]) / i
        cl = sum(losses[1:i+1]) / i
        if cl == 0:
            rsi[i] = 100.0 if cg > 0 else 50.0
        else:
            rsi[i] = 100.0 - (100.0 / (1.0 + (cg / cl)))

    if avg_loss == 0:
        rsi[period] = 100.0 if avg_gain > 0 else 50.0
    else:
        rs = avg_gain / avg_loss
        rsi[period] = 100.0 - (100.0 / (1.0 + rs))

    for i in range(period + 1, n):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsi[i] = 100.0 if avg_gain > 0 else 50.0
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100.0 - (100.0 / (1.0 + rs))

    return rsi


def calculate_bollinger_bands(
    closes: List[float],
    period: int = 20,
    num_std: float = 2.0,
) -> Tuple[List[float], List[float], List[float], List[float], List[float]]:
    """
    Tính Bollinger Bands:
        Middle Band = SMA(period)
        Upper Band  = Middle + (num_std * StdDev)
        Lower Band  = Middle - (num_std * StdDev)
        Bandwidth   = (Upper - Lower) / Middle
        %B          = (Close - Lower) / (Upper - Lower)
    """
    n = len(closes)
    if n == 0 or period <= 0:
        return [], [], [], [], []

    upper = [0.0] * n
    middle = [0.0] * n
    lower = [0.0] * n
    bandwidth = [0.0] * n
    pct_b = [0.5] * n

    for i in range(n):
        window_start = max(0, i - period + 1)
        window = closes[window_start:i+1]
        m = sum(window) / len(window)
        middle[i] = m

        if len(window) > 1:
            variance = sum((x - m) ** 2 for x in window) / (len(window) - 1)
            std = math.sqrt(variance)
        else:
            std = 0.0

        up = m + num_std * std
        lo = m - num_std * std
        upper[i] = up
        lower[i] = lo

        band_spread = up - lo
        if m > 0:
            bandwidth[i] = band_spread / m
        if band_spread > 0:
            pct_b[i] = (closes[i] - lo) / band_spread

    return upper, middle, lower, bandwidth, pct_b


def compute_indicator_matrix(
    rows: List[Dict[str, Any]],
    ema_periods: List[int] = [20, 50, 200],
    rsi_period: int = 14,
    atr_period: int = 14,
    bb_period: int = 20,
    bb_std: float = 2.0,
) -> List[Dict[str, Any]]:
    """
    Nhận chuỗi candle dicts và tính toán toàn bộ matrix indicators.
    
    Args:
        rows: Danh sách candles đã sort theo timestamp tăng dần.
              Bắt buộc có: open, high, low, close, volume, timestamp/ts_open_ms.
    """
    if not rows:
        return []

    closes = [float(r["close"]) for r in rows]
    highs = [float(r.get("high", r["close"])) for r in rows]
    lows = [float(r.get("low", r["close"])) for r in rows]
    volumes = [float(r.get("volume", 0.0)) for r in rows]

    # Tính EMAs
    ema_results: Dict[int, List[float]] = {}
    for p in ema_periods:
        ema_results[p] = calculate_ema(closes, p)

    # Tính ATR
    atr_vals = calculate_atr(highs, lows, closes, atr_period)

    # Tính RSI
    rsi_vals = calculate_rsi(closes, rsi_period)

    # Tính Bollinger Bands
    bb_upper, bb_mid, bb_lower, bb_bw, bb_pctb = calculate_bollinger_bands(
        closes, period=bb_period, num_std=bb_std
    )

    out_records: List[Dict[str, Any]] = []
    for i, r in enumerate(rows):
        ts = r.get("ts_open_ms") or (r.get("timestamp", 0) * 1000 if r.get("timestamp", 0) < 1e11 else r.get("timestamp", 0))
        rec: Dict[str, Any] = {
            "symbol": r.get("symbol", ""),
            "interval": r.get("interval", "1d"),
            "timestamp": int(ts),
            "open": closes[i] if "open" not in r else float(r["open"]),
            "high": highs[i],
            "low": lows[i],
            "close": closes[i],
            "volume": volumes[i],
            "rsi_14": round(rsi_vals[i], 4),
            "atr_14": round(atr_vals[i], 4),
            "bb_upper_20": round(bb_upper[i], 4),
            "bb_middle_20": round(bb_mid[i], 4),
            "bb_lower_20": round(bb_lower[i], 4),
            "bb_bandwidth": round(bb_bw[i], 4),
            "bb_pct_b": round(bb_pctb[i], 4),
        }
        for p in ema_periods:
            rec[f"ema_{p}"] = round(ema_results[p][i], 4)

        out_records.append(rec)

    return out_records


# =============================================================================
# Concrete Strategy: Multi-Timeframe Indicator Matrix Batch Job
# =============================================================================

@JobRegistry.register("indicator-matrix")
class IndicatorMatrixBatchJob(BaseProcessingJob):
    """
    Job tính toán ma trận chỉ báo kỹ thuật cơ bản (EMA, ATR, RSI, Bollinger Bands)
    cho nhiều timeframe và lưu vào Delta Silver/Gold.
    """

    @property
    def name(self) -> str:
        return "indicator-matrix"

    @property
    def job_type(self) -> str:
        return "batch"

    @property
    def description(self) -> str:
        return "Compute multi-timeframe indicator matrix (EMA 20/50/200, ATR 14, RSI 14, Bollinger Bands 20) batch job"

    def execute(
        self,
        spark: Optional["SparkSession"] = None,
        symbol: Optional[str] = None,
        interval: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Thực thi batch job tính toán indicator matrix.

        Args:
            spark: SparkSession tùy chọn.
            symbol: Symbol cụ thể (e.g. 'BTCUSDT' hoặc 'VNINDEX'). None = chạy toàn bộ config.
            interval: Timeframe cụ thể ('1d' hoặc '1h'). None = chạy toàn bộ intervals trong config.
        """
        try:
            from pyspark.sql import functions as F
            from pyspark.sql.types import (
                DoubleType, LongType, StringType, StructField, StructType,
            )
        except ImportError as exc:
            logger.error("PySpark or Delta not installed: %s", exc, exc_info=True)
            return {"written": 0, "symbols": [], "errors": 1}

        cfg = self.settings.indicator_matrix_job
        symbols_to_run = [symbol] if symbol else cfg.symbols
        intervals_to_run = [interval] if interval else cfg.intervals

        s = self.settings.spark
        m = self.settings.minio

        created_spark_internally = False
        if spark is None:
            from pyspark.sql import SparkSession
            from delta import configure_spark_with_delta_pip
            builder = (
                SparkSession.builder
                .appName("dataops-indicator-matrix-job")
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
                logger.error("indicator_matrix_job: SparkSession creation failed: %s", exc, exc_info=True)
                return {"written": 0, "symbols": symbols_to_run, "errors": 1}

        out_schema = StructType([
            StructField("symbol",       StringType(), False),
            StructField("interval",     StringType(), False),
            StructField("timestamp",    LongType(),   False),
            StructField("open",         DoubleType(), False),
            StructField("high",         DoubleType(), False),
            StructField("low",          DoubleType(), False),
            StructField("close",        DoubleType(), False),
            StructField("volume",       DoubleType(), False),
            StructField("ema_20",       DoubleType(), True),
            StructField("ema_50",       DoubleType(), True),
            StructField("ema_200",      DoubleType(), True),
            StructField("rsi_14",       DoubleType(), True),
            StructField("atr_14",       DoubleType(), True),
            StructField("bb_upper_20",  DoubleType(), True),
            StructField("bb_middle_20", DoubleType(), True),
            StructField("bb_lower_20",  DoubleType(), True),
            StructField("bb_bandwidth", DoubleType(), True),
            StructField("bb_pct_b",     DoubleType(), True),
            StructField("computed_at",  StringType(), False),
        ])

        silver_path = self.paths.delta_silver_path(cfg.delta_table)
        now_iso = datetime.now(timezone.utc).isoformat()

        total_written = 0
        errors = 0
        processed_pairs = []

        for sym in symbols_to_run:
            for itv in intervals_to_run:
                try:
                    # Xác định nguồn Bronze phù hợp (Binance 1d/1h hoặc TCBS EOD)
                    is_crypto = "USDT" in sym or "BUSD" in sym or sym in ("BTC", "ETH", "SOL")
                    if is_crypto:
                        table_name = cfg.source_table_binance_1d if itv == "1d" else cfg.source_table_binance_1h
                    else:
                        table_name = cfg.source_table_tcbs_1d

                    bronze_path = self.paths.delta_bronze_path(table_name)
                    bronze_df = (
                        spark.read.format("delta")
                        .load(bronze_path)
                        .filter(F.col("symbol") == sym)
                        .orderBy("ts_open_ms" if "ts_open_ms" in spark.read.format("delta").load(bronze_path).columns else "timestamp")
                    )

                    rows = [r.asDict() for r in bronze_df.collect()]
                    if not rows:
                        logger.warning("indicator_matrix_job: no bronze rows for %s (%s)", sym, itv)
                        continue

                    records = compute_indicator_matrix(
                        rows,
                        ema_periods=cfg.ema_periods,
                        rsi_period=cfg.rsi_period,
                        atr_period=cfg.atr_period,
                        bb_period=cfg.bb_period,
                        bb_std=cfg.bb_std,
                    )

                    if not records:
                        continue

                    for rec in records:
                        rec["symbol"] = sym
                        rec["interval"] = itv
                        rec["computed_at"] = now_iso

                    matrix_df = spark.createDataFrame(records, schema=out_schema)
                    (
                        matrix_df.write.format("delta")
                        .mode("overwrite")
                        .option("overwriteSchema", str(cfg.overwrite_schema).lower())
                        .partitionBy("symbol", "interval")
                        .save(silver_path)
                    )

                    total_written += len(records)
                    processed_pairs.append(f"{sym}_{itv}")
                    logger.info("indicator_matrix_job: %s %s → %d records written", sym, itv, len(records))

                except Exception as exc:
                    logger.error("indicator_matrix_job: %s (%s) failed — %s", sym, itv, exc, exc_info=True)
                    errors += 1

        if created_spark_internally:
            spark.stop()

        result = {
            "written": total_written,
            "processed": processed_pairs,
            "errors": errors,
        }
        logger.info("indicator_matrix_job: DONE %s", result)
        return result


def run(
    symbol: Optional[str] = None,
    interval: Optional[str] = None,
    spark: Optional["SparkSession"] = None,
) -> Dict[str, Any]:
    """Functional entry point."""
    from dataops_processing.config.paths import get_path_resolver
    from dataops_processing.config.settings import get_settings
    job = IndicatorMatrixBatchJob(settings=get_settings(), paths=get_path_resolver())
    return job.run(spark=spark, symbol=symbol, interval=interval)
