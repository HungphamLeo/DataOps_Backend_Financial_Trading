"""
order_flow_job.py — Spark Structured Streaming Order Flow / CVD Job (TASK-P2).

Pipeline:
    Kafka (binance.trade.ticks)
        → TradeEvent.from_kafka_bytes()         [canonical deserialization]
        → withWatermark("event_time", cfg.watermark)
        → groupBy(window(cfg.window_duration), symbol)    [tumbling window]
        → buy_volume / sell_volume / delta (= buy - sell) / imbalance
        → InfluxDB foreachBatch (measurement = "order_flow")

CVD (Cumulative Volume Delta):
    Spark chỉ emit `delta` theo từng window đã đóng (stateless sau watermark).
    CVD = cumulativeSum(delta) được tính ở tầng query (Flux `cumulativeSum()`),
    nên job restart không làm mất / lệch CVD.

Imbalance = (buy - sell) / (buy + sell) ∈ [-1, 1]  (>0: người mua chủ động chiếm ưu thế).

Ranh giới trách nhiệm: Binance ticks only. BSA flow của TCBS là dữ liệu REST (batch_api),
không đi qua Kafka nên không nằm trong job này.

Config & Paths đến từ Dependency Injection (Settings + PathResolver) — không hardcode.
"""
from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

from dataops_processing.core.base_job import BaseProcessingJob
from dataops_processing.core.registry import JobRegistry
from dataops_processing.log.logger_setup import logger_manager

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logger_manager.get_logger(__name__)

_BUY_SIDES = frozenset({"buy", "b"})
_SELL_SIDES = frozenset({"sell", "s"})


# =============================================================================
# Pure-Python core (testable độc lập, không cần Spark)
# =============================================================================

def classify_side(side: Optional[str]) -> Optional[str]:
    """Chuẩn hóa side → 'buy' | 'sell' | None (không xác định → bỏ qua)."""
    if side is None:
        return None
    s = str(side).strip().lower()
    if s in _BUY_SIDES:
        return "buy"
    if s in _SELL_SIDES:
        return "sell"
    return None


def compute_order_flow_metrics(buy_volume: float, sell_volume: float) -> Dict[str, float]:
    """
    Tính delta & imbalance cho một window.

    Returns:
        {"buy_volume", "sell_volume", "delta", "imbalance"}
        imbalance = 0.0 khi tổng volume = 0.
    """
    total = buy_volume + sell_volume
    delta = buy_volume - sell_volume
    imbalance = (delta / total) if total > 0 else 0.0
    return {
        "buy_volume": buy_volume,
        "sell_volume": sell_volume,
        "delta": delta,
        "imbalance": imbalance,
    }


def aggregate_trades(
    trades: Iterable[Tuple[str, float, str, int]],
    window_ms: int,
) -> List[Dict[str, Any]]:
    """
    Reference implementation (không Spark) của window aggregation — dùng cho unit test.

    Args:
        trades: iterable (symbol, size, side, ts_ms)
        window_ms: độ dài tumbling window (milliseconds)

    Returns:
        List dict theo (symbol, window_start_ms) tăng dần, kèm cvd tích lũy theo symbol.
    """
    buckets: Dict[Tuple[str, int], List[float]] = {}
    for symbol, size, side, ts_ms in trades:
        klass = classify_side(side)
        if klass is None or size <= 0:
            continue
        key = (symbol, (ts_ms // window_ms) * window_ms)
        acc = buckets.setdefault(key, [0.0, 0.0, 0])
        acc[0 if klass == "buy" else 1] += size
        acc[2] += 1

    rows: List[Dict[str, Any]] = []
    cvd: Dict[str, float] = {}
    for (symbol, w_start) in sorted(buckets, key=lambda k: (k[0], k[1])):
        buy, sell, n = buckets[(symbol, w_start)]
        m = compute_order_flow_metrics(buy, sell)
        cvd[symbol] = cvd.get(symbol, 0.0) + m["delta"]
        rows.append({
            "symbol": symbol,
            "window_start_ms": w_start,
            "window_end_ms": w_start + window_ms,
            "num_trades": int(n),
            "cvd": cvd[symbol],
            **m,
        })
    return rows


def to_line_protocol(
    measurement: str,
    symbol: str,
    interval: str,
    buy_volume: float,
    sell_volume: float,
    delta: float,
    imbalance: float,
    num_trades: int,
    ts_ns: int,
) -> str:
    """Dựng 1 InfluxDB line protocol record (tag escape: space, comma, equals)."""
    def esc(v: str) -> str:
        return v.replace("\\", "\\\\").replace(",", "\\,").replace(" ", "\\ ").replace("=", "\\=")

    return (
        f"{measurement},symbol={esc(symbol)},interval={esc(interval)} "
        f"buy_volume={float(buy_volume)},sell_volume={float(sell_volume)},"
        f"delta={float(delta)},imbalance={float(imbalance)},"
        f"num_trades={int(num_trades)}i {int(ts_ns)}"
    )


# =============================================================================
# InfluxDB sink — foreachBatch
# =============================================================================

def _write_to_influxdb(df_pandas, batch_id: int) -> None:
    """
    foreachBatch sink: ghi order_flow rows vào InfluxDB.

    df_pandas columns: symbol, window_start, window_end, interval,
                       buy_volume, sell_volume, delta, imbalance, num_trades

    InfluxDB point schema:
        measurement: "order_flow"
        tag:   symbol, interval
        field: buy_volume, sell_volume, delta, imbalance (float), num_trades (integer)
        time:  window_end (UTC nanoseconds)
    """
    if df_pandas.empty:
        return

    from dataops_processing.config.settings import get_settings
    settings = get_settings()
    cfg = settings.influxdb
    measurement = settings.order_flow_job.influx_measurement

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
        records = [
            to_line_protocol(
                measurement=measurement,
                symbol=row["symbol"],
                interval=row["interval"],
                buy_volume=row["buy_volume"],
                sell_volume=row["sell_volume"],
                delta=row["delta"],
                imbalance=row["imbalance"],
                num_trades=row["num_trades"],
                ts_ns=int(row["window_end"].timestamp() * 1_000_000_000),
            )
            for _, row in df_pandas.iterrows()
        ]
        write_api.write(
            bucket=cfg.bucket,
            org=cfg.org,
            record=records,
            write_precision=WritePrecision.NANOSECONDS,
        )
        logger.info("batch_id=%d: wrote %d order_flow points to InfluxDB", batch_id, len(records))
    except Exception as exc:
        logger.error("batch_id=%d: InfluxDB write failed — %s", batch_id, exc, exc_info=True)
    finally:
        client.close()


def _foreach_batch(df, batch_id: int) -> None:
    """Spark DataFrame → pandas → InfluxDB (micro-batch nhỏ, sau aggregation)."""
    _write_to_influxdb(df.toPandas(), batch_id)


# =============================================================================
# Concrete Strategy: Order Flow Streaming Job
# =============================================================================

@JobRegistry.register("order-flow")
class OrderFlowStreamingJob(BaseProcessingJob):
    """Job streaming tính buy/sell volume, delta (CVD component) và imbalance từ Kafka ticks."""

    @property
    def name(self) -> str:
        return "order-flow"

    @property
    def job_type(self) -> str:
        return "streaming"

    def execute(self, spark: Optional["SparkSession"] = None, **kwargs: Any) -> None:
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            DoubleType, LongType, StringType, StructField, StructType, TimestampType,
        )

        from dataops_processing.config.spark_config import build_spark_session, get_kafka_options
        from dataops_processing.models.trade import TradeEvent

        cfg = self.settings.order_flow_job
        correlation_id = str(uuid.uuid4())
        logger.info("order_flow_job.start correlation_id=%s", correlation_id)

        if spark is None:
            spark = build_spark_session("dataops-order-flow-job")

        checkpoint = self.paths.checkpoint_path("order_flow_job")
        logger.info(
            "Order flow job config — topic=%s window=%s watermark=%s checkpoint=%s",
            self.settings.kafka.topic_trade_ticks, cfg.window_duration, cfg.watermark, checkpoint,
        )

        raw_stream = (
            spark.readStream
            .format("kafka")
            .options(**get_kafka_options())
            .load()
        )

        trade_schema = StructType([
            StructField("symbol", StringType(), nullable=False),
            StructField("price",  DoubleType(), nullable=False),
            StructField("size",   DoubleType(), nullable=False),
            StructField("side",   StringType(), nullable=True),
            StructField("ts_ms",  LongType(),   nullable=False),
        ])

        @F.udf(returnType=trade_schema)
        def parse_trade_udf(raw: bytes):
            event = TradeEvent.from_kafka_bytes(raw)
            return None if event is None else event.to_tuple()

        side_lc = F.lower(F.col("trade.side"))
        is_buy = side_lc.isin("buy", "b")
        is_sell = side_lc.isin("sell", "s")

        parsed = (
            raw_stream
            .select(parse_trade_udf(F.col("value")).alias("trade"))
            .filter(F.col("trade").isNotNull())
            .filter(is_buy | is_sell)          # side không xác định → loại, tránh làm lệch delta
            .select(
                F.col("trade.symbol").alias("symbol"),
                F.col("trade.size").alias("size"),
                is_buy.alias("is_buy"),
                (F.col("trade.ts_ms") / 1000).cast(TimestampType()).alias("event_time"),
            )
        )

        order_flow = (
            parsed
            .withWatermark("event_time", cfg.watermark)
            .groupBy(
                F.window("event_time", cfg.window_duration).alias("window"),
                F.col("symbol"),
            )
            .agg(
                F.sum(F.when(F.col("is_buy"), F.col("size")).otherwise(F.lit(0.0))).alias("buy_volume"),
                F.sum(F.when(~F.col("is_buy"), F.col("size")).otherwise(F.lit(0.0))).alias("sell_volume"),
                F.count("*").alias("num_trades"),
            )
            .select(
                F.col("symbol"),
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.lit(cfg.influx_interval_tag).alias("interval"),
                F.col("buy_volume"),
                F.col("sell_volume"),
                (F.col("buy_volume") - F.col("sell_volume")).alias("delta"),
                F.when(
                    (F.col("buy_volume") + F.col("sell_volume")) > 0,
                    (F.col("buy_volume") - F.col("sell_volume"))
                    / (F.col("buy_volume") + F.col("sell_volume")),
                ).otherwise(F.lit(0.0)).alias("imbalance"),
                F.col("num_trades"),
            )
        )

        (
            order_flow
            .writeStream
            .outputMode(cfg.output_mode)
            .trigger(processingTime=cfg.trigger_interval)
            .foreachBatch(_foreach_batch)
            .option("checkpointLocation", f"{checkpoint}/influxdb")
            .queryName("order_flow_to_influxdb")
            .start()
        )

        logger.info("Order flow stream started — awaiting termination correlation_id=%s", correlation_id)
        spark.streams.awaitAnyTermination()
        logger.info("Order flow job terminated correlation_id=%s", correlation_id)


def run(spark: Optional["SparkSession"] = None) -> None:
    """Backward-compatible functional entry point."""
    from dataops_processing.config.paths import get_path_resolver
    from dataops_processing.config.settings import get_settings
    job = OrderFlowStreamingJob(settings=get_settings(), paths=get_path_resolver())
    job.run(spark=spark)
