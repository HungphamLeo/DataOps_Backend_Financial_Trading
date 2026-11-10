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

═══════════════════════════════════════════════════════════════════════════════
ST05 — SPARK WATERMARK & PERSISTENT CHECKPOINT (PLAN_REPO03)
═══════════════════════════════════════════════════════════════════════════════

VẤN ĐỀ — Late Data & Unbounded State:
  Người dùng ở vùng 4G yếu — trade event lúc 10:00 đến server lúc 10:05 (5 phút trễ).
  Nếu không có Watermark: Spark phải giữ state của MỌI window trong RAM.
  Sau vài giờ → RAM cạn kiệt → executor OOMKilled → job crash.

Watermark = Bounded Staleness:
  .withWatermark("event_time", "30 seconds")
  → Spark chỉ chấp nhận data trễ tối đa 30 giây.
  → Watermark = max(event_time) - 30s.
  → Khi watermark vượt qua window.end → window được "đóng" → state giải phóng.
  → Data đến sau watermark → bị DROP (ghi vào DLQ nếu cần audit).

  Trade-off quan trọng:
    Watermark ngắn (10s): RAM thoải mái, nhưng drop nhiều late data.
    Watermark dài (5m): Chấp nhận nhiều late data, nhưng state RAM lớn hơn.
    Với Binance WebSocket (độ trễ < 200ms), 30s là conservative và an toàn.

outputMode("append"):
  Chỉ emit kết quả của window ĐÃ ĐÓNG (sau watermark).
  Đảm bảo: InfluxDB không nhận cùng 1 window 2 lần (no partial-write then overwrite).
  Đối lập "update": emit window sau mỗi trigger (bao gồm partial) → InfluxDB duplicate.
  Đối lập "complete": emit TẤT CẢ windows → scan toàn bộ state → cực kỳ tốn RAM.

Persistent Checkpoint (Chandy-Lamport):
  checkpointLocation = s3a://bronze/checkpoints/vwap_job/
  Spark lưu: (1) Kafka offset đang xử lý, (2) State của mọi open window.
  Khi job crash và restart → đọc checkpoint → tiếp tục từ đúng offset.
  Không có checkpoint → restart từ đầu → duplicate VWAP data trong InfluxDB.
  Mỗi sink có checkpoint riêng (/influxdb và /delta) để independent recovery.

═══════════════════════════════════════════════════════════════════════════════
ST13 — END-TO-END IDEMPOTENCY & WRITE-AUDIT-PUBLISH (WAP) (PLAN_REPO03)
═══════════════════════════════════════════════════════════════════════════════

At-Least-Once Delivery (Kafka guarantee):
  Khi Spark restart sau crash, nó có thể re-process 1 số messages từ checkpoint.
  Nếu micro-batch B đã ghi vào InfluxDB rồi crash trước khi update checkpoint →
  Batch B được xử lý lại → InfluxDB nhận cùng VWAP window 2 lần → volume tính đúp.

foreachBatch + UPSERT = Idempotent Sink:
  _write_to_influxdb() nhận mỗi batch với batch_id duy nhất.
  InfluxDB Line Protocol: nếu cùng timestamp + measurement + tags → OVERWRITE.
  → Ghi lại cùng window → InfluxDB thay thế giá trị cũ (không tích lũy).
  → Đây là "Idempotent Sink" — chạy N lần cũng cho kết quả giống chạy 1 lần.

WAP Pattern cho Delta Sink (TODO ST13):
  Hiện tại: ghi thẳng vào delta_path (append mode) → không có audit step.
  WAP chuẩn:
    1. WRITE: ghi vào staging table (delta_path + "_staging").
    2. AUDIT: chạy data quality checks (price > 0, volume > 0, timestamps hợp lý).
    3. PUBLISH: MERGE staging → final table (atomic swap qua Delta transaction).
  → Đảm bảo final table KHÔNG BAO GIỜ có partial/corrupt data.
  → Xem ST13 TODO trong PLAN_REPO03.md để implement đầy đủ.
"""
from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any, Optional

from dataops_processing.core.base_job import BaseProcessingJob
from dataops_processing.core.registry import JobRegistry
from dataops_processing.log.logger_setup import logger_manager
from dataops_processing.core.telemetry import trace

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logger_manager.get_logger(__name__)

def process_micro_batch(batch_df, batch_id):
    tracer = trace.get_tracer("dataops-processing-vwap")
    
    # Đọc headers từ Kafka stream DataFrame (cột `headers`)
    rows = batch_df.collect()
    for row in rows:
        parent_ctx = extract_trace_context(getattr(row, "headers", None))
        with tracer.start_as_current_span(
            "process_trade_record",
            context=parent_ctx,
            attributes={
                "symbol": row.symbol,
                "batch_id": batch_id,
                "spark.job": "vwap_job"
            }
        ) as span:
            # Logic tính toán và ghi sang InfluxDB / Delta Lake
            write_to_influx(row, trace_id=format(span.get_span_context().trace_id, "032x"))

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

                # ── ST05: bounded state + late-data policy ─────────────────────────────
        # Dùng event_time thực sự của message (ts_ms), không dùng ingestion time.
        # Nếu event_time quá cũ hoặc không hợp lệ thì không để state tăng vô hạn.
        # Dữ liệu trễ quá watermark nên bị drop hoặc viết sang DLQ nếu cần.
        # Đây là "bounded state" policy của production stream.
        late_data_cutoff = F.expr("INTERVAL 30 MINUTES")
        if spark is None:
            spark = build_spark_session("dataops-vwap-job")

        checkpoint_base = self.paths.checkpoint_path("vwap_job")
        delta_path      = self.paths.delta_silver_path(vwap_cfg.delta_table)

        logger.info(
            "VWAP job config — topic=%s window=%s/%s watermark=%s checkpoint=%s delta=%s",
            self.settings.kafka.topic_trade_ticks,
            vwap_cfg.window_duration, 
            vwap_cfg.slide_duration,
            vwap_cfg.watermark, 
            checkpoint_base, 
            delta_path,
        )

        # ── 1. Đọc từ Kafka ───────────────────────────────────────────────────
        raw_stream = (
            spark.readStream
            .format("kafka")
            .options(**get_kafka_options(job_name="vwap-job"))
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
            .filter(F.col("price") > 0)
            parsed = (
            raw_stream
            .select(parse_trade_udf(F.col("value")).alias("trade"))
            .filter(F.col("trade").isNotNull())
            .select(
                F.col("trade.symbol").alias("symbol"),
                F.col("trade.price").alias("price"),
                F.col("trade.size").alias("size"),
                F.col("trade.side").alias("side"),
                F.col("trade.ts_ms").cast("long").alias("ts_ms"),
            )
            .filter(F.col("price") > 0)
            .filter(F.col("size") > 0)
            .withColumn("event_time", (F.col("ts_ms") / 1000).cast(TimestampType()))
            .filter(F.col("event_time").isNotNull())
            # ST05: ngăn state bị tăng vô hạn với data quá trễ
            .filter(F.col("event_time") >= F.current_timestamp() - late_data_cutoff)
        )
            .filter(F.col("size") > 0)
            .withColumn("event_time", (F.col("ts_ms") / 1000).cast(TimestampType()))
            .filter(F.col("event_time").isNotNull())
        
        )

        # ── 3. Watermark + Window aggregation ────────────────────────────────
         windowed = (
            parsed
            .withWatermark("event_time", vwap_cfg.watermark)
            .groupBy(
                F.window("event_time", vwap_cfg.window_duration, vwap_cfg.slide_duration).alias("window"),
                F.col("symbol"),
            )
            .agg(
                (F.sum(F.col("price") * F.col("size")) / F.sum("size")).alias("vwap"),
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
        # ST05: query lifecycle / checkpoint health
        # queryName giúp dễ trace trên Spark UI; checkpointLocation là durable recovery.
        # Nếu stream bị stall, checkpoint stale/idle sẽ dễ phát hiện.
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

        # 5. Query lifecycle / monitoring
        # Avoid unbounded state: watermark + append mode + per-window closure
        # late rows beyond watermark = dropped automatically
        influx_query.start()
        delta_query.awaitTermination()

def run(spark: Optional["SparkSession"] = None) -> None:
    """Backward-compatible functional entry point."""
    from dataops_processing.config.paths import get_path_resolver
    from dataops_processing.config.settings import get_settings
    job = VWAPStreamingJob(settings=get_settings(), paths=get_path_resolver())
    job.run(spark=spark)
