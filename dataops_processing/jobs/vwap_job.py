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
        raise RuntimeError("influxdb-client is required for the VWAP sink") from exc

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
        raise
    finally:
        client.close()

def _merge_vwap_salt_partials(partials_df, interval: str):
    """
    ST12: Gộp VWAP partial aggregates từ các salt bucket thành kết quả cuối.

    Mỗi partial phải có tổng notional (sum(price * size)), tổng volume,
    số trade và window/symbol tương ứng. VWAP cuối được tính bằng tổng
    notional chia tổng volume; không được lấy trung bình các VWAP partial.

    Args:
        partials_df: Spark DataFrame có các cột symbol, window_start,
            window_end, partial_notional, partial_volume và partial_trades.
        interval: Nhãn interval được ghi cùng kết quả VWAP.

    Returns:
        Spark DataFrame có một hàng cho mỗi symbol/window, với các cột
        window_start, window_end, interval, vwap, total_volume, num_trades.
    """
    from pyspark.sql import functions as F

    merged = (
        partials_df
        .groupBy("symbol", "window_start", "window_end")
        .agg(
            F.sum("partial_notional").alias("total_notional"),
            F.sum("partial_volume").alias("total_volume"),
            F.sum("partial_trades").alias("num_trades"),
        )
        .withColumn(
            "vwap",
            F.col("total_notional") / F.col("total_volume"),
        )
        .withColumn("interval", F.lit(interval))
    )

    return merged.select(
        "symbol",
        "window_start",
        "window_end",
        "interval",
        "vwap",
        "total_volume",
        "num_trades",
    )


def _write_vwap_batch(
    batch_df,
    batch_id: int,
    delta_path: str,
    interval: str,
) -> None:
    """
    ST12: Gộp salt partials trong micro-batch rồi ghi VWAP vào Delta và InfluxDB.

    Bước gộp diễn ra trên DataFrame batch thông thường (không phải một
    streaming aggregation thứ hai). Delta transaction options giúp tránh
    append lại cùng batch khi Spark retry với cùng checkpoint và batch ID.

    Args:
        batch_df: Spark DataFrame chứa kết quả partial aggregation đã đóng
            theo watermark.
        batch_id: ID micro-batch do Structured Streaming cấp.
        delta_path: Đường dẫn Delta Silver của bảng VWAP.
        interval: Nhãn interval dùng trong output và InfluxDB.

    Raises:
        Exception: Truyền tiếp lỗi ghi Delta hoặc lỗi sink để Spark không
            đánh dấu batch thất bại là thành công.
    """
    import pandas as pd

    if not batch_df.take(1):
        return

    final_df = _merge_vwap_salt_partials(batch_df, interval)

    # ST13: Write-Audit-Publish thay vì append trực tiếp.
    # Ghi staging, audit batch, rồi MERGE vào bảng Silver chính.
    from dataops_processing.jobs.wap_sink import write_audit_publish_vwap

    wap_audit = write_audit_publish_vwap(
        spark=batch_df.sparkSession,
        final_path=delta_path,
        staging_path=f"{delta_path}_staging/batch_{batch_id}",
        batch_df=final_df,
        batch_id=batch_id,
    )
    logger.info(
        "ST13 WAP published batch_id=%d rows=%d",
        batch_id,
        wap_audit.row_count,
    )

    rows: list[dict[str, Any]] = []
    chunk_size = 500

    for row in final_df.toLocalIterator():
        rows.append(row.asDict(recursive=True))
        if len(rows) >= chunk_size:
            _write_to_influxdb(
                pd.DataFrame.from_records(rows),
                batch_id=batch_id,
            )
            rows.clear()

    if rows:
        _write_to_influxdb(
            pd.DataFrame.from_records(rows),
            batch_id=batch_id,
        )

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
        """
        ST12: Chạy VWAP Structured Streaming với RocksDB và hot-key salting.

        Mỗi trade của hot symbol được gán salt ổn định từ Kafka partition/offset.
        Pha streaming chỉ giữ partial sums theo window, symbol và salt.
        _write_vwap_batch() gộp các partial sums trước khi ghi sink.

        Args:
            spark: SparkSession có sẵn; nếu không truyền, tạo session từ cấu hình.

        Raises:
            ValueError: Nếu salting bật nhưng bucket count hoặc hot symbols không hợp lệ.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        from dataops_processing.config.spark_config import (
            build_spark_session,
            get_kafka_options,
        )
        from dataops_processing.models.trade import TradeEvent

        vwap_cfg = self.settings.vwap_job

        if vwap_cfg.salting_enabled:
            if vwap_cfg.salt_buckets < 2:
                raise ValueError("ST12 salting requires at least 2 salt buckets")
            if not vwap_cfg.hot_symbols:
                raise ValueError("ST12 salting requires at least one hot symbol")

        correlation_id = str(uuid.uuid4())
        logger.info("vwap_job.start correlation_id=%s", correlation_id)

        if spark is None:
            spark = build_spark_session("dataops-vwap-job")

        checkpoint_base = self.paths.checkpoint_path("vwap_job")
        delta_path = self.paths.delta_silver_path(vwap_cfg.delta_table)

        logger.info(
            "VWAP job config — topic=%s window=%s/%s watermark=%s "
            "checkpoint=%s delta=%s salting=%s buckets=%d",
            self.settings.kafka.topic_trade_ticks,
            vwap_cfg.window_duration,
            vwap_cfg.slide_duration,
            vwap_cfg.watermark,
            checkpoint_base,
            delta_path,
            vwap_cfg.salting_enabled,
            vwap_cfg.salt_buckets,
        )

        # Kafka partition/offset được giữ lại để tạo salt ổn định khi replay.
        raw_stream = (
            spark.readStream
            .format("kafka")
            .options(**get_kafka_options(job_name="vwap-job"))
            .load()
        )

        trade_schema = StructType([
            StructField("symbol", StringType(), nullable=False),
            StructField("price", DoubleType(), nullable=False),
            StructField("size", DoubleType(), nullable=False),
            StructField("side", StringType(), nullable=True),
            StructField("ts_ms", LongType(), nullable=False),
            StructField("trade_id", StringType(), nullable=True),
            StructField("idempotency_key", StringType(), nullable=False),
        ])

        @F.udf(returnType=trade_schema)
        def parse_trade_udf(raw: bytes):
            """ST12: Deserialize một Kafka payload thành các field trade chuẩn."""
            event = TradeEvent.from_kafka_bytes(raw)
            if event is None:
                return None
            return event.to_tuple()

        parsed = (
            raw_stream
            .select(
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                F.col("trade.trade_id").alias("trade_id"),
                F.col("trade.idempotency_key").alias("idempotency_key"),
                parse_trade_udf(F.col("value")).alias("trade"),
            )
            .filter(F.col("trade").isNotNull())
            .select(
                "kafka_partition",
                "kafka_offset",
                F.col("trade.symbol").alias("symbol"),
                F.col("trade.price").alias("price"),
                F.col("trade.size").alias("size"),
                F.col("trade.ts_ms").cast("long").alias("ts_ms"),
            )
            .filter(F.col("price") > 0)
            .filter(F.col("size") > 0)
            .withColumn(
                "event_time",
                (F.col("ts_ms") / F.lit(1000.0)).cast(TimestampType()),
            )
            .filter(F.col("event_time").isNotNull())
            .withColumn(
                "notional",
                F.col("price") * F.col("size"),
            )
        )

        # ST12: chỉ chia tải cho hot symbols; các symbol còn lại dùng salt 0.
        if vwap_cfg.salting_enabled:
            parsed = parsed.withColumn(
                "salt",
                F.when(
                    F.col("symbol").isin(*vwap_cfg.hot_symbols),
                    F.pmod(
                        F.xxhash64("kafka_partition", "kafka_offset"),
                        F.lit(vwap_cfg.salt_buckets),
                    ),
                )
                .otherwise(F.lit(0))
                .cast("int"),
            )
        else:
            parsed = parsed.withColumn("salt", F.lit(0))

        # Pha 1 ST12: giữ sum chứ không giữ VWAP trung bình từng salt.
        partials = (
            parsed
            .withWatermark("event_time", vwap_cfg.watermark)
            .groupBy(
                F.window(
                    "event_time",
                    vwap_cfg.window_duration,
                    vwap_cfg.slide_duration,
                ).alias("window"),
                F.col("symbol"),
                F.col("salt"),
            )
            .agg(
                F.sum("notional").alias("partial_notional"),
                F.sum("size").alias("partial_volume"),
                F.count("*").alias("partial_trades"),
            )
            .select(
                "symbol",
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "partial_notional",
                "partial_volume",
                "partial_trades",
            )
        )

        # Một streaming query dùng một checkpoint; foreachBatch gộp salt rồi ghi cả hai sink.
        query = (
            partials.writeStream
            .outputMode("append")
            .trigger(processingTime=vwap_cfg.trigger_interval)
            .foreachBatch(
                lambda batch_df, batch_id: _write_vwap_batch(
                    batch_df,
                    batch_id,
                    delta_path,
                    vwap_cfg.influx_interval_tag,
                )
            )
            .option("checkpointLocation", f"{checkpoint_base}/vwap-st12")
            .queryName("vwap_st12_rocksdb_salted")
            .start()
        )

        logger.info(
            "VWAP ST12 stream started — correlation_id=%s",
            correlation_id,
        )
        query.awaitTermination()

def run(spark: Optional["SparkSession"] = None) -> None:
    """Backward-compatible functional entry point."""
    from dataops_processing.config.paths import get_path_resolver
    from dataops_processing.config.settings import get_settings
    job = VWAPStreamingJob(settings=get_settings(), paths=get_path_resolver())
    job.run(spark=spark)


# python3 -m py_compile \
#   dataops_processing/config/settings.py \
#   dataops_processing/config/spark_config.py \
#   dataops_processing/jobs/vwap_job.py

# python3 -m pytest -q \
#   dataops_processing/tests/test_st12_vwap_skew.py \
#   dataops_processing/tests/test_vwap_job.py