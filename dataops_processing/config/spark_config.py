"""
spark_config.py — SparkSession builder cho dataops_processing.

Nhận SparkSettings từ get_settings() — không đọc os.environ trực tiếp.
Tất cả tham số (packages, master, memory, checkpoint) đến từ processing_config.yaml
với env var override qua settings.py.

Usage:
    from dataops_processing.config.spark_config import build_spark_session, get_kafka_options
    from dataops_processing.config.settings import get_settings

    spark = build_spark_session("dataops-vwap-job")
    kafka_opts = get_kafka_options()
"""
from __future__ import annotations

import logging

from dataops_processing.log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)


def build_spark_session(app_name: str | None = None):
    """
    Tạo SparkSession đã configured với Kafka + Delta + S3A.

    Tất cả config đọc từ ProcessingSettings (settings.py + yaml + env override).
    Không dùng os.environ trực tiếp ở đây.

    Args:
        app_name: Override tên job trên Spark UI.
                  None → dùng settings.spark.app_name từ config.

    Returns:
        pyspark.sql.SparkSession
    """
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "PySpark hoặc delta-spark chưa được cài. "
            "Chạy: pip install pyspark==3.5.1 delta-spark==3.1.0"
        ) from exc

    from dataops_processing.config.settings import get_settings
    cfg = get_settings()
    s   = cfg.spark
    m   = cfg.minio

    name = app_name or s.app_name

    logger.info(
        "Building SparkSession: app=%s master=%s minio=%s",
        name, s.master, m.endpoint,
    )

    builder = (
        SparkSession.builder
        .appName(name)
        .master(s.master)
        # ── Delta Lake ──────────────────────────────────────────────────────
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # ── Spark packages (download tại submit qua Maven) ──────────────────
        .config("spark.jars.packages", s.spark_packages_str())
        # ── S3A / MinIO ─────────────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint",    m.endpoint)
        .config("spark.hadoop.fs.s3a.access.key",  m.access_key)
        .config("spark.hadoop.fs.s3a.secret.key",  m.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", str(m.path_style_access).lower())
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(m.ssl_enabled).lower())
        # ── Streaming graceful shutdown ──────────────────────────────────────
        .config("spark.streaming.stopGracefullyOnShutdown",
                str(s.stop_gracefully_on_shutdown).lower())
        # ── Shuffle / parallelism (local mode tuning) ───────────────────────
        .config("spark.sql.shuffle.partitions", str(s.shuffle_partitions))
        .config("spark.default.parallelism",    str(s.default_parallelism))
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(s.log_level)

    logger.info("SparkSession created: Spark %s", spark.version)
    return spark


def get_kafka_options(topic: str | None = None) -> dict[str, str]:
    """
    Trả về Kafka read options cho spark.readStream.format("kafka").

    Args:
        topic: Override Kafka topic. None → dùng kafka.topics.binance_trade_ticks từ config.

    Returns:
        Dict options để truyền vào .options(**opts).
    """
    from dataops_processing.config.settings import get_settings
    cfg = get_settings()
    k   = cfg.kafka

    resolved_topic = topic or k.topic_trade_ticks

    return {
        "kafka.bootstrap.servers":   k.bootstrap_servers,
        "subscribe":                 resolved_topic,
        "startingOffsets":           k.starting_offsets,
        "maxOffsetsPerTrigger":      str(k.max_offsets_per_trigger),
        "kafka.request.timeout.ms":  str(k.request_timeout_ms),
        "kafka.session.timeout.ms":  str(k.session_timeout_ms),
        "failOnDataLoss":            str(k.fail_on_data_loss).lower(),
    }
