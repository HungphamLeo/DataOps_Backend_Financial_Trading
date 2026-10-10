"""
spark_config.py — SparkSession builder cho dataops_processing.

Cung cấp SparkSession đã được cấu hình sẵn với:
  - Kafka source  (spark-sql-kafka)
  - Delta Lake    (delta-spark 3.1.0)
  - S3A / MinIO   (hadoop-aws + aws-java-sdk-bundle)

Env vars:
  KAFKA_URL             default: localhost:9092
  MINIO_ENDPOINT        default: http://minio:9010
  MINIO_ACCESS_KEY      default: minioadmin
  MINIO_SECRET_KEY      default: minioadmin
  SPARK_APP_NAME        default: dataops-processing
  SPARK_MASTER          default: local[*]

Sử dụng:
    from dataops_processing.config.spark_config import build_spark_session
    spark = build_spark_session("vwap-job")
"""
from __future__ import annotations

import os
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Package versions — phải khớp với requirements.txt
# Spark packages được download tự động qua spark.jars.packages khi submit.
# ---------------------------------------------------------------------------
_DELTA_VERSION = "3.1.0"
_SCALA_VERSION = "2.12"
_SPARK_VERSION = "3.5"

# Maven coordinates cho Spark packages
_SPARK_PACKAGES = ",".join([
    # Kafka source/sink cho Spark Structured Streaming
    f"org.apache.spark:spark-sql-kafka-0-10_{_SCALA_VERSION}:3.5.1",
    # Delta Lake connector cho Spark 3.5
    f"io.delta:delta-spark_{_SCALA_VERSION}:{_DELTA_VERSION}",
    # S3A connector (Hadoop AWS) — dùng cho MinIO
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])


def build_spark_session(app_name: str | None = None):
    """
    Tạo SparkSession đã configured với Kafka + Delta + S3A.

    Args:
        app_name: Tên job, hiển thị trên Spark UI. Default từ env SPARK_APP_NAME.

    Returns:
        pyspark.sql.SparkSession — ready to use.

    Note:
        Hàm này lazy-import pyspark để test unit có thể mock mà không cần
        cài Spark thật (tương tự lazy import deltalake trong _delta_writer.py).
    """
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "PySpark hoặc delta-spark chưa được cài. "
            "Chạy: pip install pyspark==3.5.1 delta-spark==3.1.0"
        ) from exc

    name = app_name or os.environ.get("SPARK_APP_NAME", "dataops-processing")
    master = os.environ.get("SPARK_MASTER", "local[*]")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9010")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    logger.info("Building SparkSession: app=%s master=%s minio=%s", name, master, minio_endpoint)

    builder = (
        SparkSession.builder
        .appName(name)
        .master(master)
        # ── Delta Lake ──────────────────────────────────────────────────────
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # ── Spark packages (tải lúc submit) ────────────────────────────────
        .config("spark.jars.packages", _SPARK_PACKAGES)
        # ── S3A / MinIO ─────────────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")   # MinIO bắt buộc
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ── Streaming tuning ────────────────────────────────────────────────
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # ── Shuffle / memory (local mode) ──────────────────────────────────
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "4")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("SparkSession created: %s", spark.version)
    return spark


def get_kafka_options(topic: str) -> dict[str, str]:
    """
    Trả về Kafka read options cho spark.readStream.format("kafka").

    Args:
        topic: Kafka topic name (single topic, không dùng regex ở đây).

    Returns:
        Dict options để truyền vào .options(**opts).
    """
    kafka_url = os.environ.get("KAFKA_URL", "localhost:9092")
    return {
        "kafka.bootstrap.servers": kafka_url,
        "subscribe":               topic,
        "startingOffsets":         "latest",
        # Giới hạn số records mỗi micro-batch — tránh OOM khi lag lớn
        "maxOffsetsPerTrigger":    "10000",
        # Fail nhanh nếu Kafka unreachable thay vì retry vô hạn
        "kafka.request.timeout.ms":  "10000",
        "kafka.session.timeout.ms":  "30000",
        "failOnDataLoss":            "false",  # tránh job crash khi topic bị compact
    }
