"""
spark_session.py — SparkSession Factory cho OKX Financial Stream Processor.

Tách biệt việc khởi tạo SparkSession khỏi business logic.
Đọc cấu hình từ platforms/config/stack.yaml qua config.loader.stack_config().

Usage:
    from platforms.processing.spark_stack.spark_session import create_spark_session
    spark = create_spark_session()
"""
from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = None) -> SparkSession:
    """
    Khởi tạo SparkSession với đầy đủ connector:
        - Delta Lake (io.delta)
        - Kafka source/sink (spark-sql-kafka)
        - S3A / MinIO (hadoop-aws)

    Config được đọc từ biến môi trường (override stack.yaml defaults):
        SPARK_MASTER       — mặc định "local[*]"
        MINIO_ENDPOINT     — mặc định "http://localhost:9000"
        MINIO_ACCESS_KEY   — mặc định "minioadmin"
        MINIO_SECRET_KEY   — mặc định "minioadmin"

    Args:
        app_name: Tên Spark application. Mặc định "OKX-Financial-Stream-Processor".

    Returns:
        SparkSession đã configure đầy đủ.
    """
    # Import ở đây để tránh circular import nếu module dùng ở test context
    try:
        from config import loader
        stack = loader.stack_config()
    except FileNotFoundError as exc:
        # stack.yaml không tồn tại → dùng env vars hoàn toàn (OK cho container deploy)
        logger.warning(
            "stack.yaml not found, falling back to env vars only: %s", exc,
        )
        stack = {}
    except Exception as exc:
        logger.error(
            "Unexpected error loading stack.yaml: %s", exc, exc_info=True,
        )
        stack = {}

    _app_name    = app_name or stack.get("spark", {}).get("app_name", "OKX-Financial-Stream-Processor")
    _master      = os.getenv("SPARK_MASTER", stack.get("spark", {}).get("master", "local[*]"))
    _minio_url   = os.getenv("MINIO_ENDPOINT", stack.get("minio", {}).get("endpoint", "http://localhost:9000"))
    _minio_key   = os.getenv("MINIO_ACCESS_KEY", stack.get("minio", {}).get("access_key", "minioadmin"))
    _minio_sec   = os.getenv("MINIO_SECRET_KEY", stack.get("minio", {}).get("secret_key", "minioadmin"))

    return (
        SparkSession.builder
        .appName(_app_name)
        .master(_master)
        # ── Delta Lake extensions ──────────────────────────────────────────────
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ── Streaming behavior ─────────────────────────────────────────────────
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        # ── MinIO / S3A ────────────────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint",              _minio_url)
        .config("spark.hadoop.fs.s3a.access.key",            _minio_key)
        .config("spark.hadoop.fs.s3a.secret.key",            _minio_sec)
        .config("spark.hadoop.fs.s3a.path.style.access",     "true")
        .config("spark.hadoop.fs.s3a.impl",                  "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
