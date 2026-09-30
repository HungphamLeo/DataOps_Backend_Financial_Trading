"""
config.py — Single Source of Truth cho dataops_batch_api.

Tất cả đường dẫn, tham số cố định, và YAML loader được tập trung tại đây.
Business logic KHÔNG được hardcode path hay env-key — đọc qua module này.

Usage:
    from config import Paths, loader
    cfg = loader.pipeline_config()
    delta_uri = Paths.delta_bronze_okx("SPOT")
"""
from __future__ import annotations

import os
from pathlib import Path
import yaml

# ─── Root Paths ───────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent.resolve()
CLI_DIR    = BASE_DIR / "cli"
FLOWS_DIR  = CLI_DIR / "flows"
LOG_DIR    = BASE_DIR / "log" / "log_storage"
CONFIG_DIR = FLOWS_DIR / "config"


class Paths:
    """Tập trung tất cả đường dẫn runtime của pipeline."""

    # ── Config files ──────────────────────────────────────────────────────────
    OKX_CONFIG_YAML   = CONFIG_DIR / "okx_config.yaml"
    STACK_CONFIG_YAML = BASE_DIR / "platforms" / "config" / "stack.yaml"
    LOGGER_CONFIG_YAML = BASE_DIR / "log" / "config" / "logger_config.yaml"

    # ── Log storage ───────────────────────────────────────────────────────────
    LOG_STORAGE = LOG_DIR

    # ── Delta Lake paths (MinIO S3) ───────────────────────────────────────────
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", "okx-datalake")

    @classmethod
    def delta_bronze_okx(cls, inst_type: str = "SPOT") -> str:
        """s3://okx-datalake/delta/bronze/okx_candles_spot"""
        return f"s3://{cls.MINIO_BUCKET}/delta/bronze/okx_candles_{inst_type.lower()}"

    @classmethod
    def delta_bronze_vps(cls) -> str:
        return f"s3://{cls.MINIO_BUCKET}/delta/bronze/vps_market"

    @classmethod
    def delta_silver_vwap(cls) -> str:
        return f"s3://{cls.MINIO_BUCKET}/delta/silver/vwap"

    @classmethod
    def delta_silver_funding_stats(cls) -> str:
        return f"s3://{cls.MINIO_BUCKET}/delta/silver/funding_stats"

    # ── Spark checkpoint locations ─────────────────────────────────────────────
    @classmethod
    def spark_checkpoint(cls, job: str) -> str:
        """Ví dụ: job='vwap' → /tmp/spark_checkpoints/vwap"""
        return f"/tmp/spark_checkpoints/{job}"


class StorageOptions:
    """MinIO / S3-compatible credentials cho delta-rs và PySpark."""

    @staticmethod
    def minio() -> dict:
        return {
            "AWS_ENDPOINT_URL":           os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            "AWS_ACCESS_KEY_ID":          os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            "AWS_SECRET_ACCESS_KEY":      os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            "AWS_REGION":                 "us-east-1",
            "AWS_ALLOW_HTTP":             "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }


class _YAMLLoader:
    """Đọc và cache YAML config files."""

    _cache: dict = {}

    def _load(self, path: Path) -> dict:
        if str(path) not in self._cache:
            with open(path, "r", encoding="utf-8") as f:
                self._cache[str(path)] = yaml.safe_load(f) or {}
        return self._cache[str(path)]

    def pipeline_config(self) -> dict:
        return self._load(Paths.OKX_CONFIG_YAML)

    def stack_config(self) -> dict:
        return self._load(Paths.STACK_CONFIG_YAML)

    def logger_config(self) -> dict:
        return self._load(Paths.LOGGER_CONFIG_YAML)


loader = _YAMLLoader()
