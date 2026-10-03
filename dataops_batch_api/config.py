"""
config.py — Single Source of Truth cho dataops_batch_api.

Tất cả đường dẫn, tham số cố định, và YAML loader được tập trung tại đây.
Business logic KHÔNG được hardcode path hay env-key — đọc qua module này.

12-Factor App alignment:
    - Factor III (Config): mọi config đọc từ env vars, không hardcode trong code.
    - Factor XI (Logs): logger được init từ đây, không dùng print() hay logging.basicConfig() rải rác.

Usage:
    from config import Paths, loader, StorageOptions
    cfg = loader.pipeline_config()
    delta_uri = Paths.delta_bronze_binance("SPOT")
    opts = StorageOptions.minio()
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import yaml

_log = logging.getLogger(__name__)

# ─── Root Paths ───────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent.resolve()
CLI_DIR    = BASE_DIR / "cli"
FLOWS_DIR  = CLI_DIR / "flows"
LOG_DIR    = BASE_DIR / "log" / "log_storage"
CONFIG_DIR = FLOWS_DIR / "config"


class Paths:
    """Tập trung tất cả đường dẫn runtime của pipeline."""

    # ── Config files ──────────────────────────────────────────────────────────
    BINANCE_CONFIG_YAML = CONFIG_DIR / "exchange_config.yaml"
    STACK_CONFIG_YAML   = BASE_DIR / "platforms" / "config" / "stack.yaml"
    LOGGER_CONFIG_YAML  = BASE_DIR / "log" / "config" / "logger_config.yaml"

    # ── Log storage ───────────────────────────────────────────────────────────
    LOG_STORAGE = LOG_DIR

    # ── Delta Lake paths (MinIO S3) ───────────────────────────────────────────
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", "binance-datalake")

    @classmethod
    def delta_bronze_binance(cls, inst_type: str = "SPOT") -> str:
        """s3://binance-datalake/delta/bronze/binance_candles_spot"""
        return f"s3://{cls.MINIO_BUCKET}/delta/bronze/binance_candles_{inst_type.lower()}"

    @classmethod
    def delta_bronze_tcbs_market(cls) -> str:
        """s3://binance-datalake/delta/bronze/tcbs_market — full ticker snapshot"""
        return f"s3://{cls.MINIO_BUCKET}/delta/bronze/tcbs_market"

    @classmethod
    def delta_bronze_tcbs_bsa(cls) -> str:
        """s3://binance-datalake/delta/bronze/tcbs_bsa — Buy/Sell Analysis"""
        return f"s3://{cls.MINIO_BUCKET}/delta/bronze/tcbs_bsa"

    @classmethod
    def delta_bronze_tcbs_intraday(cls) -> str:
        """s3://binance-datalake/delta/bronze/tcbs_intraday — intraday tick history"""
        return f"s3://{cls.MINIO_BUCKET}/delta/bronze/tcbs_intraday"

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


class _YAMLLoader:
    """
    Đọc và cache YAML config files.

    Cache dùng str(path) làm key để tránh reload nhiều lần trong cùng process.
    Thread-safety: dict read/write trong CPython là atomic — đủ cho pipeline use.

    Design: KHÔNG silently return {} khi file không tồn tại — lỗi cấu hình
    phải được raise sớm (fail fast), không để dùng wrong defaults âm thầm.
    """

    _cache: dict = {}

    def _load(self, path: Path) -> dict:
        """
        Load YAML file và cache kết quả.

        Args:
            path: đường dẫn tuyệt đối đến YAML file.

        Returns:
            dict nội dung YAML.

        Raises:
            FileNotFoundError: file không tồn tại — lỗi cấu hình cần fix ngay.
            yaml.YAMLError: file malformed — cần fix ngay, không fallback.
        """
        key = str(path)
        if key not in self._cache:
            if not path.exists():
                _log.error("Config file not found: %s", path)
                raise FileNotFoundError(f"Required config file missing: {path}")
            try:
                with open(path, "r", encoding="utf-8") as f:
                    content = yaml.safe_load(f) or {}
                self._cache[key] = content
                _log.debug("Loaded config: %s (%d top-level keys)", path, len(content))
            except yaml.YAMLError as exc:
                _log.error("Malformed YAML in %s: %s", path, exc)
                raise
        return self._cache[key]

    def pipeline_config(self) -> dict:
        """Đọc exchange_config.yaml — pipeline params, instruments, topics, paths."""
        return self._load(Paths.BINANCE_CONFIG_YAML)

    def stack_config(self) -> dict:
        """Đọc stack.yaml — Spark + MinIO platform config."""
        return self._load(Paths.STACK_CONFIG_YAML)

    def logger_config(self) -> dict:
        """Đọc logger_config.yaml — logging level, handlers, formatters."""
        return self._load(Paths.LOGGER_CONFIG_YAML)


# Singleton loader — import trực tiếp trong toàn bộ codebase
loader = _YAMLLoader()


class StorageOptions:
    """
    MinIO / S3-compatible credentials cho delta-rs và PySpark.

    Giá trị đọc từ env vars (12-Factor Factor III).
    Các static flags (allow_http, allow_unsafe_rename, aws_region) đọc từ
    exchange_config.yaml section 'storage' để có thể override per-environment.
    """

    @staticmethod
    def minio() -> dict:
        """
        Build storage options dict cho MinIO.

        Thứ tự ưu tiên (cao → thấp):
            1. Env vars (MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, AWS_REGION)
            2. exchange_config.yaml section 'storage'
            3. Hardcoded safe defaults (chỉ dùng cho local dev)

        Returns:
            dict với keys AWS_* tương thích với delta-rs và hadoop-aws.
        """
        try:
            storage_cfg = loader.pipeline_config().get("storage", {})
        except Exception as exc:
            _log.warning("Could not load storage config from yaml, using env/defaults: %s", exc)
            storage_cfg = {}

        return {
            "AWS_ENDPOINT_URL":           os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            "AWS_ACCESS_KEY_ID":          os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            "AWS_SECRET_ACCESS_KEY":      os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            "AWS_REGION":                 os.getenv("AWS_REGION", storage_cfg.get("aws_region", "us-east-1")),
            "AWS_ALLOW_HTTP":             storage_cfg.get("allow_http", "true"),
            "AWS_S3_ALLOW_UNSAFE_RENAME": storage_cfg.get("allow_unsafe_rename", "true"),
        }
