"""
metadata.py — Config Loader Helpers cho Pipeline Flows.

Cung cấp các helper function đọc và trả về config sections từ okx_config.yaml.
Flows (bronze, silver, backfill) gọi vào đây — không đọc YAML trực tiếp.

Usage:
    from cli.flows.serving.metadata import get_okx_cfg, get_spark_cfg, setup_logging
    okx = get_okx_cfg()
    symbols = okx["instruments"]["spot"]
"""
from __future__ import annotations

import logging
import logging.config
import os
from pathlib import Path
from typing import Any, Dict

# config.py phải được import từ root package
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from config import loader, Paths


def get_okx_cfg() -> Dict[str, Any]:
    """Trả về toàn bộ section 'okx' từ okx_config.yaml."""
    return loader.pipeline_config().get("okx", {})


def get_vps_cfg() -> Dict[str, Any]:
    """Trả về section 'vps' từ okx_config.yaml."""
    return loader.pipeline_config().get("vps", {})


def get_delta_cfg() -> Dict[str, Any]:
    """Trả về section 'delta' (paths + compact config) từ okx_config.yaml."""
    return loader.pipeline_config().get("delta", {})


def get_spark_streaming_cfg() -> Dict[str, Any]:
    """Trả về section 'spark_streaming' (topics, window params, thresholds)."""
    return loader.pipeline_config().get("spark_streaming", {})


def get_backfill_cfg() -> Dict[str, Any]:
    """Trả về section 'backfill' từ okx_config.yaml."""
    return loader.pipeline_config().get("backfill", {})


def get_kafka_brokers() -> str:
    """
    Đọc Kafka bootstrap servers từ env var trước, fallback sang config.yaml.
    Env var KAFKA_BROKERS override config file để dễ deploy trên Docker/K8s.
    """
    env = os.getenv("KAFKA_BROKERS")
    if env:
        return env
    return loader.pipeline_config().get("kafka", {}).get("bootstrap_servers", "localhost:9092")


def setup_logging() -> None:
    """
    Cấu hình logging từ log/config/logger_config.yaml.
    Gọi 1 lần tại entrypoint (main.py hoặc daily_pipeline.py) trước khi dùng logger.

    Tự động tạo log/log_storage/ nếu chưa tồn tại.
    """
    Paths.LOG_STORAGE.mkdir(parents=True, exist_ok=True)
    try:
        cfg = loader.logger_config()
        logging.config.dictConfig(cfg)
    except Exception as e:
        # Fallback sang basicConfig nếu logger_config.yaml có lỗi
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        )
        logging.getLogger(__name__).warning(f"Logger config load failed, using basicConfig: {e}")
