"""
metadata.py — Config Loader Helpers cho Pipeline Flows.

Cung cấp các helper function đọc và trả về config sections từ exchange_config.yaml.
Flows (bronze, silver, backfill) gọi vào đây — không đọc YAML trực tiếp.

Usage:
    from cli.flows.serving.metadata import get_binance_cfg, get_spark_cfg, setup_logging
    binance = get_binance_cfg()
    symbols = binance["instruments"]["spot"]
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

# config.py phải được import từ root package
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from config import loader


def get_binance_cfg() -> Dict[str, Any]:
    """Trả về toàn bộ section 'binance' từ exchange_config.yaml."""
    return loader.pipeline_config().get("binance", {})


def get_tcbs_cfg() -> Dict[str, Any]:
    """Trả về section 'tcbs' từ exchange_config.yaml."""
    return loader.pipeline_config().get("tcbs", {})


def get_delta_cfg() -> Dict[str, Any]:
    """Trả về section 'delta' (paths + compact config) từ exchange_config.yaml."""
    return loader.pipeline_config().get("delta", {})


def get_spark_streaming_cfg() -> Dict[str, Any]:
    """Trả về section 'spark_streaming' (topics, window params, thresholds)."""
    return loader.pipeline_config().get("spark_streaming", {})


def get_backfill_cfg() -> Dict[str, Any]:
    """Trả về section 'backfill' từ exchange_config.yaml."""
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
    Khởi tạo logging cho toàn bộ dataops_batch_api pipeline.

    Delegate hoàn toàn sang log.logger_setup.setup_pipeline_logging() —
    đây là single source of truth cho logging configuration.

    Gọi 1 lần tại entrypoint (daily_pipeline.py, prefect_main.py,
    hoặc backfill_flow.py khi chạy __main__) trước khi dùng logger.
    """
    from log.logger_setup import setup_pipeline_logging
    setup_pipeline_logging()
