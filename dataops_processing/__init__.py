"""
dataops_processing — Spark Streaming / Batch Processing Layer.

Tầng xử lý dữ liệu trung gian giữa Kafka (raw events) và storage (InfluxDB + Delta Silver).
Trách nhiệm duy nhất: consume từ Kafka → aggregate / transform → ghi vào InfluxDB + Delta Lake.

Ranh giới:
  - INPUT:  Kafka topics (binance.trade.ticks, binance.funding.rates, ...)
  - OUTPUT: InfluxDB (real-time metrics) + Delta Silver (s3://binance-datalake/delta/silver/)
  - KHÔNG đọc từ REST API — đó là việc của dataops_batch_api
  - KHÔNG serve HTTP — đó là việc của dataops_backend_api

Chạy:
  python -m dataops_processing vwap
  python -m dataops_processing volume-bars --symbol BTCUSDT
"""
from dataops_processing.config.paths import StoragePathResolver, get_path_resolver
from dataops_processing.config.settings import ProcessingSettings, get_settings
from dataops_processing.core.base_job import BaseProcessingJob
from dataops_processing.core.orchestrator import ProcessingOrchestrator, get_orchestrator
from dataops_processing.core.registry import JobRegistry

__all__ = [
    "ProcessingSettings",
    "get_settings",
    "StoragePathResolver",
    "get_path_resolver",
    "ProcessingOrchestrator",
    "get_orchestrator",
    "BaseProcessingJob",
    "JobRegistry",
]
