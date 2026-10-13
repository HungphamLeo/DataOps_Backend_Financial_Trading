"""
core/orchestrator.py — Orchestrator & Facade Pattern cho dataops_processing.

Nhiệm vụ:
    - Là Class Tổng duy nhất tiếp nhận config (ProcessingSettings) và paths (StoragePathResolver).
    - Quản lý vòng đời khởi tạo, điều phối và thực thi các sub-job / indicator.
    - Đảm bảo discovery toàn bộ jobs đã đăng ký trong package.
"""
from __future__ import annotations

import importlib
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from dataops_processing.config.paths import StoragePathResolver, get_path_resolver
from dataops_processing.config.settings import ProcessingSettings, get_settings
from dataops_processing.core.base_job import BaseProcessingJob
from dataops_processing.core.registry import JobRegistry
from dataops_processing.log.logger_setup import logger_manager

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logger_manager.get_logger(__name__)


class ProcessingOrchestrator:
    """
    Class Tổng chịu trách nhiệm nạp cấu hình tập trung và điều phối toàn bộ job tính toán.
    """

    def __init__(
        self,
        settings: Optional[ProcessingSettings] = None,
        paths: Optional[StoragePathResolver] = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.paths = paths or get_path_resolver()
        self._ensure_jobs_loaded()

    def _ensure_jobs_loaded(self) -> None:
        """Tự động import các job modules để kích hoạt @JobRegistry.register."""
        known_modules = [
            "dataops_processing.jobs.vwap_job",
            "dataops_processing.jobs.order_flow_job",
            "dataops_processing.jobs.market_breadth_job",
            "dataops_processing.jobs.volume_bars_job",
            "dataops_processing.jobs.indicator_matrix_job",
        ]
        for mod_name in known_modules:
            try:
                importlib.import_module(mod_name)
            except ImportError as exc:
                logger.warning("Could not auto-import %s: %s", mod_name, exc)

    def list_available_jobs(self) -> List[str]:
        """Trả về danh sách tên tất cả các job đã đăng ký trong hệ thống."""
        return JobRegistry.list_jobs()

    def get_job(self, job_name: str) -> BaseProcessingJob:
        """Khởi tạo một instance của job với settings và paths được inject."""
        return JobRegistry.create(
            name=job_name,
            settings=self.settings,
            paths=self.paths,
        )

    def run_job(
        self,
        job_name: str,
        spark: Optional["SparkSession"] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Khởi tạo và thực thi một job theo tên định danh.

        Args:
            job_name: Tên job đăng ký (e.g. 'vwap', 'volume-bars').
            spark: SparkSession tùy chọn.
            **kwargs: Tham số truyền vào job (e.g. symbol='BTCUSDT').

        Returns:
            Kết quả từ job.
        """
        logger.info("ProcessingOrchestrator: dispatching job '%s'", job_name)
        job = self.get_job(job_name)
        return job.run(spark=spark, **kwargs)


@lru_cache(maxsize=1)
def get_orchestrator() -> ProcessingOrchestrator:
    """Trả về singleton ProcessingOrchestrator."""
    return ProcessingOrchestrator()
