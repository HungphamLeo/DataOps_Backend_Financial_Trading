"""
core/base_job.py — Abstract Base Class cho tất cả các Job trong dataops_processing.

Áp dụng Strategy Pattern & Template Method:
    - Mỗi chỉ báo (Indicator) hoặc bài toán xử lý dữ liệu (VWAP, Volume Bars, Funding Anomaly...)
      là một Concrete Strategy kế thừa từ BaseProcessingJob.
    - Nhận config và paths qua Dependency Injection từ ProcessingOrchestrator.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, Optional

from dataops_processing.log.logger_setup import logger_manager

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from dataops_processing.config.paths import StoragePathResolver
    from dataops_processing.config.settings import ProcessingSettings

logger = logger_manager.get_logger(__name__)


class BaseProcessingJob(ABC):
    """
    Abstract base class định nghĩa contract cho mọi job xử lý dữ liệu (batch & streaming).
    """

    def __init__(
        self,
        settings: "ProcessingSettings",
        paths: "StoragePathResolver",
    ) -> None:
        self.settings = settings
        self.paths = paths

    @property
    @abstractmethod
    def name(self) -> str:
        """Tên định danh duy nhất của job (dùng trong CLI dispatch và registry)."""
        pass

    @property
    @abstractmethod
    def job_type(self) -> str:
        """Loại job: 'streaming' hoặc 'batch'."""
        pass

    @property
    def description(self) -> str:
        """Mô tả vắn tắt mục đích của job."""
        return self.__doc__ or "Processing job"

    def validate_params(self, **kwargs: Any) -> None:
        """
        Hook kiểm tra tính hợp lệ của tham số trước khi thực thi.
        Các subclass có thể override để custom validation.
        """
        pass

    @abstractmethod
    def execute(self, spark: Optional["SparkSession"] = None, **kwargs: Any) -> Any:
        """
        Thực thi logic tính toán chính của job.

        Args:
            spark: SparkSession tùy chọn (nếu None, job sẽ tự khởi tạo qua build_spark_session).
            **kwargs: Tham số bổ sung truyền vào từ CLI / orchestrator.

        Returns:
            Kết quả tính toán (dict với batch job, hoặc streaming query/None với streaming job).
        """
        pass

    def run(self, spark: Optional["SparkSession"] = None, **kwargs: Any) -> Any:
        """
        Template method điều khiển vòng đời chạy job:
        1. Validate tham số
        2. Ghi nhận log khởi động
        3. Thực thi execute()
        """
        self.validate_params(**kwargs)
        logger.info("Executing job '%s' (type=%s)", self.name, self.job_type)
        return self.execute(spark=spark, **kwargs)
