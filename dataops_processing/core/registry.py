"""
core/registry.py — Registry & Factory Pattern cho dataops_processing.

Nhiệm vụ:
    - Quản lý danh mục các Concrete Processing Jobs / Indicators.
    - Cung cấp decorator @register để tự động đăng ký job mới mà không sửa core code.
    - Factory tạo instance job với đầy đủ settings & paths (Dependency Injection).
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict, List, Type, TypeVar

from dataops_processing.core.base_job import BaseProcessingJob
from dataops_processing.log.logger_setup import logger_manager

if TYPE_CHECKING:
    from dataops_processing.config.paths import StoragePathResolver
    from dataops_processing.config.settings import ProcessingSettings

logger = logger_manager.get_logger(__name__)

T = TypeVar("T", bound=BaseProcessingJob)


class JobRegistry:
    """
    Central Registry lưu trữ và khởi tạo các Data Processing Jobs.
    """

    _registry: Dict[str, Type[BaseProcessingJob]] = {}

    @classmethod
    def register(cls, name: str) -> Callable[[Type[T]], Type[T]]:
        """
        Decorator dùng để đăng ký một class job vào registry.

        Usage:
            @JobRegistry.register("vwap")
            class VWAPStreamingJob(BaseProcessingJob):
                ...
        """
        def decorator(subclass: Type[T]) -> Type[T]:
            if not issubclass(subclass, BaseProcessingJob):
                raise TypeError(f"Class {subclass.__name__} must inherit from BaseProcessingJob")
            if name in cls._registry:
                logger.warning("Overwriting existing job '%s' in registry with %s", name, subclass.__name__)
            cls._registry[name] = subclass
            logger.debug("Registered job '%s' -> %s", name, subclass.__name__)
            return subclass
        return decorator

    @classmethod
    def get(cls, name: str) -> Type[BaseProcessingJob]:
        """Lấy class của job theo tên đăng ký."""
        job_cls = cls._registry.get(name)
        if not job_cls:
            raise KeyError(
                f"Job '{name}' not found in registry. "
                f"Available jobs: {list(cls._registry.keys())}"
            )
        return job_cls

    @classmethod
    def create(
        cls,
        name: str,
        settings: "ProcessingSettings",
        paths: "StoragePathResolver",
    ) -> BaseProcessingJob:
        """Factory method: khởi tạo instance của job với DI settings & paths."""
        job_cls = cls.get(name)
        return job_cls(settings=settings, paths=paths)

    @classmethod
    def list_jobs(cls) -> List[str]:
        """Danh sách tên các job đã đăng ký."""
        return list(cls._registry.keys())

    @classmethod
    def clear(cls) -> None:
        """Xóa registry (dùng cho isolated unit tests)."""
        cls._registry.clear()
