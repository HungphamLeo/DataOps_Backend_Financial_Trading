"""
tests/test_core_orchestrator.py — Unit tests cho Architecture Engine:
  1. StoragePathResolver (paths.py)
  2. JobRegistry (registry.py)
  3. ProcessingOrchestrator (orchestrator.py)
  4. BaseProcessingJob (base_job.py)
"""
import unittest
from unittest.mock import MagicMock, patch

from dataops_processing.config.paths import StoragePathResolver
from dataops_processing.config.settings import ProcessingSettings, get_settings
from dataops_processing.core.base_job import BaseProcessingJob
from dataops_processing.core.orchestrator import ProcessingOrchestrator
from dataops_processing.core.registry import JobRegistry


class TestStoragePathResolver(unittest.TestCase):
    """Kiểm tra StoragePathResolver sinh đúng đường dẫn chuẩn hóa."""

    def setUp(self):
        self.settings = get_settings()
        self.resolver = StoragePathResolver(self.settings)

    def test_delta_bronze_path(self):
        path = self.resolver.delta_bronze_path("binance_candles_1d")
        self.assertEqual(path, f"s3a://{self.settings.delta.bucket}/delta/bronze/binance_candles_1d")

    def test_delta_silver_path(self):
        path = self.resolver.delta_silver_path("vwap_1m")
        self.assertEqual(path, f"s3a://{self.settings.delta.bucket}/delta/silver/vwap_1m")

    def test_delta_gold_path(self):
        path = self.resolver.delta_gold_path("features_daily")
        self.assertEqual(path, f"s3a://{self.settings.delta.bucket}/delta/gold/features_daily")

    def test_build_delta_path(self):
        path = self.resolver.build_delta_path(
            layer="silver",
            domain="tcbs",
            entity="market_snapshot",
            interval="1m",
        )
        self.assertEqual(path, f"s3a://{self.settings.delta.bucket}/delta/silver/tcbs_market_snapshot_1m")

    def test_checkpoint_path(self):
        chk = self.resolver.checkpoint_path("vwap_job", "influxdb")
        self.assertTrue(chk.endswith("vwap_job/influxdb"))


class DummyTestJob(BaseProcessingJob):
    """Mock job dùng cho unit test registry & orchestrator."""

    @property
    def name(self) -> str:
        return "dummy-test-job"

    @property
    def job_type(self) -> str:
        return "batch"

    def execute(self, spark=None, **kwargs):
        return {"status": "success", "received_kwargs": kwargs}


class TestJobRegistryAndOrchestrator(unittest.TestCase):
    """Kiểm tra Registry và Orchestrator pattern."""

    def setUp(self):
        self.settings = get_settings()
        self.paths = StoragePathResolver(self.settings)

    def test_register_and_create_job(self):
        JobRegistry.register("dummy-test-job")(DummyTestJob)

        self.assertIn("dummy-test-job", JobRegistry.list_jobs())
        job_cls = JobRegistry.get("dummy-test-job")
        self.assertEqual(job_cls, DummyTestJob)

        instance = JobRegistry.create("dummy-test-job", self.settings, self.paths)
        self.assertIsInstance(instance, DummyTestJob)
        self.assertEqual(instance.name, "dummy-test-job")
        self.assertEqual(instance.job_type, "batch")

    def test_get_non_existent_job_raises_key_error(self):
        with self.assertRaises(KeyError):
            JobRegistry.get("non-existent-job-xyz")

    def test_orchestrator_dispatches_job(self):
        JobRegistry.register("dummy-test-job")(DummyTestJob)
        orchestrator = ProcessingOrchestrator(settings=self.settings, paths=self.paths)

        result = orchestrator.run_job("dummy-test-job", custom_param=123)
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["received_kwargs"]["custom_param"], 123)

    def test_registered_concrete_jobs_exist(self):
        orchestrator = ProcessingOrchestrator(settings=self.settings, paths=self.paths)
        available = orchestrator.list_available_jobs()
        self.assertIn("vwap", available)
        self.assertIn("volume-bars", available)


if __name__ == "__main__":
    unittest.main()
