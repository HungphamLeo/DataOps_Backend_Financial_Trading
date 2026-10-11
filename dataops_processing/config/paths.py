"""
paths.py — StoragePathResolver cho dataops_processing.

Nhiệm vụ:
    - Tập trung hoá toàn bộ quy tắc sinh URL/Path cho Delta Lake (S3A), Checkpoint, Kafka Topics, InfluxDB.
    - Triệt tiêu việc hardcode string URL ("s3a://...", "/tmp/...", topic names) rải rác trong code.
    - Tương thích 100% với ProcessingSettings từ settings.py.

Usage:
    from dataops_processing.config.paths import StoragePathResolver, get_path_resolver

    paths = get_path_resolver()
    silver_vwap = paths.delta_silver_path("vwap_1m")
    bronze_candles = paths.delta_bronze_path("binance_candles_1d")
    chk_dir = paths.checkpoint_path("vwap_job", "influxdb")
"""
from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from dataops_processing.config.settings import ProcessingSettings


class StoragePathResolver:
    """
    Trừu tượng hoá và chuẩn hoá toàn bộ các đường dẫn lưu trữ trong DataOps processing.
    """

    def __init__(self, settings: "ProcessingSettings") -> None:
        self._settings = settings

    @property
    def bucket(self) -> str:
        return self._settings.delta.bucket

    @property
    def silver_base(self) -> str:
        return self._settings.delta.silver_base

    # ── Delta Lake Paths ──────────────────────────────────────────────────────

    def delta_bronze_path(self, table_name: str) -> str:
        """
        Sinh S3A URI cho Delta Bronze table.
        Ví dụ: s3a://binance-datalake/delta/bronze/binance_candles_1d
        """
        return f"s3a://{self.bucket}/delta/bronze/{table_name}"

    def delta_silver_path(self, table_key_or_name: str) -> str:
        """
        Sinh S3A URI cho Delta Silver table dựa trên config map hoặc table name.
        Ví dụ: s3a://binance-datalake/delta/silver/vwap_1m
        """
        resolved_name = self._settings.delta.tables.get(table_key_or_name, table_key_or_name)
        return f"s3a://{self.bucket}/{self.silver_base}/{resolved_name}"

    def delta_gold_path(self, table_name: str) -> str:
        """
        Sinh S3A URI cho Delta Gold table.
        Ví dụ: s3a://binance-datalake/delta/gold/feature_store
        """
        return f"s3a://{self.bucket}/delta/gold/{table_name}"

    def build_delta_path(
        self,
        layer: str,
        domain: str,
        entity: str,
        interval: Optional[str] = None,
    ) -> str:
        """
        Sinh đường dẫn Delta chuẩn hóa theo quy ước:
        s3a://{bucket}/delta/{layer}/{domain}_{entity}_{interval}
        """
        layer_clean = layer.strip("/").lower()
        table_name = f"{domain}_{entity}"
        if interval:
            table_name = f"{table_name}_{interval}"
        return f"s3a://{self.bucket}/delta/{layer_clean}/{table_name}"

    # ── Spark Checkpoint Paths ────────────────────────────────────────────────

    def checkpoint_path(self, job_name: str, sink_name: Optional[str] = None) -> str:
        """
        Sinh checkpoint directory cho streaming job.
        Ví dụ: /tmp/spark-checkpoints/vwap_job/influxdb
        """
        base = self._settings.spark.checkpoint_dir(job_name)
        if sink_name:
            return f"{base}/{sink_name}"
        return base

    # ── Kafka Topic Names ─────────────────────────────────────────────────────

    def kafka_trade_ticks_topic(self) -> str:
        return self._settings.kafka.topic_trade_ticks

    # ── InfluxDB Measurement Names ────────────────────────────────────────────

    def influx_vwap_measurement(self) -> str:
        return self._settings.vwap_job.influx_measurement


@lru_cache(maxsize=1)
def get_path_resolver() -> StoragePathResolver:
    """Trả về StoragePathResolver singleton kết nối với ProcessingSettings."""
    from dataops_processing.config.settings import get_settings
    return StoragePathResolver(get_settings())
