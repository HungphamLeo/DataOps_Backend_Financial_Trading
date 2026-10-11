"""
settings.py — Centralized configuration loader cho dataops_processing.

Thiết kế:
    1. Load processing_config.yaml một lần duy nhất (singleton).
    2. Env vars OVERRIDE giá trị YAML (12-Factor App pattern).
    3. Tất cả jobs/modules chỉ import từ đây — không dùng os.environ trực tiếp.
    4. Trả về typed dataclass thay vì raw dict — IDE-friendly, fail-fast khi missing key.

Thứ tự ưu tiên:
    env var > processing_config.yaml > built-in default (dùng khi YAML missing key)

Usage:
    from dataops_processing.config.settings import get_settings

    cfg = get_settings()
    kafka_url  = cfg.kafka.bootstrap_servers
    influx_url = cfg.influxdb.url
    silver_path = cfg.delta.silver_path("vwap_1m")
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

import yaml

from dataops_processing.log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

_CONFIG_PATH = Path(__file__).parent / "processing_config.yaml"


# =============================================================================
# Typed config dataclasses — một cấu trúc rõ ràng, không dùng dict["key"]
# =============================================================================

@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    topic_trade_ticks: str
    starting_offsets: str
    max_offsets_per_trigger: int
    request_timeout_ms: int
    session_timeout_ms: int
    fail_on_data_loss: bool


@dataclass(frozen=True)
class InfluxDBSettings:
    url: str
    token: str
    org: str
    bucket: str
    write_precision: str


@dataclass(frozen=True)
class MinIOSettings:
    endpoint: str
    access_key: str
    secret_key: str
    path_style_access: bool
    ssl_enabled: bool


@dataclass(frozen=True)
class DeltaSettings:
    bucket: str
    silver_base: str
    tables: Dict[str, str]

    def silver_path(self, table_key: str) -> str:
        """
        Trả về full S3A path cho Delta Silver table.

        Args:
            table_key: key trong delta.tables (e.g. "vwap_1m", "volume_bars")

        Returns:
            e.g. "s3a://binance-datalake/delta/silver/vwap_1m"
        """
        table_name = self.tables.get(table_key)
        if not table_name:
            raise KeyError(
                f"Delta table '{table_key}' not found in config. "
                f"Available: {list(self.tables.keys())}"
            )
        return f"s3a://{self.bucket}/{self.silver_base}/{table_name}"

    def bronze_path(self, table_name: str) -> str:
        """Trả về full S3A path cho Delta Bronze table theo tên trực tiếp."""
        return f"s3a://{self.bucket}/delta/bronze/{table_name}"


@dataclass(frozen=True)
class SparkSettings:
    app_name: str
    master: str
    log_level: str
    shuffle_partitions: int
    default_parallelism: int
    stop_gracefully_on_shutdown: bool
    checkpoint_base: str
    packages: Dict[str, str]

    def spark_packages_str(self) -> str:
        """Trả về comma-separated Maven coordinates cho spark.jars.packages."""
        return ",".join(self.packages.values())

    def checkpoint_dir(self, job_name: str) -> str:
        """Trả về checkpoint path cho một job cụ thể."""
        return f"{self.checkpoint_base}/{job_name}"


@dataclass(frozen=True)
class VWAPJobSettings:
    window_duration: str
    slide_duration: str
    watermark: str
    trigger_interval: str
    output_mode: str
    influx_measurement: str
    influx_interval_tag: str
    delta_table: str


@dataclass(frozen=True)
class VolumeBarsJobSettings:
    thresholds: Dict[str, float]
    source_table: str
    source_interval: str
    delta_table: str
    overwrite_schema: bool


@dataclass(frozen=True)
class ProcessingSettings:
    """Root settings — inject vào mọi job/service."""
    kafka: KafkaSettings
    influxdb: InfluxDBSettings
    minio: MinIOSettings
    delta: DeltaSettings
    spark: SparkSettings
    vwap_job: VWAPJobSettings
    volume_bars_job: VolumeBarsJobSettings


# =============================================================================
# Loader
# =============================================================================

def _load_yaml() -> Dict[str, Any]:
    if not _CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"processing_config.yaml not found at {_CONFIG_PATH}. "
            "File này là required — không thể khởi động dataops_processing."
        )
    with open(_CONFIG_PATH, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    logger.debug("Loaded processing_config.yaml from %s", _CONFIG_PATH)
    return data


def _env(key: str, yaml_val: Any, cast=str) -> Any:
    """
    Lấy giá trị từ env var nếu có, fallback về yaml_val.
    Tự động cast về đúng type.
    """
    raw = os.environ.get(key)
    if raw is None:
        return yaml_val
    if cast is bool:
        return raw.lower() in ("true", "1", "yes")
    try:
        return cast(raw)
    except (ValueError, TypeError):
        logger.warning("env %s='%s' cannot cast to %s — using yaml value", key, raw, cast)
        return yaml_val


@lru_cache(maxsize=1)
def get_settings() -> ProcessingSettings:
    """
    Load và trả về ProcessingSettings singleton.

    Cached sau lần đầu — gọi nhiều lần không re-read file.
    Env vars được evaluate tại thời điểm gọi lần đầu.

    Returns:
        ProcessingSettings — typed, frozen dataclass.
    """
    cfg = _load_yaml()

    k = cfg.get("kafka", {})
    kc = k.get("consumer", {})
    kafka = KafkaSettings(
        bootstrap_servers=_env("KAFKA_URL",                  k.get("bootstrap_servers", "localhost:9092")),
        topic_trade_ticks=k.get("topics", {}).get("binance_trade_ticks", "binance.trade.ticks"),
        starting_offsets= kc.get("starting_offsets", "latest"),
        max_offsets_per_trigger=int(kc.get("max_offsets_per_trigger", 10000)),
        request_timeout_ms=     int(kc.get("request_timeout_ms", 10000)),
        session_timeout_ms=     int(kc.get("session_timeout_ms", 30000)),
        fail_on_data_loss=      bool(kc.get("fail_on_data_loss", False)),
    )

    i = cfg.get("influxdb", {})
    influxdb = InfluxDBSettings(
        url=            _env("INFLUXDB_URL",    i.get("url",    "http://localhost:8086")),
        token=          _env("INFLUXDB_TOKEN",  i.get("token",  "")),
        org=            _env("INFLUXDB_ORG",    i.get("org",    "binance-vps-platform")),
        bucket=         _env("INFLUXDB_BUCKET", i.get("bucket", "market-data")),
        write_precision=i.get("write_precision", "ns"),
    )

    m = cfg.get("minio", {})
    minio = MinIOSettings(
        endpoint=         _env("MINIO_ENDPOINT",   m.get("endpoint",   "http://minio:9010")),
        access_key=       _env("MINIO_ACCESS_KEY", m.get("access_key", "minioadmin")),
        secret_key=       _env("MINIO_SECRET_KEY", m.get("secret_key", "minioadmin")),
        path_style_access=bool(m.get("path_style_access", True)),
        ssl_enabled=      bool(m.get("ssl_enabled", False)),
    )

    d = cfg.get("delta", {})
    delta = DeltaSettings(
        bucket=      _env("DELTA_BUCKET", d.get("bucket", "binance-datalake")),
        silver_base= d.get("silver_base", "delta/silver"),
        tables=      d.get("tables", {"vwap_1m": "vwap_1m", "volume_bars": "volume_bars"}),
    )

    s = cfg.get("spark", {})
    spark = SparkSettings(
        app_name=                  _env("SPARK_APP_NAME",      s.get("app_name", "dataops-processing")),
        master=                    _env("SPARK_MASTER",         s.get("master",   "local[*]")),
        log_level=                 s.get("log_level", "WARN"),
        shuffle_partitions=    int(s.get("shuffle_partitions", 8)),
        default_parallelism=   int(s.get("default_parallelism", 4)),
        stop_gracefully_on_shutdown=bool(s.get("stop_gracefully_on_shutdown", True)),
        checkpoint_base=           _env("SPARK_CHECKPOINT_DIR", s.get("checkpoint_base", "/tmp/spark-checkpoints")),
        packages=                  s.get("packages", {}),
    )

    v = cfg.get("vwap_job", {})
    vwap_job = VWAPJobSettings(
        window_duration=  v.get("window_duration",   "5 minutes"),
        slide_duration=   v.get("slide_duration",    "1 minute"),
        watermark=        v.get("watermark",         "30 seconds"),
        trigger_interval= v.get("trigger_interval",  "30 seconds"),
        output_mode=      v.get("output_mode",       "append"),
        influx_measurement=v.get("influx_measurement", "vwap"),
        influx_interval_tag=v.get("influx_interval_tag", "5m"),
        delta_table=      v.get("delta_table",       "vwap_1m"),
    )

    vb = cfg.get("volume_bars_job", {})
    volume_bars_job = VolumeBarsJobSettings(
        thresholds=     {k: float(v) for k, v in vb.get("thresholds", {}).items()},
        source_table=   vb.get("source_table",   "binance_candles_1d"),
        source_interval=vb.get("source_interval", "1d"),
        delta_table=    vb.get("delta_table",    "volume_bars"),
        overwrite_schema=bool(vb.get("overwrite_schema", True)),
    )

    settings = ProcessingSettings(
        kafka=kafka,
        influxdb=influxdb,
        minio=minio,
        delta=delta,
        spark=spark,
        vwap_job=vwap_job,
        volume_bars_job=volume_bars_job,
    )
    logger.info(
        "ProcessingSettings loaded — kafka=%s influx=%s minio=%s spark_master=%s",
        settings.kafka.bootstrap_servers,
        settings.influxdb.url,
        settings.minio.endpoint,
        settings.spark.master,
    )
    return settings
