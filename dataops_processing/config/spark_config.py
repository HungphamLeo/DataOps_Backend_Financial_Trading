"""
spark_config.py — SparkSession builder cho dataops_processing.

Nhận SparkSettings từ get_settings() — không đọc os.environ trực tiếp.
Tất cả tham số (packages, master, memory, checkpoint) đến từ processing_config.yaml
với env var override qua settings.py.

Usage:
    from dataops_processing.config.spark_config import build_spark_session, get_kafka_options
    from dataops_processing.config.settings import get_settings

    spark = build_spark_session("dataops-vwap-job")
    kafka_opts = get_kafka_options()

═══════════════════════════════════════════════════════════════════════════════
KIẾN TRÚC SPARK PRODUCTION — ST02, ST12 (PLAN_REPO03)
═══════════════════════════════════════════════════════════════════════════════

ST02 — Kafka Static Membership & Rebalance Storm Prevention:
  Vấn đề: Consumer pod bị Stop-The-World GC Pause (Java/JVM overhead).
  Group Coordinator thấy heartbeat miss → đánh dấu pod "chết" → kích hoạt
  TOÀN BỘ cụm Stop-The-World Rebalance (tất cả consumers ngừng xử lý).
  Pod "hồi sinh" sau GC → Rebalance lại → vòng lặp vô tận.

  Kafka Consumer Configs phải đặt trong SparkSession Kafka options:
    kafka.session.timeout.ms=45000     — chịu GC pause dài đến 45s (default thường 10s)
    kafka.heartbeat.interval.ms=15000  — heartbeat mỗi 15s (phải < session.timeout/3)
    kafka.max.poll.interval.ms=300000  — 5 phút cho Spark micro-batch nặng

  Static Membership (Kafka 2.3+):
    kafka.group.instance.id={hostname}-{app_name}  — định danh cố định per consumer.
    Khi pod restart trong session.timeout → Coordinator KHÔNG rebalance,
    giữ nguyên partition assignment → pipeline tiếp tục trong suốt.

ST12 — Spark RocksDB State Store & Hot-Key Skew Defense:
  Vấn đề: Window Aggregation giữ state trong JVM Heap.
  BTC chiếm ~40% volume → toàn bộ BTC windows dồn vào 1 executor → OOMKilled.

  RocksDB State Backend:
    spark.sql.streaming.stateStore.providerClass = RocksDBStateStoreProvider
    RocksDB (LSM-Tree) xả State từ JVM Heap xuống NVMe disk cục bộ của executor.
    Chỉ cache working set lên RAM → loại bỏ JVM GC pressure từ State management.
    LSM-Tree: write → MemTable (RAM) → immutable SSTable (disk) → Compaction.

  Adaptive Query Execution (AQE) — Spark 3.x:
    spark.sql.adaptive.enabled = true
    spark.sql.adaptive.skewJoin.enabled = true
    AQE phát hiện Skew Partition tại RUNTIME (khi data đã có) thay vì plan time.
    Tự động chia nhỏ partition lớn (BTC) → distribute đều sang nhiều executors.
    advisoryPartitionSizeInBytes = 128MB → mỗi partition tối đa 128MB trước khi split.

  Two-Phase Aggregation (Salting):
    Khi AQE không đủ (extreme skew), dùng Salting:
    Thêm random suffix (0-N) vào partition key → BTC chia thành N sub-groups.
    Phase 1: Local aggregation (mỗi executor tính VWAP riêng).
    Phase 2: Global merge (gom kết quả N sub-groups lại).
    Xem vwap_job.py để thấy implementation.
"""
from __future__ import annotations
import socket
import logging

from dataops_processing.log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)


def build_spark_session(app_name: str | None = None):
    """
    Tạo SparkSession đã configured với Kafka + Delta + S3A.

    Tất cả config đọc từ ProcessingSettings (settings.py + yaml + env override).
    Không dùng os.environ trực tiếp ở đây.

    Args:
        app_name: Override tên job trên Spark UI.
                  None → dùng settings.spark.app_name từ config.

    Returns:
        pyspark.sql.SparkSession

    Production notes (ST12):
        - Để bật RocksDB State Backend, thêm vào builder:
          .config("spark.sql.streaming.stateStore.providerClass",
                  "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        - Để bật AQE:
          .config("spark.sql.adaptive.enabled", "true")
          .config("spark.sql.adaptive.skewJoin.enabled", "true")
          .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")  # 128MB
        Các config này chưa bật mặc định vì cần test trên cluster trước (ST12 TODO).
    """
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "PySpark hoặc delta-spark chưa được cài. "
            "Chạy: pip install pyspark==3.5.1 delta-spark==3.1.0"
        ) from exc

    from dataops_processing.config.settings import get_settings
    cfg = get_settings()
    s   = cfg.spark
    m   = cfg.minio

    name = app_name or s.app_name

    logger.info(
        "Building SparkSession: app=%s master=%s minio=%s",
        name, s.master, m.endpoint,
    )

    builder = (
        SparkSession.builder
        .appName(name)
        .master(s.master)
        # ── Delta Lake ──────────────────────────────────────────────────────
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # ── Spark packages (download tại submit qua Maven) ──────────────────
        .config("spark.jars.packages", s.spark_packages_str())
        # ── S3A / MinIO ─────────────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint",    m.endpoint)
        .config("spark.hadoop.fs.s3a.access.key",  m.access_key)
        .config("spark.hadoop.fs.s3a.secret.key",  m.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", str(m.path_style_access).lower())
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(m.ssl_enabled).lower())
        # ── Streaming graceful shutdown ──────────────────────────────────────
        .config("spark.streaming.stopGracefullyOnShutdown",
                str(s.stop_gracefully_on_shutdown).lower())
        # ── Shuffle / parallelism (local mode tuning) ───────────────────────
        .config("spark.sql.shuffle.partitions", str(s.shuffle_partitions))
        .config("spark.default.parallelism",    str(s.default_parallelism))
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(s.log_level)

    logger.info("SparkSession created: Spark %s", spark.version)
    return spark


def get_kafka_options(topic: str | None = None, job_name: str = "vwap-job") -> dict[str, str]:
    """
    Trả về Kafka read options cho spark.readStream.format("kafka").

    Args:
        topic: Override Kafka topic. None → dùng kafka.topics.binance_trade_ticks từ config.

    Returns:
        Dict options để truyền vào .options(**opts).

    ST02 — Static Membership & Timeout Tuning:
        maxOffsetsPerTrigger kiểm soát tốc độ consume — quan trọng khi catchup sau outage.
        Nếu để không giới hạn: Spark kéo hàng triệu records vào 1 micro-batch → executor OOM.

        kafka.session.timeout.ms: thời gian Group Coordinator chờ heartbeat trước khi
        đánh dấu consumer "chết". Phải đủ dài để qua Stop-The-World GC Pause (JVM).
        Default Kafka: 10000ms (10s) — quá ngắn cho JVM workloads (GC có thể kéo dài 30s+).

        kafka.heartbeat.interval.ms: tần suất gửi heartbeat.
        Quy tắc: heartbeat.interval < session.timeout / 3 (để có ít nhất 3 cơ hội trước khi timeout).

        kafka.max.poll.interval.ms: khoảng thời gian tối đa giữa 2 lần poll().
        Spark micro-batch xử lý nặng có thể mất vài phút → phải đặt cao hơn thời gian processing.
        Nếu micro-batch kéo dài hơn max.poll.interval.ms → Coordinator kick consumer → Rebalance.

        Static Membership (TODO ST02):
        Thêm "kafka.group.instance.id": f"{socket.gethostname()}-vwap-job"
        để prevent Rebalance khi pod restart trong window session.timeout.
    """
    from dataops_processing.config.settings import get_settings
    cfg = get_settings()
    k   = cfg.kafka

    resolved_topic = topic or k.topic_trade_ticks

    opts = {
        "kafka.bootstrap.servers":     k.bootstrap_servers,
        "subscribe":                   resolved_topic,
        "startingOffsets":             k.starting_offsets,
        "maxOffsetsPerTrigger":        str(k.max_offsets_per_trigger),
        "kafka.request.timeout.ms":    str(k.request_timeout_ms),
        "kafka.session.timeout.ms":    str(k.session_timeout_ms),
        "kafka.heartbeat.interval.ms": str(k.heartbeat_interval_ms),
        "kafka.max.poll.interval.ms":  str(k.max_poll_interval_ms),
        "failOnDataLoss":              str(k.fail_on_data_loss).lower(),
    }
    if k.group_instance_id_enabled:
        opts["kafka.group.instance.id"] = f"{socket.gethostname()}-{job_name}"
    return opts