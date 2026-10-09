"""
metrics.py — Prometheus metrics cho dataops_batch_api batch jobs.

Expose counters theo dõi số records ingest thành công / thất bại
cho từng batch job. Push sang Prometheus Pushgateway hoặc expose
HTTP /metrics endpoint nếu cần.

Design:
    - Dùng prometheus_client nếu đã cài. Nếu thiếu → stub no-op.
    - Tất cả counters được label bởi: job_name, symbol, inst_type.
    - Module này là dependency tùy chọn — không làm crash job nếu thiếu lib.

Counters:
    batch_ingest_records_total{job,symbol,inst_type,status}
        status = "success" | "error"

Usage:
    from cli.jobs.metrics import record_ingest

    record_ingest("binance_1d_job", symbol="BTCUSDT", inst_type="SPOT", count=365, status="success")
    record_ingest("tcbs_eod_job",   symbol="VCB",     inst_type="STOCK", count=0,   status="error")

Prometheus Pushgateway (nếu PROMETHEUS_PUSHGATEWAY_URL được set):
    push_metrics(job="binance_1d_job")
"""
from __future__ import annotations

import logging
import os
from typing import Literal

logger = logging.getLogger(__name__)

# ── Optional Prometheus import ─────────────────────────────────────────────────
try:
    from prometheus_client import CollectorRegistry, Counter, push_to_gateway

    _registry = CollectorRegistry()

    _INGEST_TOTAL = Counter(
        name="batch_ingest_records_total",
        documentation="Total records ingested by batch jobs, labeled by status.",
        labelnames=["job", "symbol", "inst_type", "status"],
        registry=_registry,
    )
    _PROMETHEUS_AVAILABLE = True
    logger.debug("prometheus_client loaded — metrics enabled.")

except ImportError:
    _PROMETHEUS_AVAILABLE = False
    _INGEST_TOTAL = None  # type: ignore[assignment]
    logger.warning(
        "prometheus_client not installed — metrics are no-ops. "
        "Add prometheus-client to requirements.txt to enable."
    )


# ── Public API ─────────────────────────────────────────────────────────────────

def record_ingest(
    job: str,
    count: int,
    status: Literal["success", "error"],
    symbol: str = "all",
    inst_type: str = "all",
) -> None:
    """
    Tăng counter batch_ingest_records_total.

    Args:
        job:       Tên job, e.g. "binance_1d_job", "tcbs_eod_job".
        count:     Số records thành công hoặc thất bại trong batch này.
        status:    "success" | "error".
        symbol:    Symbol liên quan, e.g. "BTCUSDT". Default "all".
        inst_type: Loại instrument, e.g. "SPOT", "FUTURES", "STOCK". Default "all".
    """
    if not _PROMETHEUS_AVAILABLE or count <= 0:
        return
    _INGEST_TOTAL.labels(
        job=job, symbol=symbol, inst_type=inst_type, status=status
    ).inc(count)


def push_metrics(job: str) -> None:
    """
    Push metrics lên Prometheus Pushgateway.

    Đọc URL từ env var PROMETHEUS_PUSHGATEWAY_URL.
    No-op nếu prometheus_client chưa cài hoặc URL không được set.

    Args:
        job: Grouping key 'job' cho Pushgateway, e.g. "binance_1d_job".
    """
    if not _PROMETHEUS_AVAILABLE:
        return

    gateway_url = os.environ.get("PROMETHEUS_PUSHGATEWAY_URL", "")
    if not gateway_url:
        logger.debug("PROMETHEUS_PUSHGATEWAY_URL not set — skipping push_metrics.")
        return

    try:
        push_to_gateway(gateway_url, job=job, registry=_registry)
        logger.info("push_metrics: pushed to %s (job=%s)", gateway_url, job)
    except Exception as exc:
        # Metrics push failure không được làm crash job pipeline
        logger.warning("push_metrics failed (job=%s): %s", job, exc)
