"""
daily_pipeline.py — Master Orchestration Flow.

Gọi Bronze + Silver + Indicators theo thứ tự đúng cho daily pipeline run.
Đây là entrypoint cho scheduled execution (cron / Prefect / Airflow).

Pipeline order:
    1. Bronze Binance SPOT    — fetch candles mới nhất → Delta Bronze
    2. Bronze Binance FUTURES — fetch futures candles → Delta Bronze
    3. Bronze VPS HOSE        — fetch market snapshot (trong giờ giao dịch) → Delta Bronze
    4. Silver Streaming       — PySpark jobs đã chạy riêng (silver_flow.py), không khởi tại đây

Separation of concerns:
    - daily_pipeline.py = orchestrator (gọi flows theo thứ tự, log kết quả)
    - bronze_flow.py    = Bronze ingestion logic
    - silver_flow.py    = Silver PySpark logic (luôn chạy dưới dạng long-running process)
    - backfill_flow.py  = historical backfill (chạy riêng, không trong daily)
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from cli.flows.serving.bronze_flow import run_binance_bronze, run_vps_bronze
from cli.flows.serving.metadata import setup_logging

logger = logging.getLogger(__name__)


def run_daily_pipeline() -> dict:
    """
    Chạy toàn bộ daily pipeline theo thứ tự Bronze → (Silver riêng).

    Returns:
        dict tổng kết kết quả: {"binance_spot": n, "binance_futures": n, "vps": n, "duration_sec": t}
    """
    start_time = time.time()
    started_at = datetime.now(timezone.utc).isoformat()
    logger.info("=== daily_pipeline started at %s ===", started_at)

    results: dict = {}

    # ── Step 1: Bronze Binance SPOT ───────────────────────────────────────────
    logger.info("[daily_pipeline] Step 1/3 — Bronze Binance SPOT")
    try:
        n = run_binance_bronze(inst_type="SPOT")
        results["binance_spot"] = n
        logger.info("[daily_pipeline] Step 1 done: %d SPOT records", n)
    except Exception as e:
        logger.error("[daily_pipeline] Step 1 failed: %s", e, exc_info=True)
        results["binance_spot"] = 0

    # ── Step 2: Bronze Binance FUTURES ────────────────────────────────────────
    logger.info("[daily_pipeline] Step 2/3 — Bronze Binance FUTURES")
    try:
        n = run_binance_bronze(inst_type="FUTURES")
        results["binance_futures"] = n
        logger.info("[daily_pipeline] Step 2 done: %d FUTURES records", n)
    except Exception as e:
        logger.error("[daily_pipeline] Step 2 failed: %s", e, exc_info=True)
        results["binance_futures"] = 0

    # ── Step 3: Bronze VPS HOSE ───────────────────────────────────────────────
    logger.info("[daily_pipeline] Step 3/3 — Bronze VPS HOSE")
    try:
        n = run_vps_bronze(exchange="HOSE")
        results["vps"] = n
        logger.info("[daily_pipeline] Step 3 done: %d VPS snapshots", n)
    except Exception as e:
        logger.error("[daily_pipeline] Step 3 failed: %s", e, exc_info=True)
        results["vps"] = 0

    duration = round(time.time() - start_time, 2)
    results["duration_sec"] = duration

    total = results.get("binance_spot", 0) + results.get("binance_futures", 0) + results.get("vps", 0)
    logger.info(
        "=== daily_pipeline complete: %d total records, duration=%.2fs ===",
        total, duration,
    )
    return results


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    setup_logging()
    summary = run_daily_pipeline()
    print(f"\nPipeline Summary: {summary}")
