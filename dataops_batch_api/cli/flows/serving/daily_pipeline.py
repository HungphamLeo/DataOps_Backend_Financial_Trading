"""
daily_pipeline.py — Master Orchestration Flow.

Gọi Bronze + Silver + Indicators theo thứ tự đúng cho daily pipeline run.
Đây là entrypoint cho scheduled execution (cron / Prefect / Airflow).

Pipeline order:
    1. Bronze OKX SPOT  — fetch candles mới nhất → Delta Bronze
    2. Bronze OKX SWAP  — fetch funding/swap candles → Delta Bronze
    3. Bronze VPS HOSE  — fetch market snapshot (trong giờ giao dịch) → Delta Bronze
    4. Silver Streaming — PySpark jobs đã chạy riêng (silver_flow.py), không khởi tại đây

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

from cli.flows.serving.bronze_flow import run_okx_bronze, run_vps_bronze
from cli.flows.serving.metadata import setup_logging

logger = logging.getLogger("daily_pipeline")


def run_daily_pipeline() -> dict:
    """
    Chạy toàn bộ daily pipeline theo thứ tự Bronze → (Silver riêng).

    Returns:
        dict tổng kết kết quả: {"okx_spot": n, "okx_swap": n, "vps": n, "duration_sec": t}
    """
    start_time = time.time()
    started_at = datetime.now(timezone.utc).isoformat()
    logger.info(f"=== daily_pipeline started at {started_at} ===")

    results: dict = {}

    # ── Step 1: Bronze OKX SPOT ───────────────────────────────────────────────
    logger.info("[daily_pipeline] Step 1/3 — Bronze OKX SPOT")
    try:
        n = run_okx_bronze(inst_type="SPOT")
        results["okx_spot"] = n
        logger.info(f"[daily_pipeline] Step 1 done: {n} SPOT records")
    except Exception as e:
        logger.error(f"[daily_pipeline] Step 1 failed: {e}", exc_info=True)
        results["okx_spot"] = 0

    # ── Step 2: Bronze OKX SWAP ───────────────────────────────────────────────
    logger.info("[daily_pipeline] Step 2/3 — Bronze OKX SWAP")
    try:
        n = run_okx_bronze(inst_type="SWAP")
        results["okx_swap"] = n
        logger.info(f"[daily_pipeline] Step 2 done: {n} SWAP records")
    except Exception as e:
        logger.error(f"[daily_pipeline] Step 2 failed: {e}", exc_info=True)
        results["okx_swap"] = 0

    # ── Step 3: Bronze VPS HOSE ───────────────────────────────────────────────
    logger.info("[daily_pipeline] Step 3/3 — Bronze VPS HOSE")
    try:
        n = run_vps_bronze(exchange="HOSE")
        results["vps"] = n
        logger.info(f"[daily_pipeline] Step 3 done: {n} VPS snapshots")
    except Exception as e:
        logger.error(f"[daily_pipeline] Step 3 failed: {e}", exc_info=True)
        results["vps"] = 0

    duration = round(time.time() - start_time, 2)
    results["duration_sec"] = duration

    total = results.get("okx_spot", 0) + results.get("okx_swap", 0) + results.get("vps", 0)
    logger.info(
        f"=== daily_pipeline complete: {total:,} total records, "
        f"duration={duration}s ==="
    )
    return results


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    setup_logging()
    summary = run_daily_pipeline()
    print(f"\nPipeline Summary: {summary}")
