"""
daily_pipeline.py — Master Orchestration Flow.

Gọi Bronze + Silver + Indicators theo thứ tự đúng cho daily pipeline run.
Đây là entrypoint cho scheduled execution (cron / Prefect / Airflow).

Pipeline order:
    1. Bronze Binance SPOT    — fetch candles mới nhất → Delta Bronze
    2. Bronze Binance FUTURES — fetch futures candles → Delta Bronze
    3. Bronze TCBS Market     — fetch full market snapshot theo index → Delta Bronze
    4. Bronze TCBS BSA        — fetch Buy/Sell Analysis per ticker → Delta Bronze
    5. Bronze TCBS Intraday   — fetch intraday tick history per ticker → Delta Bronze
    6. Silver Streaming       — PySpark jobs đã chạy riêng (silver_flow.py), không khởi tại đây

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

from cli.flows.serving.bronze_flow import (
    run_binance_bronze,
    run_tcbs_bsa_bronze,
    run_tcbs_intraday_bronze,
    run_tcbs_market_bronze,
)
from cli.flows.serving.metadata import setup_logging

logger = logging.getLogger(__name__)


def run_daily_pipeline() -> dict:
    """
    Chạy toàn bộ daily pipeline theo thứ tự Bronze → (Silver riêng).

    Returns:
        dict tổng kết kết quả:
        {
            "binance_spot":     n,
            "binance_futures":  n,
            "tcbs_market":      n,
            "tcbs_bsa":         n,
            "tcbs_intraday":    n,
            "duration_sec":     t,
        }
    """
    start_time = time.time()
    started_at = datetime.now(timezone.utc).isoformat()
    logger.info("=== daily_pipeline started at %s ===", started_at)

    results: dict = {}

    # ── Step 1: Bronze Binance SPOT ───────────────────────────────────────────
    logger.info("[daily_pipeline] Step 1/5 — Bronze Binance SPOT")
    try:
        n = run_binance_bronze(inst_type="SPOT")
        results["binance_spot"] = n
        logger.info("[daily_pipeline] Step 1 done: %d SPOT records", n)
    except Exception as e:
        logger.error("[daily_pipeline] Step 1 failed: %s", e, exc_info=True)
        results["binance_spot"] = 0

    # ── Step 2: Bronze Binance FUTURES ────────────────────────────────────────
    logger.info("[daily_pipeline] Step 2/5 — Bronze Binance FUTURES")
    try:
        n = run_binance_bronze(inst_type="FUTURES")
        results["binance_futures"] = n
        logger.info("[daily_pipeline] Step 2 done: %d FUTURES records", n)
    except Exception as e:
        logger.error("[daily_pipeline] Step 2 failed: %s", e, exc_info=True)
        results["binance_futures"] = 0

    # ── Step 3: Bronze TCBS Market Snapshot ──────────────────────────────────
    # Config index list từ tcbs.default_indexes trong exchange_config.yaml
    logger.info("[daily_pipeline] Step 3/5 — Bronze TCBS Market (all configured indexes)")
    try:
        n = run_tcbs_market_bronze()
        results["tcbs_market"] = n
        logger.info("[daily_pipeline] Step 3 done: %d TCBS market records", n)
    except Exception as e:
        logger.error("[daily_pipeline] Step 3 failed: %s", e, exc_info=True)
        results["tcbs_market"] = 0

    # ── Step 4: Bronze TCBS BSA ───────────────────────────────────────────────
    # Config ticker list từ tcbs.bsa_tickers trong exchange_config.yaml
    logger.info("[daily_pipeline] Step 4/5 — Bronze TCBS BSA (Buy/Sell Analysis)")
    try:
        n = run_tcbs_bsa_bronze()
        results["tcbs_bsa"] = n
        logger.info("[daily_pipeline] Step 4 done: %d TCBS BSA records", n)
    except Exception as e:
        logger.error("[daily_pipeline] Step 4 failed: %s", e, exc_info=True)
        results["tcbs_bsa"] = 0

    # ── Step 5: Bronze TCBS Intraday Ticks ───────────────────────────────────
    # Config ticker list từ tcbs.bsa_tickers; pages từ tcbs.intraday_max_pages
    logger.info("[daily_pipeline] Step 5/5 — Bronze TCBS Intraday Tick History")
    try:
        n = run_tcbs_intraday_bronze()
        results["tcbs_intraday"] = n
        logger.info("[daily_pipeline] Step 5 done: %d TCBS intraday tick records", n)
    except Exception as e:
        logger.error("[daily_pipeline] Step 5 failed: %s", e, exc_info=True)
        results["tcbs_intraday"] = 0

    duration = round(time.time() - start_time, 2)
    results["duration_sec"] = duration

    total = (
        results.get("binance_spot", 0)
        + results.get("binance_futures", 0)
        + results.get("tcbs_market", 0)
        + results.get("tcbs_bsa", 0)
        + results.get("tcbs_intraday", 0)
    )
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
