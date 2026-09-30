"""
prefect_main.py — Prefect ETL Pipeline Orchestration.

Định nghĩa Prefect flows và schedules cho OKX batch pipeline.
Tương đương với Airflow DAGs nhưng dùng Prefect 2.x API.

Flows được định nghĩa:
    1. okx_daily_bronze_flow   — Hàng giờ, Bronze ingestion OKX SPOT + SWAP
    2. vps_market_flow         — Mỗi 1 phút trong giờ giao dịch (9:00–14:45)
    3. okx_silver_streaming    — Long-running PySpark jobs (chạy 1 lần, không stop)
    4. okx_backfill_flow       — Manual trigger, không schedule tự động

Cách chạy:
    # Development
    python platforms/prefect_orchestra/prefect_main.py

    # Production với Prefect server
    prefect deployment build platforms/prefect_orchestra/prefect_main.py:daily_bronze_flow -n prod
    prefect deployment apply daily_bronze_flow-deployment.yaml
    prefect agent start -q default
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from cli.flows.serving.bronze_flow import run_okx_bronze, run_vps_bronze
from cli.flows.serving.daily_pipeline import run_daily_pipeline
from cli.flows.serving.metadata import setup_logging

logger = logging.getLogger("prefect_main")

# ── Prefect imports với graceful fallback khi không có prefect installed ───────
try:
    from prefect import flow, task
    from prefect.schedules import CronSchedule
    _PREFECT_AVAILABLE = True
except ImportError:
    _PREFECT_AVAILABLE = False
    # Tạo no-op decorators để code chạy được khi không có prefect
    def flow(*args, **kwargs):
        def decorator(fn):
            return fn
        return decorator if args and callable(args[0]) else decorator

    def task(*args, **kwargs):
        def decorator(fn):
            return fn
        return decorator if args and callable(args[0]) else decorator


# ─── Prefect Tasks ────────────────────────────────────────────────────────────

@task(name="bronze-okx-spot", retries=2, retry_delay_seconds=60)
def task_bronze_okx_spot() -> int:
    """Task: Bronze ingestion OKX SPOT candles."""
    return run_okx_bronze(inst_type="SPOT")


@task(name="bronze-okx-swap", retries=2, retry_delay_seconds=60)
def task_bronze_okx_swap() -> int:
    """Task: Bronze ingestion OKX SWAP candles."""
    return run_okx_bronze(inst_type="SWAP")


@task(name="bronze-vps-hose", retries=3, retry_delay_seconds=30)
def task_bronze_vps() -> int:
    """Task: Bronze VPS HOSE market snapshot."""
    return run_vps_bronze(exchange="HOSE")


# ─── Prefect Flows ────────────────────────────────────────────────────────────

@flow(name="okx-daily-bronze-flow")
def daily_bronze_flow() -> dict:
    """
    Flow chạy hourly: Bronze OKX SPOT + SWAP candles.
    Schedule: mỗi giờ (*/1 * * * *)

    Không bao gồm VPS (VPS có flow riêng để schedule phù hợp giờ giao dịch).
    """
    logger.info(f"[prefect] daily_bronze_flow started at {datetime.now(timezone.utc).isoformat()}")
    spot_n = task_bronze_okx_spot()
    swap_n = task_bronze_okx_swap()
    logger.info(f"[prefect] daily_bronze_flow done: spot={spot_n}, swap={swap_n}")
    return {"okx_spot": spot_n, "okx_swap": swap_n}


@flow(name="vps-market-snapshot-flow")
def vps_market_flow() -> int:
    """
    Flow chạy mỗi 1 phút trong giờ giao dịch: VPS HOSE snapshot.
    Schedule: */1 9-14 * * 1-5 (Thứ 2–6, 9:00–14:59)

    Note: Cron chạy tới 14:59 nhưng VPS đóng cửa lúc 14:45.
          Record sau 14:45 sẽ có session_state="CLOSED" — vẫn ghi, không drop.
    """
    return task_bronze_vps()


@flow(name="okx-full-daily-pipeline")
def full_daily_flow() -> dict:
    """
    Flow orchestrator chạy toàn bộ daily pipeline.
    Bao gồm cả OKX và VPS trong 1 run — dùng cho end-of-day reconciliation.
    """
    return run_daily_pipeline()


# ─── PrefectETLPipelineConfig ──────────────────────────────────────────────────

class PrefectETLPipelineConfig:
    """
    Configuration class cho Prefect deployments.
    Centralize schedule definitions và deployment parameters.
    """

    # Cron schedules
    SCHEDULE_HOURLY_OKX  = "0 * * * *"          # Mỗi giờ đầu giờ
    SCHEDULE_VPS_TRADING = "*/1 9-14 * * 1-5"   # Mỗi 1 phút, giờ giao dịch, Thứ 2–6
    SCHEDULE_EOD         = "30 15 * * 1-5"      # 15:30 hàng ngày (sau đóng cửa)

    # Deployment names
    DEPLOY_BRONZE_HOURLY = "okx-bronze-hourly-prod"
    DEPLOY_VPS_MARKET    = "vps-market-snapshot-prod"
    DEPLOY_FULL_DAILY    = "okx-full-daily-eod-prod"

    # Prefect work queue
    WORK_QUEUE = "default"


# ─── Standalone runner ────────────────────────────────────────────────────────

if __name__ == "__main__":
    setup_logging()
    if _PREFECT_AVAILABLE:
        logger.info("Running full_daily_flow via Prefect...")
        full_daily_flow()
    else:
        logger.warning("Prefect not installed — running daily_pipeline directly")
        result = run_daily_pipeline()
        print(f"Pipeline result: {result}")
