"""
scheduler.py — Entrypoint chạy các ETL jobs theo schedule.

Dùng APScheduler (lightweight, không cần Prefect server).
Mỗi job chạy theo cron riêng. Lỗi 1 job không ảnh hưởng job khác.

Schedule:
    binance_4h_job  : cron "5 0 * * *"       (UTC) — 00:05 UTC hàng ngày
    binance_1d_job  : cron "5 0 * * *"       (UTC) — 00:05 UTC hàng ngày
    tcbs_eod_job    : cron "15 8 * * 1-5"    (UTC) — 15:15 ICT, T2-T6
    tcbs_bsa_job    : cron "30 8 * * 1-5"    (UTC) — 15:30 ICT, T2-T6
    tcbs_margin_job : cron "45 8 * * 1-5"    (UTC) — 15:45 ICT, T2-T6
    benchmark_index_job : cron "50 8 * * 1-5" (UTC) — 15:50 ICT, T2-T6

Run:
    python -m cli.scheduler

Env vars bắt buộc:
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL
    BINANCE_API_KEY, BINANCE_SECRET_KEY (optional — chỉ cho private endpoints)
    TCBS_API_KEY, TCBS_JWT_TOKEN
"""
from __future__ import annotations

import logging
import signal
import sys

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from cli.jobs import (
    binance_4h_job, binance_1d_job, tcbs_eod_job, tcbs_bsa_job,
    tcbs_margin_job, benchmark_index_job,
)
from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

# APScheduler uses standard logging
logging.getLogger("apscheduler").setLevel(logging.WARNING)


def _run_job(name: str, fn) -> None:
    """Wrapper: log start/end, catch exception để tránh crash scheduler."""
    logger.info("scheduler: START job=%s", name)
    try:
        result = fn()
        logger.info("scheduler: DONE  job=%s result=%s", name, result)
    except Exception as exc:
        logger.error("scheduler: FAIL  job=%s error=%s", name, exc, exc_info=True)


def build_scheduler() -> BlockingScheduler:
    scheduler = BlockingScheduler(timezone="UTC")

    # [00:05 UTC daily] Binance 4h — 1 năm rolling
    scheduler.add_job(
        lambda: _run_job("binance_4h", binance_4h_job.run),
        CronTrigger(hour=0, minute=5),
        id="binance_4h",
        name="Binance 4h candles",
        max_instances=1,
        misfire_grace_time=300,
    )

    # [00:05 UTC daily] Binance 1d — append-only
    scheduler.add_job(
        lambda: _run_job("binance_1d", binance_1d_job.run),
        CronTrigger(hour=0, minute=5),
        id="binance_1d",
        name="Binance 1d candles",
        max_instances=1,
        misfire_grace_time=300,
    )

    # [15:15 ICT = 08:15 UTC, T2-T6] TCBS EOD snapshot
    scheduler.add_job(
        lambda: _run_job("tcbs_eod", tcbs_eod_job.run),
        CronTrigger(hour=8, minute=15, day_of_week="mon-fri"),
        id="tcbs_eod",
        name="TCBS EOD market snapshot",
        max_instances=1,
        misfire_grace_time=600,
    )

    # [15:30 ICT = 08:30 UTC, T2-T6] TCBS BSA daily summary
    scheduler.add_job(
        lambda: _run_job("tcbs_bsa", tcbs_bsa_job.run),
        CronTrigger(hour=8, minute=30, day_of_week="mon-fri"),
        id="tcbs_bsa",
        name="TCBS BSA daily summary",
        max_instances=1,
        misfire_grace_time=600,
    )

    # [15:45 ICT = 08:45 UTC, T2-T6] TCBS margin risk snapshot
    scheduler.add_job(
        lambda: _run_job("tcbs_margin", tcbs_margin_job.run),
        CronTrigger(hour=8, minute=45, day_of_week="mon-fri"),
        id="tcbs_margin",
        name="TCBS margin risk snapshot",
        max_instances=1,
        misfire_grace_time=600,
    )

    # [15:50 ICT = 08:50 UTC, T2-T6] Benchmark index (VNINDEX, VN30, BTC.D proxy)
    scheduler.add_job(
        lambda: _run_job("benchmark_index", benchmark_index_job.run),
        CronTrigger(hour=8, minute=50, day_of_week="mon-fri"),
        id="benchmark_index",
        name="Benchmark index daily",
        max_instances=1,
        misfire_grace_time=600,
    )

    return scheduler


def main() -> None:
    logger.info(
        "scheduler: starting — jobs: binance_4h, binance_1d, tcbs_eod, tcbs_bsa, "
        "tcbs_margin, benchmark_index"
    )
    scheduler = build_scheduler()

    # Graceful shutdown on SIGTERM / SIGINT
    def _shutdown(signum, frame):
        logger.info("scheduler: received signal %d — shutting down", signum)
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("scheduler: stopped")


if __name__ == "__main__":
    main()
