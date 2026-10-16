"""
__main__.py — Entry point duy nhất cho dataops_batch_api.

Tương tự main.go trong Go — một nơi duy nhất parse CLI và dispatch.

Commands:
    python -m dataops_batch_api scheduler              # long-running APScheduler
    python -m dataops_batch_api run binance-4h         # chạy 1 job thủ công rồi thoát
    python -m dataops_batch_api run binance-1d
    python -m dataops_batch_api run binance-1d --from-date 2026-01-01 --to-date 2026-10-01
    python -m dataops_batch_api run tcbs-eod
    python -m dataops_batch_api run tcbs-bsa
    python -m dataops_batch_api run tcbs-margin
    python -m dataops_batch_api run benchmark-index

Trong Docker (Dockerfile CMD):
    CMD ["python", "-m", "dataops_batch_api", "scheduler"]
"""
from __future__ import annotations

import argparse
import json
import sys

from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

# ── Job registry: tên CLI → (module.run, hỗ trợ date args) ──────────────────
_JOBS = {
    "binance-4h":       ("cli.jobs.binance_4h_job",       False),
    "binance-1d":       ("cli.jobs.binance_1d_job",       True),   # supports from/to date
    "tcbs-eod":         ("cli.jobs.tcbs_eod_job",         False),
    "tcbs-bsa":         ("cli.jobs.tcbs_bsa_job",         False),
    "tcbs-margin":      ("cli.jobs.tcbs_margin_job",      False),
    "benchmark-index":  ("cli.jobs.benchmark_index_job",  False),
}


def _import_run(module_path: str):
    """Lazy import để tránh load toàn bộ deps khi chỉ hiển thị help."""
    import importlib
    mod = importlib.import_module(module_path)
    return mod.run


def cmd_run(args: argparse.Namespace) -> None:
    """Chạy một job thủ công rồi thoát — tương đương `go run ./cmd/job`."""
    job_key = args.job_name
    if job_key not in _JOBS:
        print(f"Unknown job: {job_key}. Available: {list(_JOBS)}", file=sys.stderr)
        sys.exit(1)

    module_path, has_dates = _JOBS[job_key]
    run_fn = _import_run(module_path)

    logger.info("manual run: START job=%s", job_key)
    try:
        if has_dates:
            result = run_fn(
                from_date=getattr(args, "from_date", None),
                to_date=getattr(args, "to_date", None),
            )
        else:
            result = run_fn()

        print(json.dumps(result, ensure_ascii=False))
        logger.info("manual run: DONE  job=%s result=%s", job_key, result)
        if isinstance(result, dict) and result.get("errors", 0) > 0:
            sys.exit(1)
    except Exception as exc:
        logger.error("manual run: FAIL  job=%s error=%s", job_key, exc, exc_info=True)
        sys.exit(1)


def cmd_scheduler(_args: argparse.Namespace) -> None:
    """Khởi chạy APScheduler long-running — tương đương `go run ./cmd/scheduler`."""
    from cli.scheduler import main as scheduler_main
    scheduler_main()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="dataops_batch_api",
        description="DataOps Batch API — ETL jobs và scheduler",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ── scheduler ─────────────────────────────────────────────────────────────
    sub.add_parser("scheduler", help="Khởi chạy APScheduler (long-running cron)")

    # ── run <job> ─────────────────────────────────────────────────────────────
    run_parser = sub.add_parser("run", help="Chạy một job thủ công rồi thoát")
    run_parser.add_argument(
        "job_name",
        choices=list(_JOBS),
        metavar="JOB",
        help=f"Tên job: {list(_JOBS)}",
    )
    run_parser.add_argument("--from-date", metavar="YYYY-MM-DD", default=None,
                            help="Backfill từ ngày (chỉ dùng cho binance-1d)")
    run_parser.add_argument("--to-date",   metavar="YYYY-MM-DD", default=None,
                            help="Backfill đến ngày (chỉ dùng cho binance-1d)")

    args = parser.parse_args(argv)

    if args.command == "scheduler":
        cmd_scheduler(args)
    elif args.command == "run":
        cmd_run(args)


if __name__ == "__main__":
    main()
