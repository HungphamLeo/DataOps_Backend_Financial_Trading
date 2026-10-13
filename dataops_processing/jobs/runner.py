"""
runner.py — Entry point duy nhất cho tất cả dataops_processing jobs.

Thiết kế:
    - Sử dụng ProcessingOrchestrator (Class Tổng) để điều phối và dispatch job.
    - Một file duy nhất xử lý CLI parsing + logging setup.
    - Job được chọn qua CLI command; args bổ sung truyền tiếp vào Orchestrator.

Commands:
    python -m dataops_processing vwap
    python -m dataops_processing order-flow
    python -m dataops_processing market-breadth [--date YYYY-MM-DD]
    python -m dataops_processing volume-bars
    python -m dataops_processing volume-bars --symbol BTCUSDT

Trong Docker (Dockerfile CMD):
    CMD ["python", "-m", "dataops_processing", "vwap"]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys

from dataops_processing.core.orchestrator import get_orchestrator
from dataops_processing.log.logger_setup import logger_manager, setup_pipeline_logging

logger = logger_manager.get_logger(__name__)


def _setup_logging(level: str = "INFO") -> None:
    """
    Khởi tạo logging từ logger_config.yaml (qua LoggerManager).
    Gọi 1 lần duy nhất tại đầu main().
    Root log level có thể override qua --log-level CLI arg.
    """
    setup_pipeline_logging()
    if level.upper() != "INFO":
        logging.getLogger().setLevel(getattr(logging, level.upper(), logging.INFO))


def main(argv: list[str] | None = None) -> None:
    """
    CLI entry point kết nối với ProcessingOrchestrator.

    Args:
        argv: Argument list (default: sys.argv[1:]).
              Inject trong tests: main(["volume-bars", "--symbol", "BTCUSDT"])
    """
    parser = argparse.ArgumentParser(
        prog="dataops_processing",
        description="DataOps Processing — Spark Streaming & Batch jobs managed by Orchestrator",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )

    subparsers = parser.add_subparsers(dest="job", required=True)

    # ── vwap: Spark Structured Streaming VWAP job ─────────────────────────────
    subparsers.add_parser(
        "vwap",
        help="Start VWAP Spark Structured Streaming job (long-running)",
    )

    # ── order-flow: Spark Structured Streaming Order Flow / CVD job ───────────
    subparsers.add_parser(
        "order-flow",
        help="Start Order Flow / CVD Spark Structured Streaming job (long-running)",
    )

    # ── market-breadth: EOD market breadth & foreign flow batch job ───────────
    mb_parser = subparsers.add_parser(
        "market-breadth",
        help="Run EOD Market Breadth & Foreign Flow batch job and exit",
    )
    mb_parser.add_argument(
        "--date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Trading date to process. Default: latest date in Bronze tcbs_market_eod.",
    )

    # ── volume-bars: Batch volume bars job ────────────────────────────────────
    vb_parser = subparsers.add_parser(
        "volume-bars",
        help="Run Volume Bars batch job and exit",
    )
    vb_parser.add_argument(
        "--symbol",
        type=str,
        default=None,
        metavar="SYMBOL",
        help="Process single symbol (e.g. BTCUSDT). Default: all symbols from config.",
    )

    # ── indicator-matrix: Multi-timeframe indicator matrix job ────────────────
    im_parser = subparsers.add_parser(
        "indicator-matrix",
        help="Run Multi-Timeframe Technical Indicator Matrix batch job (EMA, ATR, RSI, BB)",
    )
    im_parser.add_argument(
        "--symbol",
        type=str,
        default=None,
        metavar="SYMBOL",
        help="Process single symbol (e.g. BTCUSDT, VNINDEX). Default: all symbols from config.",
    )
    im_parser.add_argument(
        "--interval",
        type=str,
        default=None,
        choices=["1d", "1h"],
        help="Process single interval ('1d' or '1h'). Default: all intervals from config.",
    )

    args = parser.parse_args(argv)
    _setup_logging(args.log_level)

    orchestrator = get_orchestrator()

    try:
        if args.job == "vwap":
            orchestrator.run_job("vwap")
        elif args.job == "order-flow":
            orchestrator.run_job("order-flow")
        elif args.job == "market-breadth":
            result = orchestrator.run_job("market-breadth", trading_date=getattr(args, "date", None))
            print(json.dumps(result))
            if result.get("errors", 0) > 0:
                sys.exit(1)
        elif args.job == "volume-bars":
            result = orchestrator.run_job("volume-bars", symbol=getattr(args, "symbol", None))
            print(json.dumps(result))
            if result.get("errors", 0) > 0:
                sys.exit(1)
        elif args.job == "indicator-matrix":
            result = orchestrator.run_job(
                "indicator-matrix",
                symbol=getattr(args, "symbol", None),
                interval=getattr(args, "interval", None),
            )
            print(json.dumps(result))
            if result.get("errors", 0) > 0:
                sys.exit(1)
        else:
            parser.error(f"Unknown job: {args.job}")
    except KeyboardInterrupt:
        logger.info("Job '%s' stopped by user (KeyboardInterrupt)", args.job)
        sys.exit(0)


if __name__ == "__main__":
    main()
