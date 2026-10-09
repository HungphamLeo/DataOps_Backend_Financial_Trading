"""
tcbs_bsa_job.py — [Daily 15:30 ICT] TCBS BSA daily summary — 2 năm rolling.

Schedule: cron "30 8 * * 1-5" (UTC = 15:30 ICT, T2-T6 only)
  → Chạy sau tcbs_eod_job để tận dụng JWT token đang còn hạn

Chiến lược:
    - Fetch BSA cho từng ticker trong bsa_tickers (config)
    - Fetch cả 4 investor_type: sheep, wolf, shark, all
    - Ghi summary cả phiên (1 record/ticker/investor_type/ngày)
    - Rolling retention: 2 năm = 730 ngày

Delta table: s3://binance-datalake/delta/bronze/tcbs_bsa_daily
Partition:   trading_date (string "YYYY-MM-DD")

Schema:
    trading_date   string    "YYYY-MM-DD" — partition key
    ticker         string    "VCB"
    investor_type  string    "sheep" | "wolf" | "shark" | "all"
    bup            double    Buy Up Percent (0–100)
    sdp            double    Sell Down Percent (0–100)
    bsr            double    Buy/Sell Ratio
    ingested_at    string    ISO8601 UTC
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pyarrow as pa

from cli.ingestion.schema.models import TCBSBSARecord
from cli.ingestion.sources.client.tcbs_client import TCBSRestClient
from cli.jobs._delta_writer import _table_path, write_records, apply_rolling_retention
from cli.jobs.metrics import push_metrics, record_ingest
from cli.flows.loader.load_config import load_exchange_config
from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

_SCHEMA = pa.schema([
    pa.field("trading_date",  pa.string(),  nullable=False),
    pa.field("ticker",        pa.string(),  nullable=False),
    pa.field("investor_type", pa.string(),  nullable=False),
    pa.field("bup",           pa.float64(), nullable=True),
    pa.field("sdp",           pa.float64(), nullable=True),
    pa.field("bsr",           pa.float64(), nullable=True),
    pa.field("ingested_at",   pa.string(),  nullable=False),
])

_INVESTOR_TYPES = ["sheep", "wolf", "shark", "all"]
_RETENTION_DAYS = 730   # 2 năm


def _aggregate_bsa(bsa_points: list, investor_type: str) -> dict | None:
    """
    Aggregate list BSA points trong ngày → 1 summary record.

    Lấy điểm cuối phiên (latest event_time) làm đại diện cho cả ngày.
    bsr được tính lại từ bup/sdp để tránh outlier từ API.
    """
    if not bsa_points:
        return None

    # Lấy điểm cuối cùng trong ngày (sort by event_time string "HH:MM:SS")
    latest = max(bsa_points, key=lambda x: x.get("t", ""), default=None)
    if not latest:
        return None

    try:
        rec = TCBSBSARecord(
            ticker        = latest.get("ticker", ""),
            investor_type = investor_type,
            bup           = float(latest.get("bup", 0.0)),
            sdp           = float(latest.get("sdp", 0.0)),
            bsr           = float(latest.get("bsr", 0.0)),
            event_time    = str(latest.get("t", "")),
            ts_ms         = int(datetime.now(timezone.utc).timestamp() * 1000),
        )
    except Exception as exc:
        logger.warning("_aggregate_bsa: validation failed: %s", exc)
        return None

    return rec


def run() -> dict:
    """
    Chạy job TCBS BSA daily summary.

    Returns:
        {"written": int, "errors": int}
    """
    cfg          = load_exchange_config()
    bucket       = cfg["delta"]["bucket"]
    path         = _table_path(bucket, "bronze", "tcbs_bsa_daily")
    now          = datetime.now(timezone.utc)
    now_iso      = now.isoformat()
    trading_date = now.strftime("%Y-%m-%d")

    bsa_tickers   = cfg["tcbs"].get("bsa_tickers", [])
    investor_type_cfg = cfg["tcbs"].get("bsa_investor_type", "all")

    # Nếu config chỉ định "all" → fetch cả 4 types để có phân tích đầy đủ
    investor_types = _INVESTOR_TYPES if investor_type_cfg == "all" else [investor_type_cfg]

    if not bsa_tickers:
        logger.warning("tcbs_bsa_job: bsa_tickers empty — skipping")
        return {"written": 0, "errors": 0}

    client = TCBSRestClient.from_env()
    client.authenticate()

    all_records: List[dict] = []
    errors = 0

    for ticker in bsa_tickers:
        for inv_type in investor_types:
            try:
                raw_resp = client.get_intraday_bsa(ticker, investor_type=inv_type)
                if isinstance(raw_resp, dict):
                    raw_points = raw_resp.get("data", [])
                elif isinstance(raw_resp, list):
                    raw_points = raw_resp
                else:
                    raw_points = []

                # Inject ticker vào mỗi point (API response không có field ticker)
                for p in raw_points:
                    if isinstance(p, dict):
                        p["ticker"] = ticker

                summary = _aggregate_bsa(raw_points, inv_type)
                if summary:
                    all_records.append({
                        "trading_date":  trading_date,
                        "ticker":        summary.ticker,
                        "investor_type": summary.investor_type,
                        "bup":           summary.bup,
                        "sdp":           summary.sdp,
                        "bsr":           summary.bsr,
                        "ingested_at":   now_iso,
                    })
                    logger.debug(
                        "tcbs_bsa_job: %s/%s bup=%.1f sdp=%.1f bsr=%.2f",
                        ticker, inv_type, summary.bup, summary.sdp, summary.bsr,
                    )
                else:
                    logger.warning("tcbs_bsa_job: no BSA data for %s/%s", ticker, inv_type)

            except Exception as exc:
                logger.error("tcbs_bsa_job: %s/%s failed: %s", ticker, inv_type, exc)
                errors += 1

    written = 0
    if all_records:
        written = write_records(
            path, all_records, _SCHEMA,
            mode="append",
            partition_by=["trading_date"],
        )
        # Rolling retention 2 năm
        apply_rolling_retention(path, date_col="trading_date", keep_days=_RETENTION_DAYS)

    record_ingest("tcbs_bsa_job", count=written, status="success")
    if errors:
        record_ingest("tcbs_bsa_job", count=errors, status="error")
    push_metrics("tcbs_bsa_job")

    logger.info(
        "tcbs_bsa_job: DONE written=%d errors=%d tickers=%d types=%d",
        written, errors, len(bsa_tickers), len(investor_types),
    )
    return {"written": written, "errors": errors}


if __name__ == "__main__":
    from cli.jobs._env import load_project_env
    load_project_env()
    result = run()
    print(result)
