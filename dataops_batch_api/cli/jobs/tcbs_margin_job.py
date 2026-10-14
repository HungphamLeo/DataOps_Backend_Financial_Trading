"""
tcbs_margin_job.py — [Daily 15:45 ICT] TCBS margin risk snapshot → Delta Bronze.

Schedule: cron "45 8 * * 1-5" (UTC = 15:45 ICT, T2-T6)

Nguồn: GET /hydros/v1/account/{accountNo}/risk
    (KHÔNG dùng /hydros/v1/margin-limits — endpoint này không tồn tại trong OpenAPI.)

Tài khoản margin lấy từ env TCBS_MARGIN_ACCOUNTS (comma-separated),
fallback config tcbs.margin_accounts. Empty → skip.

Delta table: s3://binance-datalake/delta/bronze/tcbs_margin_risk
Partition:   trading_date (string "YYYY-MM-DD")
Retention:   2 năm (730 ngày)

Schema:
    trading_date       string
    account_no         string
    rtt                double   Actual Rtt
    outstanding        double   Dư nợ gốc
    accrued_interest   double
    due_amount         double
    overdue_amount     double
    total_fee_debt     double
    initial_margin     double
    maintenance_margin double
    liquidation_margin double
    risk_status_code   string
    risk_status_desc   string
    ingested_at        string   ISO8601 UTC
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import pyarrow as pa

from cli.ingestion.sources.client.tcbs_client import TCBSRestClient
from cli.jobs._delta_writer import _table_path, write_records, apply_rolling_retention
from cli.jobs.metrics import push_metrics, record_ingest
from cli.flows.loader.load_config import load_exchange_config
from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

_SCHEMA = pa.schema([
    pa.field("trading_date",       pa.string(),  nullable=False),
    pa.field("account_no",         pa.string(),  nullable=False),
    pa.field("rtt",                pa.float64(), nullable=True),
    pa.field("outstanding",        pa.float64(), nullable=True),
    pa.field("accrued_interest",   pa.float64(), nullable=True),
    pa.field("due_amount",         pa.float64(), nullable=True),
    pa.field("overdue_amount",     pa.float64(), nullable=True),
    pa.field("total_fee_debt",     pa.float64(), nullable=True),
    pa.field("initial_margin",     pa.float64(), nullable=True),
    pa.field("maintenance_margin", pa.float64(), nullable=True),
    pa.field("liquidation_margin", pa.float64(), nullable=True),
    pa.field("risk_status_code",   pa.string(),  nullable=True),
    pa.field("risk_status_desc",   pa.string(),  nullable=True),
    pa.field("ingested_at",        pa.string(),  nullable=False),
])

_RETENTION_DAYS = 730


def _f(value: Any) -> float | None:
    """Ép về float; None nếu thiếu/không hợp lệ."""
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def parse_risk_response(
    resp: Any, account_no: str, trading_date: str, now_iso: str
) -> List[Dict[str, Any]]:
    """
    Chuẩn hóa response /risk (list hoặc {"data": [...]}) → list canonical dict.
    Pure function — không I/O, dễ test.
    """
    if isinstance(resp, dict):
        items = resp.get("data", [resp] if "rtt" in resp else [])
    elif isinstance(resp, list):
        items = resp
    else:
        items = []

    rows: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        policy = item.get("riskPolicy") or {}
        status = item.get("riskStatus") or {}
        rows.append({
            "trading_date":       trading_date,
            "account_no":         str(item.get("accountNo") or account_no),
            "rtt":                _f(item.get("rtt")),
            "outstanding":        _f(item.get("outstanding")),
            "accrued_interest":   _f(item.get("accruedInterest")),
            "due_amount":         _f(item.get("dueAmount")),
            "overdue_amount":     _f(item.get("overdueAmount")),
            "total_fee_debt":     _f(item.get("totalFeeDebt")),
            "initial_margin":     _f(policy.get("initialMargin")),
            "maintenance_margin": _f(policy.get("maintenanceMargin")),
            "liquidation_margin": _f(policy.get("liquidationMargin")),
            "risk_status_code":   status.get("code"),
            "risk_status_desc":   status.get("description"),
            "ingested_at":        now_iso,
        })
    return rows


def _margin_accounts(cfg: dict) -> List[str]:
    env = os.getenv("TCBS_MARGIN_ACCOUNTS", "")
    if env.strip():
        return [a.strip() for a in env.split(",") if a.strip()]
    return list(cfg["tcbs"].get("margin_accounts", []) or [])


def run() -> dict:
    """Chạy job TCBS margin risk snapshot. Returns {"written": int, "errors": int}."""
    cfg          = load_exchange_config()
    bucket       = cfg["delta"]["bucket"]
    path         = _table_path(bucket, "bronze", "tcbs_margin_risk")
    now          = datetime.now(timezone.utc)
    now_iso      = now.isoformat()
    trading_date = now.strftime("%Y-%m-%d")

    accounts = _margin_accounts(cfg)
    if not accounts:
        logger.warning("tcbs_margin_job: no margin accounts configured — skipping")
        return {"written": 0, "errors": 0}

    client = TCBSRestClient.from_env()
    client.authenticate()

    records: List[Dict[str, Any]] = []
    errors = 0
    for acc in accounts:
        try:
            resp = client.get_account_risk(acc)
            rows = parse_risk_response(resp, acc, trading_date, now_iso)
            if not rows:
                logger.warning("tcbs_margin_job: empty risk response for %s", acc)
            records.extend(rows)
        except Exception as exc:
            logger.error("tcbs_margin_job: %s failed: %s", acc, exc)
            errors += 1

    written = 0
    if records:
        written = write_records(
            path, records, _SCHEMA, mode="append", partition_by=["trading_date"],
        )
        apply_rolling_retention(path, date_col="trading_date", keep_days=_RETENTION_DAYS)

    record_ingest("tcbs_margin_job", count=written, status="success")
    if errors:
        record_ingest("tcbs_margin_job", count=errors, status="error")
    push_metrics("tcbs_margin_job")

    logger.info("tcbs_margin_job: DONE written=%d errors=%d accounts=%d",
                written, errors, len(accounts))
    return {"written": written, "errors": errors}


if __name__ == "__main__":
    try:
        from dotenv import find_dotenv, load_dotenv
        load_dotenv(find_dotenv(usecwd=True), override=False)
    except Exception:
        pass
    print(run())
