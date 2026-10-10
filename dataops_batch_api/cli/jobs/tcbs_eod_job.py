"""
tcbs_eod_job.py — [Daily 15:15 ICT] TCBS market EOD snapshot — 10 năm append-only.

Schedule: cron "15 8 * * 1-5" (UTC = 15:15 ICT, T2-T6 only)
  → Chạy SAU khi phiên ATC kết thúc (ATC đóng lúc 14:45 ICT, settlement 15:00 ICT)
  → 15:15 ICT đảm bảo toàn bộ ATC đã khớp xong

Chiến lược:
    - Fetch tickerCommons cho tất cả symbols trong batch 50/request
    - Validate qua TCBSMarketRecord
    - Ghi 1 record/symbol/ngày — đây là "nến 1D" của VN stock
    - Mode: append-only (10 năm lịch sử, không xóa)

Delta table: s3://binance-datalake/delta/bronze/tcbs_market_eod
Partition:   exchange, trading_date (string "YYYY-MM-DD")

Schema:
    trading_date    string    "YYYY-MM-DD" — partition key
    symbol          string    "VCB"
    exchange        string    "HOSE" | "HNX" | "UPCOM"
    open            double    giá ATO
    high            double    cao nhất phiên
    low             double    thấp nhất phiên
    close           double    giá ATC / match cuối
    total_vol       double    tổng KL
    total_val       double    tổng GT (VND)
    buy_foreign_qty double    KL NN mua
    sell_foreign_qty double   KL NN bán
    net_foreign_qty double    = buy - sell
    change_pct      double    % thay đổi
    ref_price       double    giá tham chiếu ngày đó
    ceil_price      double    giá trần
    floor_price     double    giá sàn
    ingested_at     string    ISO8601 UTC
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pyarrow as pa

from cli.ingestion.schema.canonical import from_tcbs_ticker_record
from cli.ingestion.sources.client.tcbs_client import TCBSRestClient
from cli.ingestion.sources.tcbs_constants_loader import TCBSConstants
from cli.jobs._delta_writer import _table_path, write_records
from cli.jobs.metrics import push_metrics, record_ingest
from cli.flows.loader.load_config import load_exchange_config
from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

_SCHEMA = pa.schema([
    pa.field("trading_date",     pa.string(),  nullable=False),
    pa.field("symbol",           pa.string(),  nullable=False),
    pa.field("exchange",         pa.string(),  nullable=False),
    pa.field("open",             pa.float64(), nullable=True),
    pa.field("high",             pa.float64(), nullable=True),
    pa.field("low",              pa.float64(), nullable=True),
    pa.field("close",            pa.float64(), nullable=True),
    pa.field("total_vol",        pa.float64(), nullable=True),
    pa.field("total_val",        pa.float64(), nullable=True),
    pa.field("buy_foreign_qty",  pa.float64(), nullable=True),
    pa.field("sell_foreign_qty", pa.float64(), nullable=True),
    pa.field("net_foreign_qty",  pa.float64(), nullable=True),
    pa.field("change_pct",       pa.float64(), nullable=True),
    pa.field("ref_price",        pa.float64(), nullable=True),
    pa.field("ceil_price",       pa.float64(), nullable=True),
    pa.field("floor_price",      pa.float64(), nullable=True),
    pa.field("ingested_at",      pa.string(),  nullable=False),
])


def _validate_and_map(raw: dict, now_iso: str) -> dict | None:
    """Validate raw tickerCommons item → canonical row dict. Returns None nếu lỗi."""
    fields = TCBSConstants.ticker_commons_fields()
    result = from_tcbs_ticker_record(raw, fields, now_iso)
    if result is None:
        logger.warning("tcbs_eod_job: validation failed for %s", raw.get("symbol", "?"))
    return result


def run() -> dict:
    """
    Chạy job TCBS EOD snapshot.

    Returns:
        {"written": int, "errors": int, "records_ok": int, "records_err": int}
    """
    cfg     = load_exchange_config()
    bucket  = cfg["delta"]["bucket"]
    path    = _table_path(bucket, "bronze", "tcbs_market_eod")
    now     = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    batch_size = cfg["tcbs"].get("symbols_per_batch", 50)
    indexes    = cfg["tcbs"].get("default_indexes", ["HOSE", "VN30", "HNX"])

    client = TCBSRestClient.from_env()
    client.authenticate()

    all_records: List[dict] = []
    errors = 0

    for index in indexes:
        try:
            raw_resp = client.get_ticker_commons(index=index)
            if isinstance(raw_resp, dict):
                raw_list = raw_resp.get("data", [])
            elif isinstance(raw_resp, list):
                raw_list = raw_resp
            else:
                raw_list = []

            for raw in raw_list:
                row = _validate_and_map(raw, now_iso)
                if row:
                    all_records.append(row)
                else:
                    errors += 1
            logger.info("tcbs_eod_job: index=%s → %d records", index, len(raw_list))
        except Exception as exc:
            logger.error("tcbs_eod_job: index=%s failed: %s", index, exc)
            errors += 1

    written = 0
    if all_records:
        # append-only — 10 năm, không rolling
        written = write_records(
            path, all_records, _SCHEMA,
            mode="append",
            partition_by=["exchange", "trading_date"],
        )

    record_ingest("tcbs_eod_job", count=written, status="success")
    if errors:
        record_ingest("tcbs_eod_job", count=errors, status="error")
    push_metrics("tcbs_eod_job")

    logger.info("tcbs_eod_job: DONE written=%d errors=%d", written, errors)
    return {"written": written, "errors": errors, "records_ok": written, "records_err": errors}


if __name__ == "__main__":
    from pathlib import Path
    try:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).resolve().parents[4] / ".env", override=False)
    except ImportError:
        pass
    result = run()
    print(result)
