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

from cli.ingestion.schema.models import TCBSMarketRecord
from cli.ingestion.sources.client.tcbs_client import TCBSRestClient
from cli.jobs._delta_writer import _table_path, write_records
from cli.jobs.metrics import push_metrics, record_ingest
from cli.flows.loader.load_config import load_exchange_config
from cli.flows.loader.tcbs_constants_loader import TCBSConstants
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


def _tcbs_date_to_iso(tcbs_date: str) -> str:
    """
    Chuyển TCBS trading_date "dd/MM/yyyy" → "YYYY-MM-DD".
    Fallback về ngày hôm nay nếu parse lỗi.
    """
    try:
        return datetime.strptime(tcbs_date, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _validate_and_map(raw: dict, now_iso: str) -> dict | None:
    """Validate raw tickerCommons item → row dict. Returns None nếu lỗi."""
    try:
        fields = TCBSConstants.ticker_commons_fields()
        rec = TCBSMarketRecord(
            symbol           = raw.get(fields["symbol"], ""),
            exchange         = raw.get("exchange", "HOSE"),
            ref_price        = raw.get(fields["ref_price"], 0.0),
            ceil_price       = raw.get(fields["ceil_price"], 0.0),
            floor_price      = raw.get(fields["floor_price"], 0.0),
            match_price      = raw.get(fields["match_price"], 0.0),
            match_qty        = raw.get(fields["match_qty"], 0.0),
            bid_price_01     = raw.get(fields["bid_price_01"], 0.0),
            bid_price_02     = raw.get(fields["bid_price_02"], 0.0),
            bid_price_03     = raw.get(fields["bid_price_03"], 0.0),
            bid_qty_01       = raw.get(fields["bid_qty_01"], 0.0),
            bid_qty_02       = raw.get(fields["bid_qty_02"], 0.0),
            bid_qty_03       = raw.get(fields["bid_qty_03"], 0.0),
            offer_price_01   = raw.get(fields["offer_price_01"], 0.0),
            offer_price_02   = raw.get(fields["offer_price_02"], 0.0),
            offer_price_03   = raw.get(fields["offer_price_03"], 0.0),
            offer_qty_01     = raw.get(fields["offer_qty_01"], 0.0),
            offer_qty_02     = raw.get(fields["offer_qty_02"], 0.0),
            offer_qty_03     = raw.get(fields["offer_qty_03"], 0.0),
            open             = raw.get(fields["open"], 0.0),
            high             = raw.get(fields["high"], 0.0),
            low              = raw.get(fields["low"], 0.0),
            avg              = raw.get(fields["avg"], 0.0),
            total_vol        = raw.get(fields["total_vol"], 0.0),
            total_val        = raw.get(fields["total_val"], 0.0),
            buy_foreign_qty  = raw.get(fields["buy_foreign_qty"], 0.0),
            sell_foreign_qty = raw.get(fields["sell_foreign_qty"], 0.0),
            room             = str(raw.get(fields["room"], "")),
            change           = raw.get(fields["change"], 0.0),
            change_pct       = raw.get(fields["change_pct"], 0.0),
            trading_date     = str(raw.get(fields["trading_date"], "")),
            ts_ms            = int(datetime.now(timezone.utc).timestamp() * 1000),
        )
    except Exception as exc:
        logger.warning("tcbs_eod_job: validation failed for %s: %s", raw.get("symbol", "?"), exc)
        return None

    trading_date = _tcbs_date_to_iso(rec.trading_date)
    buy  = rec.buy_foreign_qty
    sell = rec.sell_foreign_qty

    return {
        "trading_date":     trading_date,
        "symbol":           rec.symbol,
        "exchange":         rec.exchange,
        "open":             rec.open,
        "high":             rec.high,
        "low":              rec.low,
        "close":            rec.match_price,   # ATC close = match_price lúc 15:15
        "total_vol":        rec.total_vol,
        "total_val":        rec.total_val,
        "buy_foreign_qty":  buy,
        "sell_foreign_qty": sell,
        "net_foreign_qty":  round(buy - sell, 0),
        "change_pct":       rec.change_pct,
        "ref_price":        rec.ref_price,
        "ceil_price":       rec.ceil_price,
        "floor_price":      rec.floor_price,
        "ingested_at":      now_iso,
    }


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
            index_code = TCBSConstants.index_code(index)
            raw_list   = client.get_ticker_commons(index=index_code)
            for raw in raw_list:
                row = _validate_and_map(raw, now_iso)
                if row:
                    all_records.append(row)
                else:
                    errors += 1
            logger.info("tcbs_eod_job: index=%s → %d records", index, len(raw_list))
        except Exception as exc:
            logger.error("tcbs_eod_job: index=%s failed: %s", index, exc)
            errors += len(indexes)  # conservative estimate

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
    result = run()
    print(result)
