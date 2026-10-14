"""
binance_4h_job.py — [Daily 00:05 UTC] Binance 4h OHLCV candles — 1 năm rolling.

Schedule: cron "5 0 * * *" (UTC) — chạy sau midnight, lấy ngày hôm qua đầy đủ.

Chiến lược:
    - Mỗi lần chạy: fetch candles từ (now - 25h) → now để đảm bảo có đủ 6 candles
      của ngày hôm qua (6 candles × 4h = 24h) kể cả overlap.
    - Mode: append — không overwrite (DeltaLake dedup nếu cần qua MERGE).
    - Retention: 365 ngày rolling → xóa candles cũ hơn 365 ngày.

Delta table: s3://binance-datalake/delta/bronze/binance_candles_4h
Partition:   inst_type, symbol, year (string "2026")

Chỉ ingest Binance Spot (không dùng Futures).

Schema (PyArrow):
    symbol       string
    inst_type    string       "SPOT"
    interval     string       "4h"
    ts_open_ms   int64        candle open time Unix ms
    ts_close_ms  int64        candle close time Unix ms
    open/high/low/close  double
    volume       double       base asset
    quote_volume double       quote asset (USDT) — row[7]
    num_trades   int32
    confirm      bool         always True (REST = closed candle)
    year         string       partition key "2026"
    ingested_at  string       ISO8601 UTC — lineage tracking
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import List

import pyarrow as pa

from cli.ingestion.schema.canonical import from_binance_kline_row
from cli.ingestion.sources.client.binance_client import BinanceSpotClient
from cli.jobs._delta_writer import _table_path, write_records, apply_rolling_retention
from cli.jobs.metrics import push_metrics, record_ingest
from cli.flows.loader.load_config import load_exchange_config
from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

# ── PyArrow schema ─────────────────────────────────────────────────────────────
_SCHEMA = pa.schema([
    pa.field("symbol",       pa.string(),  nullable=False),
    pa.field("inst_type",    pa.string(),  nullable=False),
    pa.field("interval",     pa.string(),  nullable=False),
    pa.field("ts_open_ms",   pa.int64(),   nullable=False),
    pa.field("ts_close_ms",  pa.int64(),   nullable=False),
    pa.field("open",         pa.float64(), nullable=False),
    pa.field("high",         pa.float64(), nullable=False),
    pa.field("low",          pa.float64(), nullable=False),
    pa.field("close",        pa.float64(), nullable=False),
    pa.field("volume",       pa.float64(), nullable=False),
    pa.field("quote_volume", pa.float64(), nullable=False),
    pa.field("num_trades",   pa.int32(),   nullable=False),
    pa.field("confirm",      pa.bool_(),   nullable=False),
    pa.field("year",         pa.string(),  nullable=False),
    pa.field("ingested_at",  pa.string(),  nullable=False),
])

_INTERVAL      = "4h"
_RETENTION_DAYS = 365
_CANDLES_PER_DAY = 6   # 24h / 4h


def _parse_kline_row(row: list, symbol: str, inst_type: str, now_iso: str) -> dict | None:
    """Parse 1 Binance kline row → validated dict. Returns None nếu validation fail."""
    result = from_binance_kline_row(row, symbol, inst_type, _INTERVAL, now_iso)
    if result is None:
        logger.warning("_parse_kline_row: validation failed symbol=%s", symbol)
    return result


def run() -> dict:
    """
    Chạy job Binance 4h. Entry point cho scheduler.

    Returns:
        {"written": int, "errors": int, "symbols_ok": list, "symbols_err": list}
    """
    cfg     = load_exchange_config()
    bucket  = cfg["delta"]["bucket"]
    path    = _table_path(bucket, "bronze", "binance_candles_4h")
    now     = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    # Fetch window: 25h trước → now (lấy dư 1 candle để đảm bảo đủ ngày hôm qua)
    start_ms = int((now - timedelta(hours=25)).timestamp() * 1000)
    end_ms   = int(now.timestamp() * 1000)

    spot_symbols = cfg["binance"]["instruments"]["spot"]

    all_records: List[dict] = []
    errors, symbols_ok, symbols_err = 0, [], []

    # ── Spot ──────────────────────────────────────────────────────────────────
    spot_client = BinanceSpotClient.from_env()
    for symbol in spot_symbols:
        try:
            rows = spot_client.get_klines(
                symbol,
                interval   = _INTERVAL,
                start_time = start_ms,
                end_time   = end_ms,
                limit      = _CANDLES_PER_DAY + 2,
            )
            records = [r for row in rows if (r := _parse_kline_row(row, symbol, "SPOT", now_iso))]
            all_records.extend(records)
            symbols_ok.append(f"spot:{symbol}")
            logger.info("binance_4h_job: SPOT %s → %d candles", symbol, len(records))
        except Exception as exc:
            logger.error("binance_4h_job: SPOT %s failed: %s", symbol, exc)
            symbols_err.append(f"spot:{symbol}")
            errors += 1

    written = 0
    if all_records:
        written = write_records(path, all_records, _SCHEMA, mode="append", partition_by=["inst_type", "symbol", "year"])
        # Rolling retention — xóa candles cũ hơn 1 năm
        apply_rolling_retention(path, date_col="ingested_at", keep_days=_RETENTION_DAYS)

    record_ingest("binance_4h_job", count=written, status="success")
    if errors:
        record_ingest("binance_4h_job", count=errors, status="error")
    push_metrics("binance_4h_job")

    logger.info(
        "binance_4h_job: DONE written=%d errors=%d ok=%s err=%s",
        written, errors, symbols_ok, symbols_err,
    )
    return {"written": written, "errors": errors, "symbols_ok": symbols_ok, "symbols_err": symbols_err}


if __name__ == "__main__":
    try:
        from dotenv import find_dotenv, load_dotenv
        load_dotenv(find_dotenv(usecwd=True), override=False)
    except Exception:
        pass
    result = run()
    print(result)
