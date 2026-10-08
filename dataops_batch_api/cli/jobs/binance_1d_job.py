"""
binance_1d_job.py — [Daily 00:05 UTC] Binance 1d OHLCV candles — 10 năm append-only.

Schedule: cron "5 0 * * *" (UTC) — chạy cùng lúc với 4h job.

Chiến lược:
    - Mỗi lần chạy: fetch 1 candle 1d của ngày hôm qua (closed candle).
    - Mode: append — KHÔNG xóa records cũ (append-only, 10 năm lịch sử).
    - Backfill: chạy thủ công với --from-date để lấy dữ liệu lịch sử.

Delta table: s3://binance-datalake/delta/bronze/binance_candles_1d
Partition:   inst_type, symbol, year

Schema: giống binance_candles_4h nhưng interval = "1d"
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List

import pyarrow as pa

from cli.ingestion.schema.models import BinanceCandleRecord
from cli.ingestion.sources.client.binance_client import BinanceSpotClient, BinanceFuturesClient
from cli.jobs._delta_writer import _table_path, write_records
from cli.flows.loader.load_config import load_exchange_config
from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

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

_INTERVAL = "1d"


def _parse_kline_row(row: list, symbol: str, inst_type: str, now_iso: str) -> dict | None:
    try:
        rec = BinanceCandleRecord(
            inst_id   = symbol,
            inst_type = inst_type,
            ts_ms     = int(row[6]),
            open      = float(row[1]),
            high      = float(row[2]),
            low       = float(row[3]),
            close     = float(row[4]),
            vol       = float(row[5]),
            vol_ccy   = float(row[7]),
            confirm   = 1,
        )
    except Exception as exc:
        logger.warning("_parse_kline_row: validation failed symbol=%s: %s", symbol, exc)
        return None

    year = datetime.fromtimestamp(int(row[0]) / 1000, tz=timezone.utc).strftime("%Y")
    return {
        "symbol":       rec.inst_id,
        "inst_type":    rec.inst_type,
        "interval":     _INTERVAL,
        "ts_open_ms":   int(row[0]),
        "ts_close_ms":  rec.ts_ms,
        "open":         rec.open,
        "high":         rec.high,
        "low":          rec.low,
        "close":        rec.close,
        "volume":       rec.vol,
        "quote_volume": rec.vol_ccy,
        "num_trades":   int(row[8]),
        "confirm":      True,
        "year":         year,
        "ingested_at":  now_iso,
    }


def run(from_date: str | None = None, to_date: str | None = None) -> dict:
    """
    Chạy job Binance 1d.

    Args:
        from_date: "YYYY-MM-DD" để backfill. None = chỉ lấy ngày hôm qua.
        to_date:   "YYYY-MM-DD" để backfill. None = ngày hôm qua.

    Returns:
        {"written": int, "errors": int, "symbols_ok": list, "symbols_err": list}
    """
    cfg     = load_exchange_config()
    bucket  = cfg["delta"]["bucket"]
    path    = _table_path(bucket, "bronze", "binance_candles_1d")
    now     = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    if from_date:
        start_ms = int(datetime.fromisoformat(from_date).replace(tzinfo=timezone.utc).timestamp() * 1000)
    else:
        # Ngày hôm qua 00:00 UTC
        yesterday = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_ms = int(yesterday.timestamp() * 1000)

    if to_date:
        end_ms = int(datetime.fromisoformat(to_date).replace(tzinfo=timezone.utc).timestamp() * 1000)
    else:
        end_ms = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)

    # Tính limit dựa trên khoảng thời gian (1 candle/ngày)
    days   = max(1, (end_ms - start_ms) // (86400 * 1000) + 1)
    limit  = min(days + 1, 1000)   # Binance max 1000/request

    spot_symbols    = cfg["binance"]["instruments"]["spot"]
    futures_symbols = cfg["binance"]["instruments"]["futures"]

    all_records: List[dict] = []
    errors, symbols_ok, symbols_err = 0, [], []

    spot_client = BinanceSpotClient.from_env()
    for symbol in spot_symbols:
        try:
            rows = spot_client.get_klines(symbol, interval=_INTERVAL, start_time=start_ms, end_time=end_ms, limit=limit)
            records = [r for row in rows if (r := _parse_kline_row(row, symbol, "SPOT", now_iso))]
            all_records.extend(records)
            symbols_ok.append(f"spot:{symbol}")
            logger.info("binance_1d_job: SPOT %s → %d candles", symbol, len(records))
        except Exception as exc:
            logger.error("binance_1d_job: SPOT %s failed: %s", symbol, exc)
            symbols_err.append(f"spot:{symbol}")
            errors += 1

    futures_client = BinanceFuturesClient.from_env()
    for symbol in futures_symbols:
        try:
            rows = futures_client.get_klines(symbol, interval=_INTERVAL, start_time=start_ms, end_time=end_ms, limit=limit)
            records = [r for row in rows if (r := _parse_kline_row(row, symbol, "FUTURES", now_iso))]
            all_records.extend(records)
            symbols_ok.append(f"futures:{symbol}")
            logger.info("binance_1d_job: FUTURES %s → %d candles", symbol, len(records))
        except Exception as exc:
            logger.error("binance_1d_job: FUTURES %s failed: %s", symbol, exc)
            symbols_err.append(f"futures:{symbol}")
            errors += 1

    written = 0
    if all_records:
        # append-only — KHÔNG gọi apply_rolling_retention
        written = write_records(path, all_records, _SCHEMA, mode="append", partition_by=["inst_type", "symbol", "year"])

    logger.info("binance_1d_job: DONE written=%d errors=%d", written, errors)
    return {"written": written, "errors": errors, "symbols_ok": symbols_ok, "symbols_err": symbols_err}


if __name__ == "__main__":
    import sys
    from_d = sys.argv[1] if len(sys.argv) > 1 else None
    to_d   = sys.argv[2] if len(sys.argv) > 2 else None
    result = run(from_date=from_d, to_date=to_d)
    print(result)
