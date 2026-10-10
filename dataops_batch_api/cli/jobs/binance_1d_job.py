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

from cli.ingestion.schema.canonical import from_binance_kline_row
from cli.ingestion.sources.client.binance_client import BinanceSpotClient, BinanceFuturesClient
from cli.jobs._delta_writer import _table_path, write_records
from cli.jobs.metrics import push_metrics, record_ingest
from cli.flows.loader.load_config import load_exchange_config
from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

# CloudFront 403 geo-block tại VN nodes (SGN*, HAN*) cho /fapi/v1/klines — bị chặn ở tầng CDN,
# không phải lỗi code. Không tính vào errors counter; log WARN thay vì ERROR.
_GEO_BLOCK_NODES = ("SGN", "HAN")

def _is_geo_blocked(exc: Exception) -> bool:
    """True nếu exception là 403 do CloudFront geo-block (không phải lỗi code)."""
    msg = str(exc)
    return "403" in msg and "CloudFront" in msg

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
    result = from_binance_kline_row(row, symbol, inst_type, _INTERVAL, now_iso)
    if result is None:
        logger.warning("_parse_kline_row: validation failed symbol=%s", symbol)
    return result


_CHUNK_DAYS = 900  # < 1000 max per Binance request, buffer để tránh off-by-one
_MS_PER_DAY = 86_400_000


def _fetch_chunked(client, symbol: str, inst_type: str, start_ms: int, end_ms: int, now_iso: str) -> List[dict]:
    """
    Fetch klines với pagination tự động.
    Binance max 1000 candles/request — với interval 1d, chunk 900 ngày mỗi lần.

    Args:
        client:    BinanceSpotClient hoặc BinanceFuturesClient.
        symbol:    e.g. "BTCUSDT".
        inst_type: "SPOT" | "FUTURES".
        start_ms:  Unix ms — bắt đầu.
        end_ms:    Unix ms — kết thúc (exclusive).
        now_iso:   ISO8601 string cho ingested_at.

    Returns:
        List of validated row dicts.
    """
    records: List[dict] = []
    chunk_start = start_ms
    chunk_size_ms = _CHUNK_DAYS * _MS_PER_DAY

    while chunk_start < end_ms:
        chunk_end = min(chunk_start + chunk_size_ms, end_ms)
        chunk_days = max(1, (chunk_end - chunk_start) // _MS_PER_DAY + 1)
        limit = min(chunk_days + 1, 1000)

        rows = client.get_klines(
            symbol, interval=_INTERVAL,
            start_time=chunk_start,
            end_time=chunk_end,
            limit=limit,
        )
        if not rows:
            break

        chunk_records = [r for row in rows if (r := _parse_kline_row(row, symbol, inst_type, now_iso))]
        records.extend(chunk_records)
        logger.debug(
            "_fetch_chunked: %s %s chunk [%d→%d] → %d candles",
            inst_type, symbol, chunk_start, chunk_end, len(chunk_records),
        )

        # Advance: last candle closeTime + 1ms để tránh overlap
        last_close_ms = int(rows[-1][6])
        chunk_start = last_close_ms + 1
        if len(rows) < limit:
            break  # Binance trả về ít hơn limit → đã hết data

    return records


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

    spot_symbols    = cfg["binance"]["instruments"]["spot"]
    futures_symbols = cfg["binance"]["instruments"]["futures"]

    all_records: List[dict] = []
    errors, symbols_ok, symbols_err = 0, [], []

    spot_client = BinanceSpotClient.from_env()
    for symbol in spot_symbols:
        try:
            records = _fetch_chunked(spot_client, symbol, "SPOT", start_ms, end_ms, now_iso)
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
            records = _fetch_chunked(futures_client, symbol, "FUTURES", start_ms, end_ms, now_iso)
            all_records.extend(records)
            symbols_ok.append(f"futures:{symbol}")
            logger.info("binance_1d_job: FUTURES %s → %d candles", symbol, len(records))
        except Exception as exc:
            if _is_geo_blocked(exc):
                logger.warning(
                    "binance_1d_job: FUTURES %s SKIPPED — CloudFront 403 geo-block (VN node). "
                    "Spot data unaffected.",
                    symbol,
                )
                symbols_err.append(f"futures:{symbol}:geo-blocked")
            else:
                logger.error("binance_1d_job: FUTURES %s failed: %s", symbol, exc)
                symbols_err.append(f"futures:{symbol}")
                errors += 1

    written = 0
    if all_records:
        # append-only — KHÔNG gọi apply_rolling_retention
        written = write_records(path, all_records, _SCHEMA, mode="append", partition_by=["inst_type", "symbol", "year"])

    record_ingest("binance_1d_job", count=written, status="success")
    if errors:
        record_ingest("binance_1d_job", count=errors, status="error")
    push_metrics("binance_1d_job")

    logger.info("binance_1d_job: DONE written=%d errors=%d", written, errors)
    return {"written": written, "errors": errors, "symbols_ok": symbols_ok, "symbols_err": symbols_err}


if __name__ == "__main__":
    import sys
    from pathlib import Path
    try:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).resolve().parents[4] / ".env", override=False)
    except ImportError:
        pass
    from_d = sys.argv[1] if len(sys.argv) > 1 else None
    to_d   = sys.argv[2] if len(sys.argv) > 2 else None
    result = run(from_date=from_d, to_date=to_d)
    print(result)
