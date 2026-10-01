"""
backfill_flow.py — Binance Historical Backfill Flow.

Fetch dữ liệu lịch sử Binance cho nhiều instruments song song bằng ThreadPoolExecutor.
Dùng cursor-based pagination (Binance 'endTime' param) để đi ngược từ mới → cũ.

Design:
    - ThreadPoolExecutor(max_workers) cho I/O-bound parallel fetch
    - tqdm progress bar cho terminal visibility
    - Idempotent write: re-run an toàn nhờ partition by year/month/inst_id
    - Retry built-in trong BinanceRestClient (tenacity exponential backoff)
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from tqdm import tqdm

from cli.flows.serving.metadata import get_backfill_cfg, get_binance_cfg, setup_logging
from cli.ingestion.schema.models import BinanceCandleRecord
from cli.ingestion.sources.binance_client import BinanceRestClient
from platforms.processing.config.delta_utils import DeltaBronzeWriter

logger = logging.getLogger(__name__)


class BinanceBackfillPipeline:
    """
    Backfill historical Binance OHLCV data cho danh sách instruments.

    Strategy:
        1. Paginate Binance API bằng 'endTime' cursor (Unix ms) đi ngược từ end → start
        2. Validate từng record qua BinanceCandleRecord Pydantic model
        3. Ghi batch vào Delta Bronze (append + partition → idempotent)
        4. ThreadPoolExecutor: parallel fetch cho nhiều instruments
        5. tqdm: dual progress bar (tổng instruments + candles per instrument)
    """

    def __init__(self, client: BinanceRestClient, writer: DeltaBronzeWriter):
        self.client = client
        self.writer = writer

    def _backfill_one(
        self,
        inst_id: str,
        inst_type: str,
        bar: str,
        start_ms: int,
        end_ms: int,
        candles_per_req: int = 300,
    ) -> int:
        """
        Backfill 1 instrument từ start_ms đến end_ms.
        Dùng cursor-based pagination đi ngược (mới → cũ).

        Returns:
            Số records đã ghi thành công.
        """
        cursor = end_ms
        total  = 0

        with tqdm(desc=f"  {inst_id}", unit="candles", leave=False) as pbar:
            while cursor > start_ms:
                try:
                    raw = self.client.get_candles(
                        inst_id=inst_id,
                        bar=bar,
                        before=cursor,
                        limit=candles_per_req,
                    )
                except Exception as e:
                    logger.error("[backfill] get_candles failed for %s: %s", inst_id, e)
                    break

                if not raw:
                    logger.info(
                        "[backfill] %s: empty response at cursor=%d — stopping pagination",
                        inst_id, cursor,
                    )
                    break

                valid: List[BinanceCandleRecord] = []
                for row in raw:
                    ts_ms = int(row[0])
                    if ts_ms < start_ms:
                        continue
                    try:
                        # Binance klines: [openTime, open, high, low, close, volume,
                        #   closeTime, quoteAssetVolume, ...]
                        valid.append(BinanceCandleRecord(
                            inst_id=inst_id,
                            inst_type=inst_type,
                            ts_ms=ts_ms,
                            open=float(row[1]),
                            high=float(row[2]),
                            low=float(row[3]),
                            close=float(row[4]),
                            vol=float(row[5]),
                            vol_ccy=float(row[7]) if len(row) > 7 else 0.0,
                            confirm=1,
                        ))
                    except Exception as e:
                        logger.warning(
                            "[backfill] Validation failed %s ts=%d: %s",
                            inst_id, int(row[0]), e,
                        )

                if valid:
                    try:
                        self.writer.write_binance_candles(valid, inst_type)
                        total += len(valid)
                        pbar.update(len(valid))
                    except Exception as exc:
                        logger.error(
                            "[backfill] write failed for %s batch at cursor=%d: %s",
                            inst_id, cursor, exc, exc_info=True,
                        )
                        # Tiếp tục pagination thay vì abort toàn bộ instrument

                oldest_ts = int(raw[-1][0])
                cursor    = oldest_ts - 1
                if oldest_ts <= start_ms:
                    break

        return total

    def run(
        self,
        inst_ids: List[str],
        inst_type: str = "SPOT",
        bar: str = "1m",
        days_back: int = 30,
        max_workers: int = 4,
        candles_per_req: int = 300,
    ) -> int:
        """
        Chạy backfill parallel cho nhiều instruments.

        LEARN: ThreadPoolExecutor phù hợp với I/O-bound tasks (HTTP requests).
               ProcessPoolExecutor dùng cho CPU-bound (pandas transform nặng).

        Args:
            inst_ids:       Danh sách instrument IDs
            inst_type:      "SPOT" | "SWAP" | "FUTURES"
            bar:            Candle timeframe "1m","5m","1H","1D"
            days_back:      Số ngày lịch sử cần lấy
            max_workers:    Số worker threads chạy song song
            candles_per_req: Số candles mỗi request (max 300)

        Returns:
            Tổng records đã ghi thành công.
        """
        end_dt   = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days_back)
        end_ms   = int(end_dt.timestamp() * 1000)
        start_ms = int(start_dt.timestamp() * 1000)

        logger.info(
            "[backfill] Starting Binance %s backfill: %d instruments, %d days back, bar=%s, workers=%d",
            inst_type, len(inst_ids), days_back, bar, max_workers,
        )

        total_records = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self._backfill_one,
                    inst_id, inst_type, bar, start_ms, end_ms, candles_per_req,
                ): inst_id
                for inst_id in inst_ids
            }
            with tqdm(total=len(inst_ids), desc="Instruments", unit="inst") as pbar:
                for future in as_completed(futures):
                    inst_id = futures[future]
                    try:
                        count = future.result()
                        total_records += count
                        logger.info("[backfill] OK %s: %d records", inst_id, count)
                    except Exception as e:
                        logger.error("[backfill] FAIL %s: %s", inst_id, e, exc_info=True)
                    finally:
                        pbar.update(1)

        logger.info("[backfill] Complete — total %d records written", total_records)
        return total_records


# ─── Convenience runner ───────────────────────────────────────────────────────

def run_backfill(
    inst_ids: Optional[List[str]] = None,
    inst_type: str = "SPOT",
    bar: str = "1m",
    days_back: Optional[int] = None,
    max_workers: Optional[int] = None,
) -> int:
    """
    Convenience function để gọi từ daily_pipeline.py hoặc CLI.

    Đọc defaults từ exchange_config.yaml; caller có thể override từng param.
    """
    bf_cfg      = get_backfill_cfg()
    binance_cfg = get_binance_cfg()

    resolved_ids: List[str]
    if inst_ids is None:
        key          = "futures" if inst_type == "FUTURES" else "spot"
        resolved_ids = list(binance_cfg.get("instruments", {}).get(key, ["BTCUSDT", "ETHUSDT"]))
    else:
        resolved_ids = inst_ids

    resolved_days: int    = days_back    if days_back    is not None else int(bf_cfg.get("default_days_back", 30))
    resolved_workers: int = max_workers  if max_workers  is not None else int(bf_cfg.get("max_workers", 4))

    client   = BinanceRestClient.from_env()
    writer   = DeltaBronzeWriter.from_env()
    pipeline = BinanceBackfillPipeline(client, writer)

    return pipeline.run(
        inst_ids=resolved_ids,
        inst_type=inst_type,
        bar=bar,
        days_back=resolved_days,
        max_workers=resolved_workers,
        candles_per_req=int(bf_cfg.get("candles_per_request", 300)),
    )


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    setup_logging()

    parser = argparse.ArgumentParser(description="OKX Historical Backfill")
    parser.add_argument("--inst-type", default="SPOT", choices=["SPOT", "SWAP", "FUTURES"])
    parser.add_argument("--bar",       default="1m",   help="Candle timeframe: 1m,5m,1H,1D")
    parser.add_argument("--days",      default=30,     type=int, help="Days to backfill")
    parser.add_argument("--workers",   default=4,      type=int, help="Parallel workers")
    parser.add_argument(
        "--instruments",
        default=None,
        help="Comma-separated instrument IDs (default: from exchange_config.yaml)",
    )
    args = parser.parse_args()

    inst_ids = [s.strip() for s in args.instruments.split(",")] if args.instruments else None
    run_backfill(
        inst_ids=inst_ids,
        inst_type=args.inst_type,
        bar=args.bar,
        days_back=args.days,
        max_workers=args.workers,
    )
