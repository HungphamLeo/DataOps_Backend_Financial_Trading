"""
backfill_flow.py — OKX Historical Backfill Flow.

Fetch dữ liệu lịch sử OKX cho nhiều instruments song song bằng ThreadPoolExecutor.
Dùng cursor-based pagination (OKX 'before' param) để đi ngược từ mới → cũ.

Design theo OKX_CRYPTO_PLATFORM_DESIGN.md Section 5 (T1):
    - ThreadPoolExecutor(max_workers) cho I/O-bound parallel fetch
    - tqdm progress bar cho terminal visibility
    - Idempotent write: re-run an toàn nhờ partition by year/month/inst_id
    - Retry built-in trong OKXRestClient (tenacity exponential backoff)
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from tqdm import tqdm

from cli.flows.serving.metadata import get_backfill_cfg, get_okx_cfg, setup_logging
from cli.ingestion.schema.models import OKXCandleRecord
from cli.ingestion.sources.okx_client import OKXRestClient
from platforms.processing.config.delta_utils import DeltaBronzeWriter

logger = logging.getLogger("backfill_flow")


class OKXBackfillPipeline:
    """
    Backfill historical OKX OHLCV data cho danh sách instruments.

    Strategy:
        1. Paginate OKX API bằng 'before' cursor (Unix ms) đi ngược từ end → start
        2. Validate từng record qua OKXCandleRecord Pydantic model
        3. Ghi batch vào Delta Bronze (append + partition → idempotent)
        4. ThreadPoolExecutor: parallel fetch cho nhiều instruments
        5. tqdm: dual progress bar (tổng instruments + candles per instrument)
    """

    def __init__(self, client: OKXRestClient, writer: DeltaBronzeWriter):
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
                    logger.error(f"[backfill] get_candles failed for {inst_id}: {e}")
                    break

                if not raw:
                    break

                valid: List[OKXCandleRecord] = []
                for row in raw:
                    ts_ms = int(row[0])
                    if ts_ms < start_ms:
                        continue
                    try:
                        valid.append(OKXCandleRecord(
                            inst_id=inst_id,
                            inst_type=inst_type,
                            ts_ms=ts_ms,
                            open=float(row[1]),
                            high=float(row[2]),
                            low=float(row[3]),
                            close=float(row[4]),
                            vol=float(row[5]),
                            vol_ccy=float(row[6]) if len(row) > 6 else 0.0,
                            confirm=int(row[8]) if len(row) > 8 else 1,
                        ))
                    except Exception as e:
                        logger.warning(f"[backfill] Validation failed {inst_id} ts={int(row[0])}: {e}")

                if valid:
                    self.writer.write_okx_candles(valid, inst_type)
                    total += len(valid)
                    pbar.update(len(valid))

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
            f"[backfill] Starting {inst_type} backfill: {len(inst_ids)} instruments, "
            f"{days_back} days back, bar={bar}, workers={max_workers}"
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
                        logger.info(f"[backfill] ✓ {inst_id}: {count:,} records")
                    except Exception as e:
                        logger.error(f"[backfill] ✗ {inst_id}: {e}", exc_info=True)
                    finally:
                        pbar.update(1)

        logger.info(f"[backfill] Complete — total {total_records:,} records written")
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

    Đọc defaults từ okx_config.yaml; caller có thể override từng param.
    """
    bf_cfg   = get_backfill_cfg()
    okx_cfg  = get_okx_cfg()

    if inst_ids is None:
        key      = "swap" if inst_type == "SWAP" else "spot"
        inst_ids = okx_cfg.get("instruments", {}).get(key, ["BTC-USDT", "ETH-USDT"])
    if days_back is None:
        days_back = bf_cfg.get("default_days_back", 30)
    if max_workers is None:
        max_workers = bf_cfg.get("max_workers", 4)

    client   = OKXRestClient.from_env()
    writer   = DeltaBronzeWriter.from_env()
    pipeline = OKXBackfillPipeline(client, writer)

    return pipeline.run(
        inst_ids=inst_ids,
        inst_type=inst_type,
        bar=bar,
        days_back=days_back,
        max_workers=max_workers,
        candles_per_req=bf_cfg.get("candles_per_request", 300),
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
        help="Comma-separated instrument IDs (default: from okx_config.yaml)",
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
