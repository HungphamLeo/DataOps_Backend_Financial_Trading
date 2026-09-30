"""
bronze_flow.py — Bronze Ingestion Flow: OKX/VPS REST → Delta Lake Bronze.

Đây là Stage 1 (T1) trong pipeline:
    OKX REST API → Pydantic Validate → Delta Lake Bronze (MinIO)
    VPS REST API → Pydantic Validate → Delta Lake Bronze (MinIO)

Medallion Architecture:
    Bronze (Raw) → Silver (Clean, Spark) → Gold (Aggregated)
    ↑ File này chịu trách nhiệm tầng Bronze.

Idempotency:
    - partitionBy(year, month, inst_id) → re-run an toàn, không tạo duplicate.
    - OKX rate limit: 20 req/2s — enforce qua OKXRestClient._throttle().
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import List, Optional

from cli.flows.serving.metadata import get_okx_cfg, get_vps_cfg, setup_logging
from cli.ingestion.schema.models import OKXCandleRecord, VPSCandleRecord
from cli.ingestion.sources.okx_client import OKXRestClient
from cli.ingestion.sources.vps_client import VPSRestClient
from platforms.processing.config.delta_utils import DeltaBronzeWriter

logger = logging.getLogger("bronze_flow")


# ─── OKX Bronze Ingestion ─────────────────────────────────────────────────────

def run_okx_bronze(
    inst_ids: Optional[List[str]] = None,
    inst_type: str = "SPOT",
    bar: str = "1m",
    limit: int = 300,
) -> int:
    """
    Fetch OKX candles và ghi vào Delta Bronze.

    Dùng cho incremental daily run (lấy ~300 candles gần nhất).
    Backfill lịch sử dùng backfill_flow.py.

    Args:
        inst_ids:  Danh sách instrument IDs. None → đọc từ okx_config.yaml.
        inst_type: "SPOT" | "SWAP" | "FUTURES"
        bar:       Timeframe, mặc định "1m"
        limit:     Số candles tối đa mỗi instrument (max 300)

    Returns:
        Tổng số records đã ghi thành công.
    """
    okx_cfg = get_okx_cfg()
    if inst_ids is None:
        key     = "swap" if inst_type == "SWAP" else "spot"
        inst_ids = okx_cfg.get("instruments", {}).get(key, ["BTC-USDT", "ETH-USDT"])

    client  = OKXRestClient.from_env()
    writer  = DeltaBronzeWriter.from_env()
    total   = 0

    logger.info(f"[bronze_flow:okx] Starting {inst_type} ingestion — {len(inst_ids)} instruments, bar={bar}")

    for inst_id in inst_ids:
        try:
            raw = client.get_candles(inst_id=inst_id, bar=bar, limit=limit)
            if not raw:
                logger.warning(f"[bronze_flow:okx] No data returned for {inst_id}")
                continue

            records: List[OKXCandleRecord] = []
            for row in raw:
                try:
                    # OKX format: [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
                    records.append(OKXCandleRecord(
                        inst_id=inst_id,
                        inst_type=inst_type,
                        ts_ms=int(row[0]),
                        open=float(row[1]),
                        high=float(row[2]),
                        low=float(row[3]),
                        close=float(row[4]),
                        vol=float(row[5]),
                        vol_ccy=float(row[6]) if len(row) > 6 else 0.0,
                        confirm=int(row[8]) if len(row) > 8 else 1,
                    ))
                except Exception as e:
                    logger.warning(f"[bronze_flow:okx] Validation failed {inst_id}: {e}")

            if records:
                n = writer.write_okx_candles(records, inst_type)
                total += n
                logger.info(f"[bronze_flow:okx] {inst_id}: wrote {n} records")

        except Exception as e:
            logger.error(f"[bronze_flow:okx] Error fetching {inst_id}: {e}", exc_info=True)

    logger.info(f"[bronze_flow:okx] Done — total {total} records written")
    return total


# ─── VPS Bronze Ingestion ──────────────────────────────────────────────────────

def run_vps_bronze(exchange: str = "HOSE") -> int:
    """
    Fetch VPS market snapshots và ghi vào Delta Bronze.

    VPS không có historical data API → chỉ lấy snapshot hiện tại.
    Để accumulate: chạy script này mỗi 1 phút trong giờ giao dịch (9:00–14:45).

    Args:
        exchange: Sàn giao dịch — "HOSE" (scope hiện tại, HNX/UPCOM out of scope)

    Returns:
        Số snapshot records đã ghi thành công.
    """
    vps_cfg = get_vps_cfg()
    exchange = vps_cfg.get("exchange", exchange)

    client = VPSRestClient.from_env()
    writer = DeltaBronzeWriter.from_env()

    logger.info(f"[bronze_flow:vps] Fetching stock list for {exchange}...")
    stocks   = client.get_stock_list(exchange)
    symbols  = [s["stockCode"] for s in stocks if s.get("stockCode")]
    logger.info(f"[bronze_flow:vps] Found {len(symbols)} symbols on {exchange}")

    batch_size = vps_cfg.get("symbols_per_batch", 200)
    raw_data   = client.get_market_data(symbols, batch_size=batch_size)

    ts_now   = int(time.time() * 1000)
    records: List[VPSCandleRecord] = []

    for item in raw_data:
        try:
            records.append(VPSCandleRecord(
                symbol=item.get("stockCode", ""),
                exchange=exchange,
                price=float(item.get("matchPrice", 0) or 0),
                open=float(item.get("openPrice", 0) or 0),
                high=float(item.get("highPrice", 0) or 0),
                low=float(item.get("lowPrice", 0) or 0),
                reference_price=float(item.get("referencePrice", 0) or 0),
                ceiling_price=float(item.get("ceilPrice", 0) or 0),
                floor_price=float(item.get("floorPrice", 0) or 0),
                volume=float(item.get("totalTradingVolume", 0) or 0),
                total_value=float(item.get("totalTradingValue", 0) or 0),
                change_pct=float(item.get("changePercent", 0) or 0),
                session_state=item.get("currentSession", "UNKNOWN"),
                ts_ms=ts_now,
            ))
        except Exception as e:
            logger.warning(f"[bronze_flow:vps] Validation failed for {item.get('stockCode')}: {e}")

    n = writer.write_vps_snapshots(records)
    logger.info(f"[bronze_flow:vps] Done — wrote {n} snapshot records")
    return n


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    setup_logging()
    run_okx_bronze(inst_type="SPOT")
    run_okx_bronze(inst_type="SWAP")
    run_vps_bronze(exchange="HOSE")
