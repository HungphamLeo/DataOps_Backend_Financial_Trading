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

12-Factor alignment:
    - Factor III: mọi config đọc qua metadata.get_*_cfg(), không hardcode.
    - Factor XI: dùng logger module-level, không print() hay ad-hoc logging.
"""
from __future__ import annotations

import logging
import time
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
        inst_ids:  Danh sách instrument IDs. None → đọc từ exchange_config.yaml.
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

    logger.info(
        "[bronze_flow:okx] Starting %s ingestion — %d instruments, bar=%s",
        inst_type, len(inst_ids), bar,
    )

    for inst_id in inst_ids:
        try:
            raw = client.get_candles(inst_id=inst_id, bar=bar, limit=limit)
            if not raw:
                logger.warning("[bronze_flow:okx] No data returned for %s", inst_id)
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
                    logger.warning("[bronze_flow:okx] Validation failed %s: %s", inst_id, e)

            if records:
                n = writer.write_okx_candles(records, inst_type)
                total += n
                logger.info("[bronze_flow:okx] %s: wrote %d records", inst_id, n)

        except Exception as e:
            logger.error("[bronze_flow:okx] Error fetching %s: %s", inst_id, e, exc_info=True)

    logger.info("[bronze_flow:okx] Done — total %d records written", total)
    return total


# ─── VPS Bronze Ingestion ──────────────────────────────────────────────────────

def run_vps_bronze(exchange: str = None) -> int:
    """
    Fetch VPS market snapshots và ghi vào Delta Bronze.

    VPS không có historical data API → chỉ lấy snapshot hiện tại.
    Để accumulate: chạy script này mỗi 1 phút trong giờ giao dịch (9:00–14:45).

    Exchange đọc từ exchange_config.yaml (vps.exchange) → không hardcode tại đây.
    Caller có thể override bằng param nếu cần.

    Args:
        exchange: Override sàn giao dịch. None → đọc từ vps.exchange trong yaml.

    Returns:
        Số snapshot records đã ghi thành công. 0 nếu có lỗi nghiêm trọng.
    """
    vps_cfg  = get_vps_cfg()
    exchange = exchange or vps_cfg.get("exchange", "HOSE")

    client = VPSRestClient.from_env()
    writer = DeltaBronzeWriter.from_env()

    # ── Lấy danh sách cổ phiếu ────────────────────────────────────────────────
    logger.info("[bronze_flow:vps] Fetching stock list for %s...", exchange)
    try:
        stocks = client.get_stock_list(exchange)
    except Exception as exc:
        logger.error(
            "[bronze_flow:vps] Failed to get stock list for %s: %s",
            exchange, exc, exc_info=True,
        )
        return 0

    symbols = [s["stockCode"] for s in stocks if s.get("stockCode")]
    logger.info("[bronze_flow:vps] Found %d symbols on %s", len(symbols), exchange)

    if not symbols:
        logger.warning("[bronze_flow:vps] Empty symbol list — aborting")
        return 0

    # ── Lấy market data ───────────────────────────────────────────────────────
    batch_size = vps_cfg.get("symbols_per_batch", 200)
    try:
        raw_data = client.get_market_data(symbols, batch_size=batch_size)
    except Exception as exc:
        logger.error(
            "[bronze_flow:vps] Failed to get market data: %s", exc, exc_info=True,
        )
        return 0

    ts_now  = int(time.time() * 1000)
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
        except Exception as exc:
            logger.warning(
                "[bronze_flow:vps] Validation failed for %s: %s",
                item.get("stockCode"), exc,
            )

    n = writer.write_vps_snapshots(records)
    logger.info("[bronze_flow:vps] Done — wrote %d snapshot records", n)
    return n


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    setup_logging()
    run_okx_bronze(inst_type="SPOT")
    run_okx_bronze(inst_type="SWAP")
    run_vps_bronze()   # exchange đọc từ exchange_config.yaml
