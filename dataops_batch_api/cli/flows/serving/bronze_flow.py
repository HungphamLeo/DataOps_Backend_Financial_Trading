"""
bronze_flow.py — Bronze Ingestion Flow: Binance/TCBS REST → Delta Lake Bronze.

Đây là Stage 1 (T1) trong pipeline:
    Binance REST API → Pydantic Validate → Delta Lake Bronze (MinIO)
    TCBS REST API   → Pydantic Validate → Delta Lake Bronze (MinIO)

Medallion Architecture:
    Bronze (Raw) → Silver (Clean, Spark) → Gold (Aggregated)
    ↑ File này chịu trách nhiệm tầng Bronze.

TCBS Bronze Functions:
    run_tcbs_market_bronze()  — tickerCommons per index (full orderbook + OHLC)
    run_tcbs_bsa_bronze()     — BSA supply/demand analytics per ticker
    run_tcbs_intraday_bronze() — tick-by-tick intraday history per ticker

Idempotency:
    - partitionBy(year, month, exchange/ticker) → re-run an toàn, không tạo duplicate.
    - Binance rate limit: 1200 req/min — enforce qua BinanceRestClient._throttle().
    - TCBS rate limit: không document → dùng batch_size nhỏ + retry backoff.

12-Factor alignment:
    - Factor III: mọi config đọc qua metadata.get_*_cfg(), không hardcode.
    - Factor XI: dùng logger module-level, không print() hay ad-hoc logging.
"""
from __future__ import annotations

import logging
import time
from typing import List, Optional

from cli.flows.serving.metadata import get_binance_cfg, get_tcbs_cfg, setup_logging
from cli.ingestion.schema.models import (
    BinanceCandleRecord,
    TCBSBSARecord,
    TCBSIntradayTickRecord,
    TCBSMarketRecord,
)
from cli.ingestion.sources.binance_client import BinanceRestClient
from cli.ingestion.sources.tcbs_client import TCBSRestClient
from platforms.processing.config.delta_utils import DeltaBronzeWriter

logger = logging.getLogger(__name__)


# ─── Binance Bronze Ingestion ─────────────────────────────────────────────────

def run_binance_bronze(
    inst_ids: Optional[List[str]] = None,
    inst_type: str = "SPOT",
    bar: str = "1m",
    limit: int = 300,
) -> int:
    """
    Fetch Binance candles và ghi vào Delta Bronze.

    Dùng cho incremental daily run (lấy ~300 candles gần nhất).
    Backfill lịch sử dùng backfill_flow.py.

    Args:
        inst_ids:  Danh sách instrument IDs. None → đọc từ exchange_config.yaml.
        inst_type: "SPOT" | "FUTURES" | "PERP"
        bar:       Timeframe, mặc định "1m"
        limit:     Số candles tối đa mỗi instrument (max 1000)

    Returns:
        Tổng số records đã ghi thành công.
    """
    binance_cfg = get_binance_cfg()

    resolved_ids: List[str]
    if inst_ids is None:
        key          = "futures" if inst_type == "FUTURES" else "spot"
        resolved_ids = list(binance_cfg.get("instruments", {}).get(key, ["BTCUSDT", "ETHUSDT"]))
    else:
        resolved_ids = inst_ids

    client  = BinanceRestClient.from_env()
    writer  = DeltaBronzeWriter.from_env()
    total   = 0

    logger.info(
        "[bronze_flow:binance] Starting %s ingestion — %d instruments, bar=%s",
        inst_type, len(resolved_ids), bar,
    )

    for inst_id in resolved_ids:
        try:
            raw = client.get_candles(inst_id=inst_id, bar=bar, limit=limit)
            if not raw:
                logger.warning("[bronze_flow:binance] No data returned for %s", inst_id)
                continue

            records: List[BinanceCandleRecord] = []
            for row in raw:
                try:
                    # Binance klines format: [openTime, open, high, low, close, volume,
                    #   closeTime, quoteAssetVolume, numTrades, takerBuyBaseVol,
                    #   takerBuyQuoteVol, ignore]
                    records.append(BinanceCandleRecord(
                        inst_id=inst_id,
                        inst_type=inst_type,
                        ts_ms=int(row[0]),
                        open=float(row[1]),
                        high=float(row[2]),
                        low=float(row[3]),
                        close=float(row[4]),
                        vol=float(row[5]),
                        vol_ccy=float(row[7]) if len(row) > 7 else 0.0,
                        confirm=1,
                    ))
                except Exception as e:
                    logger.warning("[bronze_flow:binance] Validation failed %s: %s", inst_id, e)

            if records:
                n = writer.write_binance_candles(records, inst_type)
                total += n
                logger.info("[bronze_flow:binance] %s: wrote %d records", inst_id, n)

        except Exception as e:
            logger.error("[bronze_flow:binance] Error fetching %s: %s", inst_id, e, exc_info=True)

    logger.info("[bronze_flow:binance] Done — total %d records written", total)
    return total


# ─── TCBS Bronze Ingestion ─────────────────────────────────────────────────────

def run_tcbs_market_bronze(
    indexes: Optional[List[str]] = None,
    exchange_label: str = "HOSE",
) -> int:
    """
    Fetch TCBS full market snapshots (tickerCommons) và ghi vào Delta Bronze.

    Lấy bảng giá đầy đủ gồm orderbook 3 cấp (bid/offer × 3), OHLC, foreign room
    cho từng index basket được cấu hình trong exchange_config.yaml tcbs.default_indexes.

    Nên chạy mỗi 1 phút trong giờ giao dịch (9:00–14:45 T2-T6).

    Args:
        indexes:        Danh sách index baskets cần lấy, e.g. ["HOSE", "VN30", "HNX"].
                        None → đọc từ tcbs.default_indexes trong exchange_config.yaml.
        exchange_label: Label sàn ghi vào record (dùng cho partition), default "HOSE".
                        Khi fetch nhiều index, label được set theo từng index tương ứng.

    Returns:
        Tổng số records đã ghi thành công.
    """
    tcbs_cfg = get_tcbs_cfg()

    resolved_indexes: List[str]
    if indexes is None:
        resolved_indexes = list(tcbs_cfg.get("default_indexes", ["HOSE"]))
    else:
        resolved_indexes = indexes

    client = TCBSRestClient.from_env()
    writer = DeltaBronzeWriter.from_env()
    total  = 0

    logger.info(
        "[bronze_flow:tcbs_market] Starting market snapshot ingestion — indexes=%s",
        resolved_indexes,
    )

    for index in resolved_indexes:
        try:
            logger.info("[bronze_flow:tcbs_market] Fetching tickerCommons for index=%s", index)
            result = client.get_ticker_commons(index=index)
            raw_data = result.get("data", [])
            trading_date = result.get("tradingDate", "")

            if not raw_data:
                logger.warning(
                    "[bronze_flow:tcbs_market] No data returned for index=%s", index
                )
                continue

            ts_now = int(time.time() * 1000)
            records: List[TCBSMarketRecord] = []

            for item in raw_data:
                try:
                    records.append(TCBSMarketRecord(
                        symbol=item.get("symbol", ""),
                        exchange=index,   # dùng index name làm exchange label
                        ref_price=float(item.get("refPrice", 0) or 0),
                        ceil_price=float(item.get("ceilPrice", 0) or 0),
                        floor_price=float(item.get("floorPrice", 0) or 0),
                        match_price=float(item.get("matchPrice", 0) or 0),
                        match_qty=float(item.get("matchQtty", 0) or 0),
                        # Orderbook 3 cấp
                        bid_price_01=float(item.get("bidPrice01", 0) or 0),
                        bid_price_02=float(item.get("bidPrice02", 0) or 0),
                        bid_price_03=float(item.get("bidPrice03", 0) or 0),
                        bid_qty_01=float(item.get("bidQtty01", 0) or 0),
                        bid_qty_02=float(item.get("bidQtty02", 0) or 0),
                        bid_qty_03=float(item.get("bidQtty03", 0) or 0),
                        offer_price_01=float(item.get("offerPrice01", 0) or 0),
                        offer_price_02=float(item.get("offerPrice02", 0) or 0),
                        offer_price_03=float(item.get("offerPrice03", 0) or 0),
                        offer_qty_01=float(item.get("offerQtty01", 0) or 0),
                        offer_qty_02=float(item.get("offerQtty02", 0) or 0),
                        offer_qty_03=float(item.get("offerQtty03", 0) or 0),
                        # OHLC + avg
                        open=float(item.get("open", 0) or 0),
                        high=float(item.get("high", 0) or 0),
                        low=float(item.get("low", 0) or 0),
                        avg=float(item.get("avg", 0) or 0),
                        # Volume & value
                        total_vol=float(item.get("totalVol", 0) or 0),
                        total_val=float(item.get("totalVal", 0) or 0),
                        # Foreign
                        buy_foreign_qty=float(item.get("buyForeignQtty", 0) or 0),
                        sell_foreign_qty=float(item.get("sellForeignQtty", 0) or 0),
                        room=str(item.get("room", "") or ""),
                        # Change
                        change=float(item.get("change", 0) or 0),
                        change_pct=float(item.get("changePercent", 0) or 0),
                        index_number=float(item.get("indexNumber", 0) or 0) or None,
                        trading_date=trading_date,
                        ts_ms=ts_now,
                    ))
                except Exception as exc:
                    logger.warning(
                        "[bronze_flow:tcbs_market] Validation failed for symbol=%s index=%s: %s",
                        item.get("symbol"), index, exc,
                    )

            if records:
                n = writer.write_tcbs_market(records)
                total += n
                logger.info(
                    "[bronze_flow:tcbs_market] index=%s: wrote %d records, tradingDate=%s",
                    index, n, trading_date,
                )
            else:
                logger.warning(
                    "[bronze_flow:tcbs_market] index=%s: 0 valid records after validation "
                    "(raw_data had %d items)", index, len(raw_data),
                )

        except Exception as exc:
            logger.error(
                "[bronze_flow:tcbs_market] Error fetching index=%s: %s",
                index, exc, exc_info=True,
            )
            # Không dừng — tiếp tục với index tiếp theo

    logger.info("[bronze_flow:tcbs_market] Done — total %d market records written", total)
    return total


def run_tcbs_bsa_bronze(
    tickers: Optional[List[str]] = None,
    investor_type: Optional[str] = None,
) -> int:
    """
    Fetch TCBS Buy/Sell Analysis (BSA) và ghi vào Delta Bronze.

    BSA cung cấp tỷ lệ mua/bán chủ động theo loại nhà đầu tư (sheep/wolf/shark/all)
    — là data độc quyền của TCBS, không có tương đương trong VPS.

    Nên chạy 1 lần/ngày sau khi thị trường đóng cửa để có full-day BSA.

    Args:
        tickers:       Danh sách mã cổ phiếu. None → đọc từ tcbs.bsa_tickers trong yaml.
        investor_type: Loại nhà đầu tư. None → đọc từ tcbs.bsa_investor_type trong yaml.
                       Phải là một trong: sheep | wolf | shark | all.

    Returns:
        Tổng số BSA records đã ghi thành công.
    """
    tcbs_cfg = get_tcbs_cfg()

    resolved_tickers: List[str]
    if tickers is None:
        resolved_tickers = list(tcbs_cfg.get("bsa_tickers", []))
    else:
        resolved_tickers = tickers

    if not resolved_tickers:
        logger.warning(
            "[bronze_flow:tcbs_bsa] No tickers configured. "
            "Set tcbs.bsa_tickers in exchange_config.yaml or pass tickers param."
        )
        return 0

    resolved_investor_type: str = investor_type or tcbs_cfg.get("bsa_investor_type", "all")

    client = TCBSRestClient.from_env()
    writer = DeltaBronzeWriter.from_env()
    total  = 0

    logger.info(
        "[bronze_flow:tcbs_bsa] Starting BSA ingestion — %d tickers, investor_type=%s",
        len(resolved_tickers), resolved_investor_type,
    )

    ts_now = int(time.time() * 1000)

    for ticker in resolved_tickers:
        try:
            logger.debug("[bronze_flow:tcbs_bsa] Fetching BSA for ticker=%s", ticker)
            result = client.get_intraday_bsa(ticker=ticker, investor_type=resolved_investor_type)
            raw_data = result.get("data", [])

            if not raw_data:
                logger.warning(
                    "[bronze_flow:tcbs_bsa] No BSA data returned for ticker=%s", ticker
                )
                continue

            records: List[TCBSBSARecord] = []
            for item in raw_data:
                try:
                    records.append(TCBSBSARecord(
                        ticker=ticker,
                        investor_type=resolved_investor_type,
                        bup=float(item.get("bup", 0) or 0),
                        sdp=float(item.get("sdp", 0) or 0),
                        bsr=float(item.get("bsr", 0) or 0),
                        event_time=str(item.get("t", "") or ""),
                        ts_ms=ts_now,
                    ))
                except Exception as exc:
                    logger.warning(
                        "[bronze_flow:tcbs_bsa] Validation failed for ticker=%s item=%s: %s",
                        ticker, item, exc,
                    )

            if records:
                n = writer.write_tcbs_bsa(records)
                total += n
                logger.info(
                    "[bronze_flow:tcbs_bsa] ticker=%s: wrote %d BSA records",
                    ticker, n,
                )

        except Exception as exc:
            logger.error(
                "[bronze_flow:tcbs_bsa] Error fetching BSA for ticker=%s: %s",
                ticker, exc, exc_info=True,
            )
            # Không dừng — tiếp tục ticker tiếp theo

    logger.info("[bronze_flow:tcbs_bsa] Done — total %d BSA records written", total)
    return total


def run_tcbs_intraday_bronze(
    tickers: Optional[List[str]] = None,
    max_pages: Optional[int] = None,
) -> int:
    """
    Fetch TCBS intraday tick history và ghi vào Delta Bronze.

    Mỗi ticker có thể có hàng nghìn tick records/ngày.
    max_pages giới hạn lượng data để kiểm soát thời gian chạy.

    Nên chạy 1 lần/ngày sau khi thị trường đóng cửa để có full-day tick data.

    Args:
        tickers:   Danh sách mã cổ phiếu. None → đọc từ tcbs.bsa_tickers trong yaml
                   (dùng chung list với BSA).
        max_pages: Số trang tối đa mỗi ticker. None → đọc từ tcbs.intraday_max_pages.

    Returns:
        Tổng số intraday tick records đã ghi thành công.
    """
    tcbs_cfg = get_tcbs_cfg()

    resolved_tickers: List[str]
    if tickers is None:
        # Dùng chung list với BSA vì cùng set mã quan trọng
        resolved_tickers = list(tcbs_cfg.get("bsa_tickers", []))
    else:
        resolved_tickers = tickers

    if not resolved_tickers:
        logger.warning(
            "[bronze_flow:tcbs_intraday] No tickers configured. "
            "Set tcbs.bsa_tickers in exchange_config.yaml or pass tickers param."
        )
        return 0

    resolved_max_pages: int = max_pages if max_pages is not None else int(
        tcbs_cfg.get("intraday_max_pages", 5)
    )

    client = TCBSRestClient.from_env()
    writer = DeltaBronzeWriter.from_env()
    total  = 0

    logger.info(
        "[bronze_flow:tcbs_intraday] Starting intraday tick ingestion — "
        "%d tickers, max_pages=%d",
        len(resolved_tickers), resolved_max_pages,
    )

    ts_now = int(time.time() * 1000)

    for ticker in resolved_tickers:
        try:
            logger.debug(
                "[bronze_flow:tcbs_intraday] Fetching intraday history for ticker=%s "
                "(max_pages=%d)", ticker, resolved_max_pages,
            )
            raw_ticks = client.get_intraday_history_all_pages(
                ticker=ticker,
                max_pages=resolved_max_pages,
            )

            if not raw_ticks:
                logger.warning(
                    "[bronze_flow:tcbs_intraday] No tick data returned for ticker=%s", ticker
                )
                continue

            records: List[TCBSIntradayTickRecord] = []
            for item in raw_ticks:
                try:
                    records.append(TCBSIntradayTickRecord(
                        ticker=ticker,
                        price=float(item.get("p", 0) or 0),
                        volume=float(item.get("v", 0) or 0),
                        price_change=float(item.get("cp", 0) or 0),
                        action=str(item.get("a", "") or ""),
                        buy_acc=float(item.get("ba", 0) or 0),
                        sell_acc=float(item.get("sa", 0) or 0),
                        highlight=bool(item.get("hl", False)),
                        event_time=str(item.get("t", "") or ""),
                        ts_ms=ts_now,
                    ))
                except Exception as exc:
                    logger.warning(
                        "[bronze_flow:tcbs_intraday] Validation failed for ticker=%s item=%s: %s",
                        ticker, item, exc,
                    )

            if records:
                n = writer.write_tcbs_intraday(records)
                total += n
                logger.info(
                    "[bronze_flow:tcbs_intraday] ticker=%s: wrote %d tick records",
                    ticker, n,
                )

        except Exception as exc:
            logger.error(
                "[bronze_flow:tcbs_intraday] Error fetching intraday for ticker=%s: %s",
                ticker, exc, exc_info=True,
            )
            # Không dừng — tiếp tục ticker tiếp theo

    logger.info(
        "[bronze_flow:tcbs_intraday] Done — total %d intraday tick records written", total
    )
    return total


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    setup_logging()
    # Binance
    run_binance_bronze(inst_type="SPOT")
    run_binance_bronze(inst_type="FUTURES")
    # TCBS — đọc tất cả config từ exchange_config.yaml
    run_tcbs_market_bronze()
    run_tcbs_bsa_bronze()
    run_tcbs_intraday_bronze()
