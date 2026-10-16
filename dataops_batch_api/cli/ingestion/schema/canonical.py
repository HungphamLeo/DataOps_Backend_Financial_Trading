"""
canonical.py — Canonical Data Model Mapper Functions.

Vai trò duy nhất: convert validated source-specific records
→ dict với field names khớp với CanonicalOHLCV / CanonicalTrade
trong dataops_protobuf/market-data/market_data.proto.

⚠️  SOURCE OF TRUTH: dataops_protobuf/market-data/market_data.proto
    Khi thêm/sửa field trong .proto → phải update file này cùng commit.

Layering:
    raw API response
        → Pydantic model (models.py)   ← validate, reject bad data
        → mapper function (canonical.py) ← normalize field names / enums
        → dict (downstream: Delta Lake, gRPC, Redis)

Không chứa Pydantic models, không chứa business-rule validation.
Chỉ chứa pure mapping logic — 1 input type, 1 output dict shape.

Exchange enum values (mirror từ .proto Exchange enum):
    BINANCE         = "BINANCE"
    BINANCE_FUTURES = "BINANCE_FUTURES"
    TCBS_HOSE       = "TCBS_HOSE"
    TCBS_HNX        = "TCBS_HNX"
    TCBS_UPCOM      = "TCBS_UPCOM"
    TCBS_DERIV      = "TCBS_DERIV"

Side enum values (mirror từ .proto Side enum):
    BUY   = "BUY"
    SELL  = "SELL"
    LONG  = "LONG"   # TCBS derivative position "L"
    SHORT = "SHORT"  # TCBS derivative position "S"
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from cli.ingestion.schema.models import (
    BinanceCandleRecord,
    TCBSMarketRecord,
    TCBSBSARecord,
    TCBSIntradayTickRecord,
)

# ── Exchange constants (mirror .proto Exchange enum) ──────────────────────────
EXCHANGE_BINANCE         = "BINANCE"
EXCHANGE_BINANCE_FUTURES = "BINANCE_FUTURES"
EXCHANGE_TCBS_HOSE       = "TCBS_HOSE"
EXCHANGE_TCBS_HNX        = "TCBS_HNX"
EXCHANGE_TCBS_UPCOM      = "TCBS_UPCOM"
EXCHANGE_TCBS_DERIV      = "TCBS_DERIV"

# ── Side constants (mirror .proto Side enum) ──────────────────────────────────
SIDE_BUY   = "BUY"
SIDE_SELL  = "SELL"
SIDE_LONG  = "LONG"
SIDE_SHORT = "SHORT"

# ── InstrumentType constants (mirror .proto InstrumentType enum) ──────────────
INST_SPOT       = "SPOT"
INST_FUTURES    = "FUTURES"
INST_DERIVATIVE = "DERIVATIVE"

# ── TCBS exchange label → canonical exchange ──────────────────────────────────
_TCBS_EXCHANGE_MAP: dict[str, str] = {
    "HOSE":  EXCHANGE_TCBS_HOSE,
    "HNX":   EXCHANGE_TCBS_HNX,
    "UPCOM": EXCHANGE_TCBS_UPCOM,
    "VN30":  EXCHANGE_TCBS_HOSE,   # VN30 basket lives on HOSE
    "VN100": EXCHANGE_TCBS_HOSE,
    "HNXINDEX": EXCHANGE_TCBS_HNX,
}

# ── TCBS intraday action → canonical Side ────────────────────────────────────
# Source: tcbs_constants.yaml section intraday_action_map
_TCBS_ACTION_MAP: dict[str, str] = {
    "Buy Up":    SIDE_BUY,
    "Sell Down": SIDE_SELL,
    # tên viết tắt phòng trường hợp API thay đổi
    "BD":  SIDE_BUY,
    "SD":  SIDE_SELL,
}


def _tcbs_exchange(raw_exchange: str) -> str:
    """Map TCBS basket label → canonical exchange string."""
    return _TCBS_EXCHANGE_MAP.get(raw_exchange.upper(), EXCHANGE_TCBS_HOSE)


def _tcbs_date_to_iso(tcbs_date: str) -> str:
    """
    Chuyển TCBS trading_date "dd/MM/yyyy" → "YYYY-MM-DD".
    Fallback về ngày hôm nay nếu parse lỗi.
    """
    try:
        return datetime.strptime(tcbs_date, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ── Binance mappers ───────────────────────────────────────────────────────────

def from_binance_kline_row(
    row: list,
    symbol: str,
    inst_type: str,
    interval: str,
    now_iso: str,
) -> dict | None:
    """
    Parse 1 Binance kline row → canonical OHLCV dict.

    Row index layout (Binance REST /api/v3/klines):
        row[0]  open_time_ms    (Unix ms)
        row[1]  open
        row[2]  high
        row[3]  low
        row[4]  close
        row[5]  volume          (base asset)
        row[6]  close_time_ms   (Unix ms)
        row[7]  quote_volume    (quoteAssetVolume — NOT row[6])
        row[8]  num_trades

    Returns:
        dict với field names khớp CanonicalOHLCV trong market_data.proto,
        cộng thêm partition/lineage fields (year, ingested_at).
        Returns None nếu Pydantic validation fail.

    Exchange mapping:
        inst_type="SPOT"    → EXCHANGE_BINANCE
        inst_type="FUTURES" → EXCHANGE_BINANCE_FUTURES
    """
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
    except Exception:
        return None

    exchange = EXCHANGE_BINANCE_FUTURES if inst_type == INST_FUTURES else EXCHANGE_BINANCE
    year = datetime.fromtimestamp(int(row[0]) / 1000, tz=timezone.utc).strftime("%Y")

    return {
        # ── Canonical fields (khớp CanonicalOHLCV.proto) ─────────────────
        "symbol":       rec.inst_id,
        "exchange":     exchange,
        "inst_type":    rec.inst_type,
        "interval":     interval,
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
        # ── Partition / lineage fields (Delta Lake) ───────────────────────
        "year":         year,
        "ingested_at":  now_iso,
    }


# ── TCBS mappers ──────────────────────────────────────────────────────────────

def from_tcbs_ticker_record(
    raw: dict,
    fields: dict,
    now_iso: str,
) -> dict | None:
    """
    Map 1 raw TCBS tickerCommons item → canonical EOD snapshot dict.

    Args:
        raw:    Raw dict từ TCBS /tartarus/v1/tickerCommons response.
        fields: TCBSConstants.ticker_commons_fields() — camelCase field mapping.
        now_iso: ISO8601 string cho ingested_at.

    Returns:
        dict với field names cho Delta Bronze table tcbs_market_eod.
        Returns None nếu validation fail.

    Note: close = match_price (giá ATC lúc 15:15 ICT khi job chạy).
    """
    try:
        rec = TCBSMarketRecord(
            symbol           = raw.get(fields.get("symbol", "symbol"), ""),
            exchange         = raw.get("exchange", "HOSE"),
            ref_price        = raw.get(fields.get("ref_price", "refPrice"), 0.0),
            ceil_price       = raw.get(fields.get("ceil_price", "ceilPrice"), 0.0),
            floor_price      = raw.get(fields.get("floor_price", "floorPrice"), 0.0),
            match_price      = raw.get(fields.get("match_price", "matchPrice"), 0.0),
            match_qty        = raw.get(fields.get("match_qty", "matchQtty"), 0.0),
            bid_price_01     = raw.get(fields.get("bid_price_01", "bidPrice01"), 0.0),
            bid_price_02     = raw.get(fields.get("bid_price_02", "bidPrice02"), 0.0),
            bid_price_03     = raw.get(fields.get("bid_price_03", "bidPrice03"), 0.0),
            bid_qty_01       = raw.get(fields.get("bid_qty_01", "bidQtty01"), 0.0),
            bid_qty_02       = raw.get(fields.get("bid_qty_02", "bidQtty02"), 0.0),
            bid_qty_03       = raw.get(fields.get("bid_qty_03", "bidQtty03"), 0.0),
            offer_price_01   = raw.get(fields.get("offer_price_01", "offerPrice01"), 0.0),
            offer_price_02   = raw.get(fields.get("offer_price_02", "offerPrice02"), 0.0),
            offer_price_03   = raw.get(fields.get("offer_price_03", "offerPrice03"), 0.0),
            offer_qty_01     = raw.get(fields.get("offer_qty_01", "offerQtty01"), 0.0),
            offer_qty_02     = raw.get(fields.get("offer_qty_02", "offerQtty02"), 0.0),
            offer_qty_03     = raw.get(fields.get("offer_qty_03", "offerQtty03"), 0.0),
            open             = raw.get(fields.get("open", "open"), 0.0),
            high             = raw.get(fields.get("high", "high"), 0.0),
            low              = raw.get(fields.get("low", "low"), 0.0),
            avg              = raw.get(fields.get("avg", "avg"), 0.0),
            total_vol        = raw.get(fields.get("total_vol", "totalVol"), 0.0),
            total_val        = raw.get(fields.get("total_val", "totalVal"), 0.0),
            buy_foreign_qty  = raw.get(fields.get("buy_foreign_qty", "buyForeignQty"), 0.0),
            sell_foreign_qty = raw.get(fields.get("sell_foreign_qty", "sellForeignQty"), 0.0),
            room             = str(raw.get(fields.get("room", "room"), "")),
            change           = raw.get(fields.get("change", "change"), 0.0),
            change_pct       = raw.get(fields.get("change_pct", "changePercent"), 0.0),
            trading_date     = str(raw.get(fields.get("trading_date", "tradingDate"), "")),
            ts_ms            = int(datetime.now(timezone.utc).timestamp() * 1000),
        )
    except Exception:
        return None

    trading_date = _tcbs_date_to_iso(rec.trading_date)
    buy  = rec.buy_foreign_qty
    sell = rec.sell_foreign_qty

    return {
        "trading_date":     trading_date,
        "symbol":           rec.symbol,
        "exchange":         _tcbs_exchange(rec.exchange),
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


def from_tcbs_bsa_record(
    bsa_points: list,
    ticker: str,
    investor_type: str,
    trading_date: str,
    now_iso: str,
) -> dict | None:
    """
    Aggregate list BSA points trong ngày → 1 canonical daily BSA summary dict.

    Lấy điểm cuối phiên (latest "t" key) làm đại diện cho cả ngày.
    bsr được tính lại từ bup/sdp để tránh outlier từ API.

    Args:
        bsa_points:    List raw BSA dicts từ TCBS /nyx/v1/intraday/{ticker}/bsa.
        ticker:        Mã cổ phiếu, e.g. "VCB".
        investor_type: "sheep" | "wolf" | "shark" | "all".
        trading_date:  "YYYY-MM-DD" — partition key.
        now_iso:       ISO8601 string cho ingested_at.

    Returns:
        dict cho Delta Bronze table tcbs_bsa_daily.
        Returns None nếu không có data hoặc validation fail.
    """
    if not bsa_points:
        return None

    latest = max(bsa_points, key=lambda x: x.get("t", ""), default=None)
    if not latest:
        return None

    try:
        rec = TCBSBSARecord(
            ticker        = ticker,
            investor_type = investor_type,
            bup           = float(latest.get("bup", 0.0)),
            sdp           = float(latest.get("sdp", 0.0)),
            bsr           = float(latest.get("bsr", 0.0)),
            event_time    = str(latest.get("t", "")),
            ts_ms         = int(datetime.now(timezone.utc).timestamp() * 1000),
        )
    except Exception:
        return None

    return {
        "trading_date":  trading_date,
        "ticker":        rec.ticker,
        "investor_type": rec.investor_type,
        "bup":           rec.bup,
        "sdp":           rec.sdp,
        "bsr":           rec.bsr,
        "ingested_at":   now_iso,
    }
