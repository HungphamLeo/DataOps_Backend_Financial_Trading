"""
canonical.py — Canonical Data Model (CDM) cho DataOps Platform (Python mirror).

⚠️  SOURCE OF TRUTH là dataops_protobuf/market-data/market_data.proto
    File này là MANUAL MIRROR của các enums trong .proto cho Python runtime.
    Khi thêm/sửa enum value:
      1. Sửa market_data.proto TRƯỚC
      2. Cập nhật file này để mirror đúng
      3. Cập nhật dataops_stream_producer/pkg/producer/types.go (Go mirror)

    Khi có protoc codegen → file này sẽ được replace bằng generated code.

Mục tiêu:
    Đây là lớp "ngôn ngữ chung" giữa tất cả data sources (Binance, TCBS, VPS).
    Mỗi source nói chuyện theo cách riêng — Binance dùng "1m", TCBS dùng "1 minute",
    Binance order status "CANCELED", TCBS order status "3" (cũng là hủy).
    Layer này chuẩn hóa tất cả về một format duy nhất.

Pattern sử dụng:
    1. Source adapter đọc raw response → gọi hàm map_* → trả về canonical type
    2. Downstream (Delta Lake, InfluxDB, API response) chỉ làm việc với canonical type
    3. Khi TCBS đổi format → chỉ sửa mapper của TCBS, không đụng vào downstream

Design:
    - Enums: dùng Python Enum để type-safe, IDE autocomplete, exhaustive check
    - CanonicalOHLCV: chuẩn hóa OHLCV từ Binance (1m → 1min) và TCBS (aggregate)
    - CanonicalTrade: chuẩn hóa trade tick từ Binance @trade và TCBS intraday
    - CanonicalOrderStatus: map TCBS "3"/"C" và Binance "CANCELED" → CANCELLED
    - CanonicalOrderSide: map TCBS "NB"/"B" và Binance "BUY" → BUY
    - CanonicalInstrumentType: map TCBS "STOCK"/"DERIV" và Binance "SPOT"/"FUTURES" → ...
    - Mapper functions: from_binance_*() và from_tcbs_*() cho từng type

Usage example:
    from cli.ingestion.schema.canonical import (
        CanonicalOHLCV, CanonicalInterval,
        from_binance_kline_row, from_tcbs_intraday_tick,
    )

    # Binance kline row (raw list)
    ohlcv = from_binance_kline_row(row, symbol="BTCUSDT", inst_type="SPOT")
    # ohlcv.interval == CanonicalInterval.MIN_1

    # TCBS intraday tick (raw dict)
    trade = from_tcbs_intraday_tick(tick, ticker="VCB")
    # trade.side == CanonicalSide.BUY
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — CANONICAL ENUMS
# Đây là "bảng từ điển" chung. Tất cả source phải map vào đây.
# ══════════════════════════════════════════════════════════════════════════════

class CanonicalInterval(str, Enum):
    """
    Canonical timeframe/interval enum.

    Problem này giải quyết:
        Binance API:  "1m", "5m", "1h", "1d"
        TCBS internal: "1 minute", "5 minutes", "1 hour" (khi aggregate ticks)
        InfluxDB query: "1m", "5m"
        Delta Lake partition: "1min", "5min"

    Canonical form dùng format ngắn nhất (Binance-style) vì phổ biến nhất.
    """
    SEC_1  = "1s"
    MIN_1  = "1m"
    MIN_3  = "3m"
    MIN_5  = "5m"
    MIN_15 = "15m"
    MIN_30 = "30m"
    HOUR_1 = "1h"
    HOUR_2 = "2h"
    HOUR_4 = "4h"
    HOUR_6 = "6h"
    HOUR_8 = "8h"
    HOUR_12= "12h"
    DAY_1  = "1d"
    DAY_3  = "3d"
    WEEK_1 = "1w"
    MONTH_1= "1M"

    @classmethod
    def from_binance(cls, raw: str) -> "CanonicalInterval":
        """Map Binance interval string → CanonicalInterval. 1:1 mapping."""
        try:
            return cls(raw)
        except ValueError:
            raise ValueError(f"Unknown Binance interval: {raw!r}. Valid: {[e.value for e in cls]}")

    @classmethod
    def from_tcbs_internal(cls, raw: str) -> "CanonicalInterval":
        """
        Map TCBS internal interval label → CanonicalInterval.
        TCBS không có interval param — đây là labels khi aggregate intraday ticks.
        """
        _MAP: Dict[str, str] = {
            "1 second":   "1s",
            "1 minute":   "1m",
            "5 minutes":  "5m",
            "15 minutes": "15m",
            "30 minutes": "30m",
            "1 hour":     "1h",
            "1 day":      "1d",
            # viết tắt cũng chấp nhận
            "1min": "1m", "5min": "5m", "15min": "15m",
            "30min": "30m", "1hour": "1h", "1day": "1d",
        }
        normalized = _MAP.get(raw.lower(), raw)
        try:
            return cls(normalized)
        except ValueError:
            raise ValueError(f"Unknown TCBS interval: {raw!r}")


class CanonicalSide(str, Enum):
    """
    Canonical order/trade side enum.

    Problem này giải quyết:
        Binance trade tick:    m=false → BUY, m=true → SELL
        Binance order:         side="BUY" | "SELL"
        TCBS stock order:      execType="NB" → BUY, "NS" → SELL
        TCBS deriv order:      side="B" → BUY, "S" → SELL
        TCBS deriv position:   side="L" → LONG, "S" → SHORT
        TCBS intraday action:  "Buy Up" → BUY, "Sell Down" → SELL
    """
    BUY  = "BUY"
    SELL = "SELL"
    LONG  = "LONG"    # Derivative long position
    SHORT = "SHORT"   # Derivative short position

    @classmethod
    def from_binance_is_buyer_maker(cls, is_buyer_maker: bool) -> "CanonicalSide":
        """Binance @trade stream field 'm': false = taker buy → BUY, true = taker sell → SELL."""
        return cls.SELL if is_buyer_maker else cls.BUY

    @classmethod
    def from_binance_order_side(cls, raw: str) -> "CanonicalSide":
        """Binance order API field 'side': 'BUY' | 'SELL'."""
        mapping = {"BUY": cls.BUY, "SELL": cls.SELL}
        if raw not in mapping:
            raise ValueError(f"Unknown Binance order side: {raw!r}")
        return mapping[raw]

    @classmethod
    def from_tcbs_exec_type(cls, raw: str) -> "CanonicalSide":
        """TCBS akhlys field 'execType': 'NB' → BUY, 'NS' → SELL."""
        mapping = {"NB": cls.BUY, "NS": cls.SELL}
        if raw not in mapping:
            raise ValueError(f"Unknown TCBS execType: {raw!r}. Valid: NB (buy), NS (sell)")
        return mapping[raw]

    @classmethod
    def from_tcbs_deriv_order_side(cls, raw: str) -> "CanonicalSide":
        """TCBS khronos order field 'side': 'B' → BUY, 'S' → SELL."""
        mapping = {"B": cls.BUY, "S": cls.SELL}
        if raw not in mapping:
            raise ValueError(f"Unknown TCBS derivative order side: {raw!r}. Valid: B, S")
        return mapping[raw]

    @classmethod
    def from_tcbs_deriv_position_side(cls, raw: str) -> "CanonicalSide":
        """TCBS khronos portfolio field 'side': 'L' → LONG, 'S' → SHORT."""
        mapping = {"L": cls.LONG, "S": cls.SHORT}
        if raw not in mapping:
            raise ValueError(f"Unknown TCBS position side: {raw!r}. Valid: L (long), S (short)")
        return mapping[raw]

    @classmethod
    def from_tcbs_intraday_action(cls, raw: str) -> "CanonicalSide":
        """TCBS intraday tick field 'a': 'Buy Up' → BUY, 'Sell Down' → SELL."""
        mapping = {"Buy Up": cls.BUY, "Sell Down": cls.SELL}
        if raw not in mapping:
            raise ValueError(f"Unknown TCBS intraday action: {raw!r}. Valid: 'Buy Up', 'Sell Down'")
        return mapping[raw]


class CanonicalOrderStatus(str, Enum):
    """
    Canonical order status enum.

    Problem này giải quyết:
        Binance:   "NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED", ...
        TCBS stock (orStatus): "0"=REJECTED, "2"=SENT, "3"=CANCELLED, "4"=FILLED,
                               "8"=PENDING, "12"=FILLED_ALL, "C"=CANCELLING, ...
        TCBS deriv condition: status "0"=pending, "1"=activated, "2"=canceled, "3"=rejected

    Note: "CANCELLED" (double L) được dùng làm canonical — không phải "CANCELED" (Binance-style).
    Lý do: tiêu chuẩn quốc tế (ISO) dùng "CANCELLED"; Binance là exception.
    """
    PENDING          = "PENDING"          # Chờ xử lý (chưa gửi sàn)
    SENT             = "SENT"             # Đã gửi tới sàn
    SENDING          = "SENDING"          # Đang gửi tới sàn
    PARTIALLY_FILLED = "PARTIALLY_FILLED" # Khớp một phần
    FILLED           = "FILLED"           # Khớp hết
    CANCELLED        = "CANCELLED"        # Đã hủy (dùng double L — canonical form)
    CANCELLING       = "CANCELLING"       # Đang hủy
    REJECTED         = "REJECTED"         # Bị từ chối
    EXPIRED          = "EXPIRED"          # Hết hạn
    MODIFIED         = "MODIFIED"         # Đã sửa
    MODIFYING        = "MODIFYING"        # Đang sửa
    ACTIVATED        = "ACTIVATED"        # Conditional order đã kích hoạt
    UNKNOWN          = "UNKNOWN"          # Không xác định (fallback)

    @classmethod
    def from_binance(cls, raw: str) -> "CanonicalOrderStatus":
        """
        Map Binance order status string → CanonicalOrderStatus.
        Binance dùng CANCELED (1 L) → map về CANCELLED (2 L).
        """
        mapping = {
            "NEW":              cls.PENDING,
            "PARTIALLY_FILLED": cls.PARTIALLY_FILLED,
            "FILLED":           cls.FILLED,
            "CANCELED":         cls.CANCELLED,    # Note: 1 L → canonical 2 L
            "PENDING_CANCEL":   cls.CANCELLING,
            "REJECTED":         cls.REJECTED,
            "EXPIRED":          cls.EXPIRED,
        }
        if raw not in mapping:
            return cls.UNKNOWN
        return mapping[raw]

    @classmethod
    def from_tcbs_or_status(cls, raw: str) -> "CanonicalOrderStatus":
        """
        Map TCBS orStatus code (string digits + letters) → CanonicalOrderStatus.
        Dùng cho /aion/v1/accounts/{acct}/orders/{orderID}[].orStatus
        """
        mapping = {
            "0":  cls.REJECTED,
            "2":  cls.SENT,
            "3":  cls.CANCELLED,
            "4":  cls.FILLED,
            "5":  cls.EXPIRED,
            "8":  cls.PENDING,
            "10": cls.MODIFIED,
            "11": cls.SENDING,
            "12": cls.FILLED,      # FILLED_ALL → treat as FILLED
            "A":  cls.MODIFYING,
            "C":  cls.CANCELLING,
            "S":  cls.FILLED,      # COMPLETED → treat as FILLED
        }
        if raw not in mapping:
            return cls.UNKNOWN
        return mapping[raw]

    @classmethod
    def from_tcbs_deriv_status(cls, raw: str) -> "CanonicalOrderStatus":
        """
        Map TCBS derivative order status (conditional orders).
        Dùng cho /khronos/v1/order/condition/detail[].status
        """
        mapping = {
            "0": cls.PENDING,
            "1": cls.ACTIVATED,
            "2": cls.CANCELLED,
            "3": cls.REJECTED,
        }
        if raw not in mapping:
            return cls.UNKNOWN
        return mapping[raw]


class CanonicalInstrumentType(str, Enum):
    """
    Canonical instrument type enum.

    Problem này giải quyết:
        Binance:  "SPOT", "FUTURES" (fapi domain), "COINM" (dapi domain)
        TCBS:     Cổ phiếu cơ sở → "STOCK"; Phái sinh VN30F → "DERIVATIVE"
        Internal: cần 1 type duy nhất để partition Delta Lake và filter queries
    """
    SPOT       = "SPOT"        # Cổ phiếu cơ sở (Binance spot, TCBS stock)
    FUTURES    = "FUTURES"     # Futures hợp đồng tương lai (Binance USDM)
    PERPETUAL  = "PERPETUAL"   # Perpetual swap (Binance, nếu dùng)
    DERIVATIVE = "DERIVATIVE"  # Phái sinh VN30F (TCBS)
    COINM      = "COINM"       # Binance Coin-M futures

    @classmethod
    def from_binance(cls, raw: str) -> "CanonicalInstrumentType":
        """Map Binance inst_type → CanonicalInstrumentType."""
        mapping = {
            "SPOT":    cls.SPOT,
            "FUTURES": cls.FUTURES,
            "PERP":    cls.PERPETUAL,
            "COINM":   cls.COINM,
        }
        if raw.upper() not in mapping:
            raise ValueError(f"Unknown Binance instrument type: {raw!r}")
        return mapping[raw.upper()]

    @classmethod
    def from_tcbs(cls, raw: str) -> "CanonicalInstrumentType":
        """Map TCBS instrument category → CanonicalInstrumentType."""
        mapping = {
            "STOCK":      cls.SPOT,
            "DERIVATIVE": cls.DERIVATIVE,
            "DERIV":      cls.DERIVATIVE,
            "VN30F":      cls.DERIVATIVE,  # prefix-based fallback
        }
        key = raw.upper()
        if key in mapping:
            return mapping[key]
        if key.startswith("VN30F"):
            return cls.DERIVATIVE
        raise ValueError(f"Unknown TCBS instrument type: {raw!r}")


class CanonicalExchange(str, Enum):
    """
    Canonical exchange/market identifier.

    Dùng để phân biệt nguồn dữ liệu khi lưu Delta Lake và query.
    """
    BINANCE      = "BINANCE"
    BINANCE_FUTURES = "BINANCE_FUTURES"
    TCBS_HOSE    = "TCBS_HOSE"
    TCBS_HNX     = "TCBS_HNX"
    TCBS_UPCOM   = "TCBS_UPCOM"
    TCBS_DERIV   = "TCBS_DERIV"
    UNKNOWN      = "UNKNOWN"

    @classmethod
    def from_tcbs_exchange_name(cls, raw: str) -> "CanonicalExchange":
        """Map TCBS exchange string → CanonicalExchange."""
        mapping = {
            "HOSE":  cls.TCBS_HOSE,
            "HNX":   cls.TCBS_HNX,
            "UPCOM": cls.TCBS_UPCOM,
            "DERIV": cls.TCBS_DERIV,
        }
        return mapping.get(raw.upper(), cls.UNKNOWN)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — CANONICAL DATA MODELS (Normalized output structures)
# Đây là output format chung — downstream code chỉ dùng các class này.
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class CanonicalOHLCV:
    """
    Canonical OHLCV candle — format chung cho Binance và TCBS.

    Tất cả OHLCV data phải được convert về type này trước khi:
        - Ghi vào Delta Lake Bronze/Silver
        - Ghi vào InfluxDB
        - Trả về qua REST API

    Fields:
        symbol:       Mã chuẩn — Binance: "BTCUSDT", TCBS: "VCB"
        exchange:     CanonicalExchange enum
        inst_type:    CanonicalInstrumentType enum
        interval:     CanonicalInterval enum — luôn là "1m", "5m"... không phải "1 minute"
        ts_open_ms:   Unix ms — open time của candle
        ts_close_ms:  Unix ms — close time của candle
        open/high/low/close: float > 0
        volume:       Base asset volume (BTC cho BTCUSDT, shares cho VCB)
        quote_volume: Quote asset volume (USDT cho BTCUSDT, VND cho VCB) — optional
        num_trades:   Số lệnh khớp trong candle — optional
        confirm:      True = candle đã đóng; False = đang live
        source:       "binance" | "tcbs" — để trace lineage
    """
    symbol:       str
    exchange:     CanonicalExchange
    inst_type:    CanonicalInstrumentType
    interval:     CanonicalInterval
    ts_open_ms:   int
    ts_close_ms:  int
    open:         float
    high:         float
    low:          float
    close:        float
    volume:       float
    quote_volume: float = 0.0
    num_trades:   int   = 0
    confirm:      bool  = True
    source:       str   = ""

    def __post_init__(self) -> None:
        if self.high < self.low:
            raise ValueError(f"[{self.symbol}] high={self.high} < low={self.low}")
        if self.open <= 0 or self.close <= 0:
            raise ValueError(f"[{self.symbol}] open/close must be > 0")


@dataclass
class CanonicalTrade:
    """
    Canonical trade tick — format chung cho Binance @trade và TCBS intraday tick.

    Dùng để:
        - Publish vào Kafka (sau khi producer serialize về Avro/JSON)
        - VWAP calculation trong dataops_processing
        - Buy/sell volume accumulation

    Fields:
        symbol:      Mã chuẩn
        exchange:    CanonicalExchange
        trade_id:    ID của lệnh khớp (string để tránh overflow với TCBS)
        price:       Giá khớp
        quantity:    Khối lượng khớp (base unit)
        side:        CanonicalSide.BUY | SELL
        ts_ms:       Unix ms — thời điểm khớp lệnh
        is_confirm:  True nếu đây là tick đã confirm (không phải partial update)
        source:      "binance" | "tcbs"
    """
    symbol:     str
    exchange:   CanonicalExchange
    trade_id:   str
    price:      float
    quantity:   float
    side:       CanonicalSide
    ts_ms:      int
    is_confirm: bool = True
    source:     str  = ""


@dataclass
class CanonicalOrder:
    """
    Canonical order record — format chung cho Binance và TCBS orders.

    Dùng để:
        - Audit log ghi vào MySQL
        - Trả về qua /api/v1/*/account/orders endpoint
        - Real-time order tracking qua WebSocket push

    Fields:
        order_id:     Unique order ID (string — Binance int, TCBS string)
        account_no:   Số tài khoản
        exchange:     CanonicalExchange
        symbol:       Mã giao dịch
        inst_type:    CanonicalInstrumentType
        side:         CanonicalSide (BUY/SELL)
        order_type:   "LIMIT" | "MARKET" | "STOP_LOSS" | "CONDITION" | ...
        price:        Giá đặt lệnh
        quantity:     Số lượng đặt
        filled_qty:   Số lượng đã khớp
        status:       CanonicalOrderStatus
        ts_placed_ms: Unix ms — lúc đặt lệnh
        ts_updated_ms: Unix ms — lúc cập nhật status gần nhất
        source:       "binance" | "tcbs_stock" | "tcbs_derivative"
        raw:          Dict raw response từ source — để debug, không dùng cho business logic
    """
    order_id:      str
    account_no:    str
    exchange:      CanonicalExchange
    symbol:        str
    inst_type:     CanonicalInstrumentType
    side:          CanonicalSide
    order_type:    str
    price:         float
    quantity:      float
    filled_qty:    float
    status:        CanonicalOrderStatus
    ts_placed_ms:  int
    ts_updated_ms: int
    source:        str               = ""
    raw:           Dict[str, Any]    = field(default_factory=dict)


@dataclass
class CanonicalPosition:
    """
    Canonical derivative position — format chung cho TCBS và Binance Futures.

    Dùng để:
        - Portfolio dashboard display
        - P&L calculation
        - Risk management (margin check)

    Fields:
        account_no:    Số tài khoản
        exchange:      CanonicalExchange
        symbol:        Contract code (e.g. "VN30F2411", "BTCUSDT")
        side:          CanonicalSide.LONG | SHORT
        net_qty:       Net position (absolute value; sign thể hiện qua side)
        entry_price:   Giá vào vị thế bình quân
        last_price:    Giá thị trường hiện tại
        unrealized_pnl: Lãi/lỗ chưa thực hiện
        margin_used:   Margin đang sử dụng
        ts_ms:         Unix ms — lúc lấy snapshot
        source:        "tcbs" | "binance_futures"
    """
    account_no:      str
    exchange:        CanonicalExchange
    symbol:          str
    side:            CanonicalSide
    net_qty:         float
    entry_price:     float
    last_price:      float
    unrealized_pnl:  float
    margin_used:     float
    ts_ms:           int
    source:          str = ""


@dataclass
class CanonicalAccountBalance:
    """
    Canonical account balance snapshot — format chung cho TCBS và Binance.

    Fields:
        account_no:       Số tài khoản
        exchange:         CanonicalExchange
        cash_balance:     Số dư tiền mặt (sẵn dùng)
        total_balance:    Tổng tài sản (cash + holdings market value)
        purchasing_power: Sức mua khả dụng (có thể khác cash_balance do margin)
        currency:         "VND" | "USDT" | "USD"
        ts_ms:            Unix ms — lúc lấy snapshot
        source:           "tcbs" | "binance"
    """
    account_no:       str
    exchange:         CanonicalExchange
    cash_balance:     float
    total_balance:    float
    purchasing_power: float
    currency:         str   = "VND"
    ts_ms:            int   = 0
    source:           str   = ""


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — MAPPER FUNCTIONS
# Chuyển đổi raw API response → CanonicalX model.
# Đây là nơi DUY NHẤT chứa logic "Binance dùng 'm' field, TCBS dùng 'a' field".
# Khi source đổi format → chỉ sửa hàm mapper tương ứng.
# ══════════════════════════════════════════════════════════════════════════════

def from_binance_kline_row(
    row: list,
    symbol: str,
    inst_type: str = "SPOT",
    interval: str  = "1m",
) -> CanonicalOHLCV:
    """
    Convert Binance kline raw row (list) → CanonicalOHLCV.

    Args:
        row:       Raw row từ /api/v3/klines (list of 12 elements).
                   Index mapping: 0=open_time, 1=open, 4=close, 5=vol, 6=close_time, 7=quote_vol
        symbol:    e.g. "BTCUSDT"
        inst_type: "SPOT" | "FUTURES" — để chọn CanonicalExchange
        interval:  Binance interval string, e.g. "1m"

    Returns:
        CanonicalOHLCV với confirm=True (Binance REST klines đều là closed candles)

    Raises:
        ValueError: nếu row length < 12 hoặc OHLC không hợp lệ
    """
    if len(row) < 12:
        raise ValueError(f"Binance kline row too short: {len(row)} < 12")

    exchange = (
        CanonicalExchange.BINANCE_FUTURES
        if inst_type.upper() == "FUTURES"
        else CanonicalExchange.BINANCE
    )

    return CanonicalOHLCV(
        symbol       = symbol,
        exchange     = exchange,
        inst_type    = CanonicalInstrumentType.from_binance(inst_type),
        interval     = CanonicalInterval.from_binance(interval),
        ts_open_ms   = int(row[0]),
        ts_close_ms  = int(row[6]),
        open         = float(row[1]),
        high         = float(row[2]),
        low          = float(row[3]),
        close        = float(row[4]),
        volume       = float(row[5]),
        quote_volume = float(row[7]),   # quoteAssetVolume — QUAN TRỌNG: row[7] không phải row[6]
        num_trades   = int(row[8]),
        confirm      = True,            # REST klines luôn là closed candles
        source       = "binance",
    )


def from_binance_trade_message(
    msg: Dict[str, Any],
    exchange: CanonicalExchange = CanonicalExchange.BINANCE,
) -> CanonicalTrade:
    """
    Convert Binance @trade WebSocket message data payload → CanonicalTrade.

    Args:
        msg: msg["data"] từ combined stream message (sau khi unwrap envelope).
             Expected fields: e, E, s, t, p, q, T, m
        exchange: BINANCE hoặc BINANCE_FUTURES

    Returns:
        CanonicalTrade với side được decode từ isBuyerMaker bool.
    """
    return CanonicalTrade(
        symbol     = msg["s"],
        exchange   = exchange,
        trade_id   = str(msg["t"]),                      # int64 → string
        price      = float(msg["p"]),
        quantity   = float(msg["q"]),
        side       = CanonicalSide.from_binance_is_buyer_maker(bool(msg["m"])),
        ts_ms      = int(msg["T"]),
        is_confirm = True,
        source     = "binance",
    )


def from_tcbs_intraday_tick(
    tick: Dict[str, Any],
    ticker: str,
    exchange: CanonicalExchange = CanonicalExchange.TCBS_HOSE,
    ts_ms: int = 0,
) -> CanonicalTrade:
    """
    Convert TCBS intraday tick raw dict → CanonicalTrade.

    Args:
        tick:     Raw dict từ /nyx/v1/intraday/{ticker}/his/paging data[].
                  Fields: p (price), v (volume), a (action), t (time string)
        ticker:   Stock symbol e.g. "VCB"
        exchange: Exchange enum
        ts_ms:    Unix ms timestamp (nếu đã parse từ event_time + trading_date)

    Returns:
        CanonicalTrade với side được decode từ field "a" ("Buy Up" | "Sell Down").
    """
    return CanonicalTrade(
        symbol     = ticker,
        exchange   = exchange,
        trade_id   = "",                                  # TCBS không có trade ID trong intraday tick
        price      = float(tick.get("p", 0)),
        quantity   = float(tick.get("v", 0)),
        side       = CanonicalSide.from_tcbs_intraday_action(tick.get("a", "Buy Up")),
        ts_ms      = ts_ms,
        is_confirm = True,
        source     = "tcbs",
    )


def from_tcbs_stock_order(
    order: Dict[str, Any],
    account_no: str,
    exchange: CanonicalExchange = CanonicalExchange.TCBS_HOSE,
    ts_ms: int = 0,
) -> CanonicalOrder:
    """
    Convert TCBS /aion stock order dict → CanonicalOrder.

    Args:
        order:      Raw dict từ /aion/v1/accounts/{acct}/orders[].
                    Fields: orderId, symbol, execType, price, quantity, status
        account_no: Account number
        exchange:   Exchange enum
        ts_ms:      Snapshot timestamp

    Returns:
        CanonicalOrder với status mapped từ TCBS string status code.
    """
    return CanonicalOrder(
        order_id      = str(order.get("orderId", "")),
        account_no    = account_no,
        exchange      = exchange,
        symbol        = order.get("symbol", ""),
        inst_type     = CanonicalInstrumentType.SPOT,
        side          = CanonicalSide.from_tcbs_exec_type(order.get("execType", "NB")),
        order_type    = "LIMIT",               # TCBS stock default — no explicit order type field
        price         = float(order.get("price", 0)),
        quantity      = float(order.get("quantity", 0)),
        filled_qty    = 0.0,                   # not available in list endpoint
        status        = CanonicalOrderStatus.UNKNOWN,  # list endpoint không có detailed status
        ts_placed_ms  = ts_ms,
        ts_updated_ms = ts_ms,
        source        = "tcbs_stock",
        raw           = order,
    )


def from_tcbs_deriv_position(
    pos: Dict[str, Any],
    account_id: str,
    sub_account_id: str,
    ts_ms: int = 0,
) -> CanonicalPosition:
    """
    Convert TCBS /khronos portfolio position dict → CanonicalPosition.

    Args:
        pos:            Raw dict từ /khronos/v1/account/portfolio/status data[].
                        Fields: symbol, side (L/S), net, lastPrice, vmValue, mrValue
        account_id:     Custody account
        sub_account_id: Derivative sub-account
        ts_ms:          Snapshot timestamp

    Returns:
        CanonicalPosition với side mapped từ "L"/"S".
    """
    return CanonicalPosition(
        account_no     = sub_account_id,
        exchange       = CanonicalExchange.TCBS_DERIV,
        symbol         = pos.get("symbol", ""),
        side           = CanonicalSide.from_tcbs_deriv_position_side(pos.get("side", "L")),
        net_qty        = abs(float(pos.get("net", 0))),
        entry_price    = 0.0,         # /khronos portfolio không trả về avg entry price
        last_price     = float(pos.get("lastPrice", 0)),
        unrealized_pnl = float(pos.get("vmValue", 0)),
        margin_used    = float(pos.get("mrValue", 0)),
        ts_ms          = ts_ms,
        source         = "tcbs",
    )


def from_tcbs_account_cash(
    response_item: Dict[str, Any],
    account_no: str,
    ts_ms: int = 0,
) -> CanonicalAccountBalance:
    """
    Convert TCBS /aion cashInvestments response item → CanonicalAccountBalance.

    Args:
        response_item: Single item từ response data[].
                       Fields: bodBalance, cashBalance, pp0forBF, accountNo
        account_no:    Account number
        ts_ms:         Snapshot timestamp

    Returns:
        CanonicalAccountBalance với cash_balance và purchasing_power.
    """
    return CanonicalAccountBalance(
        account_no       = response_item.get("accountNo", account_no),
        exchange         = CanonicalExchange.TCBS_HOSE,
        cash_balance     = float(response_item.get("cashBalance", 0)),
        total_balance    = float(response_item.get("bodBalance", 0)),
        purchasing_power = float(response_item.get("pp0forBF", 0)),
        currency         = "VND",
        ts_ms            = ts_ms,
        source           = "tcbs",
    )
