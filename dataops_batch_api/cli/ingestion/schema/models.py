"""
models.py — Pydantic Schemas Validation cho Delta Lake Bronze/Silver.

Validate dữ liệu tại runtime TRƯỚC khi ghi vào Delta Lake.
Nếu dữ liệu vi phạm business rule → raise ValidationError (không ghi record xấu).

Design principle: "Bad data in = bad data out"
  → Validate ở tầng ingestion, không phải tầng analytics.

Schemas:
    BinanceCandleRecord  — OHLCV candle từ Binance REST API
    TCBSMarketRecord     — Full ticker snapshot từ TCBS tickerCommons API
    TCBSTickerSnapRecord — Foreign room + lightweight snapshot từ TCBS tickerSnaps API
    TCBSBSARecord        — Buy/Sell Analysis (cung/cầu) từ TCBS Nyx service
    TCBSIntradayTickRecord — Tick-by-tick khớp lệnh từ TCBS intraday history API
    OHLCVRecord          — Generic normalized OHLCV
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class BinanceCandleRecord(BaseModel):
    """
    Validated OHLCV record từ Binance REST API.

    Fields:
        inst_id:   Instrument ID, e.g. "BTCUSDT", "ETHUSDT"
        inst_type: Loại instrument — SPOT | FUTURES | PERP
        ts_ms:     Unix timestamp milliseconds (close time của candle)
        open/high/low/close: OHLC prices — phải > 0
        vol:       Volume tính theo base currency
        vol_ccy:   Volume tính theo quote currency (USDT)
        confirm:   1 = candle đã đóng hoàn toàn, 0 = đang trong progress
    """
    inst_id:   str
    inst_type: str           # SPOT | SWAP | FUTURES
    ts_ms:     int
    open:      float = Field(gt=0)
    high:      float = Field(gt=0)
    low:       float = Field(gt=0)
    close:     float = Field(gt=0)
    vol:       float = Field(ge=0)
    vol_ccy:   float = Field(ge=0, default=0.0)
    confirm:   int   = Field(default=1, ge=0, le=1)

    @field_validator("high")
    @classmethod
    def high_gte_low(cls, v: float, info) -> float:
        """Business rule: high price phải >= low price."""
        if "low" in info.data and v < info.data["low"]:
            raise ValueError(f"high {v} < low {info.data['low']}")
        return v


# ─── TCBS Schemas ─────────────────────────────────────────────────────────────

class TCBSMarketRecord(BaseModel):
    """
    Validated full market ticker snapshot từ TCBS /tartarus/v1/tickerCommons.

    Chứa orderbook 3 bước giá (bid/offer × 3), OHLC, foreign room, average.
    Chứa đầy đủ orderbook 3 cấp, OHLC, foreign room.

    Fields:
        symbol:          Mã cổ phiếu, e.g. "VCB", "VHM"
        exchange:        Tên sàn — "HOSE" | "HNX" | "UPCOM"
        ref_price:       Giá tham chiếu
        ceil_price:      Giá trần
        floor_price:     Giá sàn
        match_price:     Giá khớp lệnh hiện tại
        match_qty:       Khối lượng khớp lệnh
        bid_price_01..03 / bid_qty_01..03: Bảng chào mua 3 cấp
        offer_price_01..03 / offer_qty_01..03: Bảng chào bán 3 cấp
        open/high/low/avg: OHLC + avg price trong phiên
        total_vol:       Tổng khối lượng giao dịch
        total_val:       Tổng giá trị giao dịch
        buy_foreign_qty: Khối lượng nước ngoài mua
        sell_foreign_qty: Khối lượng nước ngoài bán
        room:            Room nước ngoài còn lại
        change:          Thay đổi giá tuyệt đối so với tham chiếu
        change_pct:      Thay đổi giá phần trăm
        trading_date:    Ngày giao dịch (string từ API, e.g. "dd/MM/yyyy")
        ts_ms:           Unix timestamp milliseconds lúc snapshot
    """
    symbol:           str
    exchange:         str = "HOSE"

    ref_price:        float = Field(ge=0, default=0.0)
    ceil_price:       float = Field(ge=0, default=0.0)
    floor_price:      float = Field(ge=0, default=0.0)
    match_price:      float = Field(ge=0, default=0.0)
    match_qty:        float = Field(ge=0, default=0.0)

    # Orderbook 3 bước giá — ge=0 vì có thể rỗng (giá = 0 khi không có lệnh)
    bid_price_01:     float = Field(ge=0, default=0.0)
    bid_price_02:     float = Field(ge=0, default=0.0)
    bid_price_03:     float = Field(ge=0, default=0.0)
    bid_qty_01:       float = Field(ge=0, default=0.0)
    bid_qty_02:       float = Field(ge=0, default=0.0)
    bid_qty_03:       float = Field(ge=0, default=0.0)

    offer_price_01:   float = Field(ge=0, default=0.0)
    offer_price_02:   float = Field(ge=0, default=0.0)
    offer_price_03:   float = Field(ge=0, default=0.0)
    offer_qty_01:     float = Field(ge=0, default=0.0)
    offer_qty_02:     float = Field(ge=0, default=0.0)
    offer_qty_03:     float = Field(ge=0, default=0.0)

    open:             float = Field(ge=0, default=0.0)
    high:             float = Field(ge=0, default=0.0)
    low:              float = Field(ge=0, default=0.0)
    avg:              float = Field(ge=0, default=0.0)

    total_vol:        float = Field(ge=0, default=0.0)
    total_val:        float = Field(ge=0, default=0.0)
    buy_foreign_qty:  float = Field(ge=0, default=0.0)
    sell_foreign_qty: float = Field(ge=0, default=0.0)
    room:             str   = ""

    change:           float = 0.0
    change_pct:       float = 0.0
    index_number:     Optional[float] = None

    trading_date:     str   = ""
    ts_ms:            int

    @field_validator("match_price")
    @classmethod
    def match_price_within_bands(cls, v: float, info) -> float:
        """VN market rule: giá khớp không vượt trần/sàn (0.1% tolerance cho rounding)."""
        data = info.data
        ceil  = data.get("ceil_price", 0.0)
        floor = data.get("floor_price", 0.0)
        if v > 0 and ceil > 0 and v > ceil * 1.001:
            raise ValueError(f"match_price {v} exceeds ceil_price {ceil}")
        if v > 0 and floor > 0 and v < floor * 0.999:
            raise ValueError(f"match_price {v} below floor_price {floor}")
        return v


class TCBSTickerSnapRecord(BaseModel):
    """
    Lightweight market snapshot từ TCBS /tartarus/v1/tickerSnaps.

    Tập trung vào foreign room data + price change.
    Dùng cho foreign flow tracking pipeline.

    Fields:
        symbol:          Mã cổ phiếu
        ref_price:       Giá tham chiếu
        ceil_price/floor_price: Trần/sàn
        match_price/match_qty:  Giá và KL khớp
        change/change_pct:      Thay đổi giá
        total_volume/total_value: Tổng khối lượng và giá trị
        buy_foreign_qty/sell_foreign_qty: KL nước ngoài mua/bán
        room:            Room nước ngoài còn lại
        trading_date:    Ngày giao dịch
        ts_ms:           Unix timestamp milliseconds
    """
    symbol:           str
    ref_price:        float = Field(ge=0, default=0.0)
    ceil_price:       float = Field(ge=0, default=0.0)
    floor_price:      float = Field(ge=0, default=0.0)
    match_price:      float = Field(ge=0, default=0.0)
    match_qty:        float = Field(ge=0, default=0.0)
    change:           float = 0.0
    change_pct:       float = 0.0
    total_volume:     float = Field(ge=0, default=0.0)
    total_value:      float = Field(ge=0, default=0.0)
    buy_foreign_qty:  float = Field(ge=0, default=0.0)
    sell_foreign_qty: float = Field(ge=0, default=0.0)
    room:             str   = ""
    trading_date:     str   = ""
    ts_ms:            int


class TCBSBSARecord(BaseModel):
    """
    Buy/Sell Analysis record từ TCBS /nyx/v1/intraday/{ticker}/bsa.

    Cung cấp thông tin về cán cân cung/cầu: tỷ lệ mua chủ động (bup),
    bán chủ động (sdp), và tỷ lệ mua/bán (bsr) theo thời gian trong ngày.

    Fields:
        ticker:        Mã cổ phiếu
        investor_type: Loại nhà đầu tư — sheep | wolf | shark | all
        bup:           Buy Up Percent — % giao dịch mua chủ động
        sdp:           Sell Down Percent — % giao dịch bán chủ động
        bsr:           Buy/Sell Ratio — tỷ lệ mua/bán
        event_time:    Thời gian giao dịch từ API (string, e.g. "09:15:00")
        ts_ms:         Unix timestamp milliseconds lúc snapshot
    """
    ticker:        str
    investor_type: str

    bup:           float = Field(ge=0.0, le=100.0, default=0.0)
    sdp:           float = Field(ge=0.0, le=100.0, default=0.0)
    bsr:           float = Field(ge=0.0, default=0.0)
    event_time:    str   = ""
    ts_ms:         int

    @field_validator("investor_type")
    @classmethod
    def validate_investor_type(cls, v: str) -> str:
        """Validate investor_type theo constants YAML."""
        valid = {"sheep", "wolf", "shark", "all"}
        if v not in valid:
            raise ValueError(f"investor_type '{v}' not in {valid}")
        return v


class TCBSIntradayTickRecord(BaseModel):
    """
    Tick-by-tick khớp lệnh intraday từ TCBS /nyx/v1/intraday/{ticker}/his/paging.

    Lưu từng lệnh khớp thực tế trong phiên giao dịch.

    Fields:
        ticker:       Mã cổ phiếu
        price:        Giá khớp lệnh (p)
        volume:       Khối lượng khớp (v)
        price_change: Thay đổi giá so với tham chiếu (cp)
        action:       "Buy Up" hoặc "Sell Down" (a)
        buy_acc:      Lũy kế mua chủ động (ba)
        sell_acc:     Lũy kế bán chủ động (sa)
        highlight:    True nếu giá trị > 200 triệu (hl)
        event_time:   Thời gian khớp lệnh từ sàn (t)
        ts_ms:        Unix timestamp milliseconds lúc snapshot
    """
    ticker:       str
    price:        float = Field(ge=0, default=0.0)
    volume:       float = Field(ge=0, default=0.0)
    price_change: float = 0.0
    action:       str   = ""
    buy_acc:      float = Field(ge=0, default=0.0)
    sell_acc:     float = Field(ge=0, default=0.0)
    highlight:    bool  = False
    event_time:   str   = ""
    ts_ms:        int


# ─── Generic ──────────────────────────────────────────────────────────────────

class OHLCVRecord(BaseModel):
    """
    Generic OHLCV record (normalized form) dùng cho DeltaBronzeWriter.
    Phù hợp cho cả Binance và VPS khi đã qua normalize step.
    """
    symbol:    str
    exchange:  str
    interval:  str
    open:      float = Field(gt=0)
    high:      float = Field(gt=0)
    low:       float = Field(gt=0)
    close:     float = Field(gt=0)
    volume:    float = Field(ge=0)
    timestamp: datetime

    @field_validator("high")
    @classmethod
    def validate_high(cls, v: float, info) -> float:
        if "low" in info.data and v < info.data["low"]:
            raise ValueError("High price cannot be lower than Low price")
        return v
