"""
models.py — Pydantic Schemas Validation cho Delta Lake Bronze/Silver.

Validate dữ liệu tại runtime TRƯỚC khi ghi vào Delta Lake.
Nếu dữ liệu vi phạm business rule → raise ValidationError (không ghi record xấu).

Design principle: "Bad data in = bad data out"
  → Validate ở tầng ingestion, không phải tầng analytics.
"""
from __future__ import annotations

from datetime import datetime

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


class VPSCandleRecord(BaseModel):
    """
    Validated market snapshot từ VPS Securities (HOSE).

    Fields:
        symbol:          Mã cổ phiếu, e.g. "VCB", "VHM"
        exchange:        Sàn giao dịch — "HOSE" (scope hiện tại)
        price:           Giá khớp lệnh hiện tại
        ceiling_price:   Giá trần (price <= ceiling * 1.001 — 0.1% tolerance)
        floor_price:     Giá sàn (price >= floor * 0.999)
        session_state:   Trạng thái phiên: ATO | ATC | CONTINUOUS | CLOSED | UNKNOWN
        ts_ms:           Unix timestamp milliseconds lúc snapshot
    """
    symbol:          str
    exchange:        str           # HOSE (HOSE only per current scope)
    price:           float = Field(gt=0)
    open:            float = Field(ge=0, default=0.0)
    high:            float = Field(ge=0, default=0.0)
    low:             float = Field(ge=0, default=0.0)
    reference_price: float = Field(ge=0, default=0.0)
    ceiling_price:   float = Field(ge=0, default=0.0)
    floor_price:     float = Field(ge=0, default=0.0)
    volume:          float = Field(ge=0, default=0.0)
    total_value:     float = Field(ge=0, default=0.0)
    change_pct:      float = 0.0
    session_state:   str   = "UNKNOWN"
    ts_ms:           int

    @field_validator("price")
    @classmethod
    def price_within_bands(cls, v: float, info) -> float:
        """VN market rule: giá giao dịch không vượt trần/sàn."""
        data = info.data
        if data.get("ceiling_price", 0) > 0 and v > data["ceiling_price"] * 1.001:
            raise ValueError(f"price {v} exceeds ceiling {data['ceiling_price']}")
        if data.get("floor_price", 0) > 0 and v < data["floor_price"] * 0.999:
            raise ValueError(f"price {v} below floor {data['floor_price']}")
        return v


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
