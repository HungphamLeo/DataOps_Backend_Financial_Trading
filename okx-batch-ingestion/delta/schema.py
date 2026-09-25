"""
Pydantic Schemas Validation trước khi write dữ liệu vào Delta Lake Bronze/Silver.
"""
from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class OHLCVRecord(BaseModel):
    symbol: str
    exchange: str
    interval: str
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: float = Field(ge=0)
    timestamp: datetime

    @field_validator("high")
    @classmethod
    def validate_high(cls, v, info):
        if "low" in info.data and v < info.data["low"]:
            raise ValueError("High price cannot be lower than Low price")
        return v


class VPSSnapshotRecord(BaseModel):
    symbol: str
    price: float = Field(gt=0)
    change_pct: float
    volume: float = Field(ge=0)
    bid_price_1: float = 0.0
    bid_vol_1: float = 0.0
    ask_price_1: float = 0.0
    ask_vol_1: float = 0.0
    timestamp: datetime
