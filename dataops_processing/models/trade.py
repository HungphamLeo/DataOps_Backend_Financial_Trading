"""
models/trade.py — Input data model cho raw trade event từ Kafka.

Đây là ranh giới canonical giữa tầng transport (Kafka bytes) và tầng xử lý (Spark jobs).
Field names khớp với CanonicalTrade trong dataops_protobuf/market-data/market_data.proto.

Producer (Go types.go TradeEvent) gửi JSON:
    instId, price, size, side, tsMs, producedAtMs, instType

Model này normalize sang canonical names:
    instId  → symbol   (.proto CanonicalTrade.symbol)
    tsMs    → ts_ms    (.proto CanonicalTrade.ts_ms, unit: milliseconds)
    side    → side     ("buy" | "sell", lowercase, Avro TradeSide enum)

Confluent Wire Format (từ stream producer serializeAvro):
    [0x00][4-byte schema_id BE][avro/json bytes]
    Nếu Schema Registry không khả dụng → producer gửi plain JSON (không có magic byte).
"""
from __future__ import annotations

import json
import struct
from typing import Optional


# =============================================================================
# Confluent Wire Format stripping
# =============================================================================

def strip_confluent_header(raw: bytes) -> bytes:
    """
    Strip 5-byte Confluent Schema Registry header nếu có magic byte 0x00.

    Format: [0x00][schema_id: 4 bytes big-endian][payload]
    Nếu không có magic byte → dữ liệu là plain JSON → trả nguyên.
    """
    if len(raw) > 5 and raw[0] == 0x00:
        return raw[5:]
    return raw


# =============================================================================
# Input model — raw Kafka trade event
# =============================================================================

class TradeEvent:
    """
    Immutable value object cho một trade event từ Kafka.

    Không dùng Pydantic ở đây vì class này được serialize vào Spark UDF —
    Pydantic không picklable trong Spark executor context.
    Dùng __slots__ để giảm memory footprint trên executor.
    """

    __slots__ = ("symbol", "price", "size", "side", "ts_ms", "notional")

    def __init__(
        self,
        symbol: str,
        price: float,
        size: float,
        side: str,
        ts_ms: int,
    ) -> None:
        self.symbol   = symbol
        self.price    = price
        self.size     = size
        self.side     = side
        self.ts_ms    = ts_ms
        self.notional = price * size   # VWAP numerator component — pre-computed

    @classmethod
    def from_kafka_bytes(cls, raw: bytes) -> Optional["TradeEvent"]:
        """
        Parse raw Kafka value bytes → TradeEvent.

        Đây là CANONICAL BOUNDARY duy nhất xử lý bytes từ Kafka.
        Mọi deserialization logic nằm ở đây, không rải trong job code.

        Returns:
            TradeEvent nếu parse thành công.
            None nếu malformed — record bị skip, job không crash.
        """
        payload = strip_confluent_header(raw)
        try:
            data = json.loads(payload)
        except (ValueError, UnicodeDecodeError):
            return None

        symbol = data.get("instId")
        price  = data.get("price")
        size   = data.get("size")
        ts_ms  = data.get("tsMs")

        if not symbol or price is None or size is None or ts_ms is None:
            return None
        if float(price) <= 0 or float(size) <= 0:
            return None

        return cls(
            symbol=str(symbol),
            price=float(price),
            size=float(size),
            side=str(data.get("side", "buy")),
            ts_ms=int(ts_ms),
        )

    def to_tuple(self) -> tuple:
        """Trả về tuple cho Spark UDF return (khớp với trade_schema StructType)."""
        return (self.symbol, self.price, self.size, self.side, self.ts_ms)
