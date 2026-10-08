"""
cli/ingestion/schema — Data contracts và Pydantic validation models.

CDM (Canonical Data Model) đã chuyển sang dataops_protobuf/market-data/market_data.proto.
File này chỉ export Pydantic validation models dùng để validate raw API response
TRƯỚC khi ghi vào Delta Lake.

Flow:
    Source API response
        → Pydantic model (models.py) — validate, reject bad data
        → ghi Delta Lake / MySQL

Canonical enums & types: import từ generated protobuf code hoặc dùng string literals
theo spec trong dataops_protobuf/market-data/market_data.proto.
"""

# ── Pydantic validation models — Binance ──────────────────────────────────────
from cli.ingestion.schema.models import (
    BinanceCandleRecord,
    OHLCVRecord,
)

# ── Pydantic validation models — TCBS market data ─────────────────────────────
from cli.ingestion.schema.models import (
    TCBSMarketRecord,
    TCBSTickerSnapRecord,
    TCBSBSARecord,
    TCBSIntradayTickRecord,
    TCBSDerivativeRecord,
)

# ── Pydantic validation models — TCBS account/trading ─────────────────────────
from cli.ingestion.schema.models import (
    TCBSAccountCashRecord,
    TCBSStockAssetRecord,
    TCBSOrderRecord,
    TCBSOpenPositionRecord,
)

__all__ = [
    # Binance
    "BinanceCandleRecord",
    "OHLCVRecord",
    # TCBS market data
    "TCBSMarketRecord",
    "TCBSTickerSnapRecord",
    "TCBSBSARecord",
    "TCBSIntradayTickRecord",
    "TCBSDerivativeRecord",
    # TCBS account/trading
    "TCBSAccountCashRecord",
    "TCBSStockAssetRecord",
    "TCBSOrderRecord",
    "TCBSOpenPositionRecord",
]
