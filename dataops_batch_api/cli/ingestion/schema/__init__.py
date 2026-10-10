"""
cli/ingestion/schema — Data contracts, Pydantic validation, và Canonical mappers.

⚠️  SOURCE OF TRUTH: dataops_protobuf/market-data/market_data.proto
    Khi thêm/sửa field trong .proto → phải update canonical.py cùng commit.

Layering:
    raw API response
        → Pydantic model (models.py)    — validate, reject bad data
        → mapper function (canonical.py) — normalize field names / enums
        → dict (downstream: Delta Lake, gRPC, Redis)

models.py   : source-specific Pydantic validation + business rules
canonical.py: pure mapper functions → canonical dict shape theo .proto spec
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

# ── Canonical mapper functions ─────────────────────────────────────────────────
from cli.ingestion.schema.canonical import (
    from_binance_kline_row,
    from_tcbs_ticker_record,
    from_tcbs_bsa_record,
    EXCHANGE_BINANCE,
    EXCHANGE_BINANCE_FUTURES,
    EXCHANGE_TCBS_HOSE,
    EXCHANGE_TCBS_HNX,
    EXCHANGE_TCBS_UPCOM,
    EXCHANGE_TCBS_DERIV,
    SIDE_BUY,
    SIDE_SELL,
    SIDE_LONG,
    SIDE_SHORT,
    INST_SPOT,
    INST_FUTURES,
    INST_DERIVATIVE,
)

__all__ = [
    # Binance Pydantic models
    "BinanceCandleRecord",
    "OHLCVRecord",
    # TCBS market data Pydantic models
    "TCBSMarketRecord",
    "TCBSTickerSnapRecord",
    "TCBSBSARecord",
    "TCBSIntradayTickRecord",
    "TCBSDerivativeRecord",
    # TCBS account/trading Pydantic models
    "TCBSAccountCashRecord",
    "TCBSStockAssetRecord",
    "TCBSOrderRecord",
    "TCBSOpenPositionRecord",
    # Canonical mapper functions
    "from_binance_kline_row",
    "from_tcbs_ticker_record",
    "from_tcbs_bsa_record",
    # Canonical constants (mirror .proto enums)
    "EXCHANGE_BINANCE",
    "EXCHANGE_BINANCE_FUTURES",
    "EXCHANGE_TCBS_HOSE",
    "EXCHANGE_TCBS_HNX",
    "EXCHANGE_TCBS_UPCOM",
    "EXCHANGE_TCBS_DERIV",
    "SIDE_BUY",
    "SIDE_SELL",
    "SIDE_LONG",
    "SIDE_SHORT",
    "INST_SPOT",
    "INST_FUTURES",
    "INST_DERIVATIVE",
]
