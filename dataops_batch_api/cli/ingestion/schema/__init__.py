"""
<<<<<<< HEAD
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
=======
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
>>>>>>> 20261010_data_processing
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

<<<<<<< HEAD
__all__ = [
    # Binance
    "BinanceCandleRecord",
    "OHLCVRecord",
    # TCBS market data
=======
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
>>>>>>> 20261010_data_processing
    "TCBSMarketRecord",
    "TCBSTickerSnapRecord",
    "TCBSBSARecord",
    "TCBSIntradayTickRecord",
    "TCBSDerivativeRecord",
<<<<<<< HEAD
    # TCBS account/trading
=======
    # TCBS account/trading Pydantic models
>>>>>>> 20261010_data_processing
    "TCBSAccountCashRecord",
    "TCBSStockAssetRecord",
    "TCBSOrderRecord",
    "TCBSOpenPositionRecord",
<<<<<<< HEAD
=======
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
>>>>>>> 20261010_data_processing
]
