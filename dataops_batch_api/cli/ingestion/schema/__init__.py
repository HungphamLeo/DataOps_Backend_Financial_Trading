"""
cli/ingestion/schema — Data contracts và validation models.

Hai lớp tách biệt rõ ràng:

1. canonical.py — Canonical Data Model (CDM)
   Ngôn ngữ chung, source-agnostic. Downstream code CHỈ dùng các type này.
   Enums: CanonicalInterval, CanonicalSide, CanonicalOrderStatus,
          CanonicalInstrumentType, CanonicalExchange
   Models: CanonicalOHLCV, CanonicalTrade, CanonicalOrder,
           CanonicalPosition, CanonicalAccountBalance
   Mappers: from_binance_kline_row(), from_tcbs_intraday_tick(), ...

2. models.py — Pydantic validation schemas
   Source-specific Pydantic models dùng để validate raw API response
   TRƯỚC khi ghi vào Delta Lake. Mỗi model gắn với 1 API endpoint cụ thể.
   BinanceCandleRecord, TCBSMarketRecord, TCBSBSARecord, ...

Flow:
    Source API response
        → Pydantic model (models.py) — validate, reject bad data
        → Mapper function (canonical.py) — normalize to canonical form
        → Canonical model — ghi Delta Lake, InfluxDB, trả về API
"""

# ── Canonical enums ──────────────────────────────────────────────────────────
from cli.ingestion.schema.canonical import (
    CanonicalAccountBalance,
    CanonicalExchange,
    CanonicalInterval,
    CanonicalInstrumentType,
    CanonicalOrder,
    CanonicalOrderStatus,
    CanonicalOHLCV,
    CanonicalPosition,
    CanonicalSide,
    CanonicalTrade,
)

# ── Canonical mapper functions ────────────────────────────────────────────────
from cli.ingestion.schema.canonical import (
    from_binance_kline_row,
    from_binance_trade_message,
    from_tcbs_account_cash,
    from_tcbs_deriv_position,
    from_tcbs_intraday_tick,
    from_tcbs_stock_order,
)

# ── Pydantic validation models ────────────────────────────────────────────────
from cli.ingestion.schema.models import (
    BinanceCandleRecord,
    OHLCVRecord,
    TCBSAccountCashRecord,
    TCBSBSARecord,
    TCBSDerivativeRecord,
    TCBSIntradayTickRecord,
    TCBSMarketRecord,
    TCBSOpenPositionRecord,
    TCBSOrderRecord,
    TCBSStockAssetRecord,
    TCBSTickerSnapRecord,
)

__all__ = [
    # Canonical enums
    "CanonicalInterval",
    "CanonicalSide",
    "CanonicalOrderStatus",
    "CanonicalInstrumentType",
    "CanonicalExchange",
    # Canonical models
    "CanonicalOHLCV",
    "CanonicalTrade",
    "CanonicalOrder",
    "CanonicalPosition",
    "CanonicalAccountBalance",
    # Canonical mappers
    "from_binance_kline_row",
    "from_binance_trade_message",
    "from_tcbs_intraday_tick",
    "from_tcbs_stock_order",
    "from_tcbs_deriv_position",
    "from_tcbs_account_cash",
    # Pydantic models — Binance
    "BinanceCandleRecord",
    "OHLCVRecord",
    # Pydantic models — TCBS market data
    "TCBSMarketRecord",
    "TCBSTickerSnapRecord",
    "TCBSBSARecord",
    "TCBSIntradayTickRecord",
    "TCBSDerivativeRecord",
    # Pydantic models — TCBS account/trading
    "TCBSAccountCashRecord",
    "TCBSStockAssetRecord",
    "TCBSOrderRecord",
    "TCBSOpenPositionRecord",
]
