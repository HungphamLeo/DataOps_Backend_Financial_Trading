# -*- coding: utf-8 -*-
# Generated from batch_data.proto (package batchdata.v1)
# Handwritten stub — grpc_tools.protoc unavailable on this build environment.
# Semantics identical to protoc output: message classes with constructor kwargs,
# HasField() for optional Timestamp fields, and correct __repr__.
#
# Proto source:  dataops_protobuf/batch-data/batch_data.proto
# Regenerate:    cd dataops_protobuf && python -m grpc_tools.protoc \
#                    -I. --python_out=../dataops_batch_api \
#                    --grpc_python_out=../dataops_batch_api \
#                    batch-data/batch_data.proto
from __future__ import annotations

from typing import Iterator, List, Optional


# ---------------------------------------------------------------------------
# Minimal Timestamp shim — mirrors google.protobuf.timestamp_pb2.Timestamp
# ---------------------------------------------------------------------------
class _Timestamp:
    def __init__(self, seconds: int = 0, nanos: int = 0) -> None:
        self.seconds = seconds
        self.nanos = nanos

    def __repr__(self) -> str:  # pragma: no cover
        return f"Timestamp(seconds={self.seconds}, nanos={self.nanos})"


# ---------------------------------------------------------------------------
# Section 1 — BinanceOHLCV
# ---------------------------------------------------------------------------

class BinanceOHLCVRequest:
    """Request message for GetBinanceOHLCV RPC."""

    def __init__(
        self,
        symbol: str = "",
        inst_type: str = "",
        interval: str = "",
        from_: Optional[_Timestamp] = None,
        to: Optional[_Timestamp] = None,
        limit: int = 0,
    ) -> None:
        self.symbol    = symbol
        self.inst_type = inst_type
        self.interval  = interval
        self.from_     = from_   # named from_ to avoid Python keyword conflict
        self.to        = to
        self.limit     = limit

    def HasField(self, field_name: str) -> bool:
        """Mirror protobuf HasField — returns True if optional Timestamp is set."""
        if field_name == "from":
            return self.from_ is not None
        if field_name == "to":
            return self.to is not None
        return False

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"BinanceOHLCVRequest(symbol={self.symbol!r}, inst_type={self.inst_type!r}, "
            f"interval={self.interval!r}, limit={self.limit})"
        )


class BinanceOHLCVBar:
    """A single OHLCV candle bar from Binance Delta Lake."""

    def __init__(
        self,
        symbol: str = "", inst_type: str = "", interval: str = "",
        ts_open_ms: int = 0, ts_close_ms: int = 0,
        open: float = 0.0, high: float = 0.0, low: float = 0.0, close: float = 0.0,
        volume: float = 0.0, quote_volume: float = 0.0, num_trades: int = 0,
    ) -> None:
        self.symbol       = symbol
        self.inst_type    = inst_type
        self.interval     = interval
        self.ts_open_ms   = ts_open_ms
        self.ts_close_ms  = ts_close_ms
        self.open         = open
        self.high         = high
        self.low          = low
        self.close        = close
        self.volume       = volume
        self.quote_volume = quote_volume
        self.num_trades   = num_trades


class BinanceOHLCVResponse:
    """Response message for GetBinanceOHLCV RPC."""

    def __init__(
        self,
        symbol: str = "", inst_type: str = "", interval: str = "",
        bars: Optional[List[BinanceOHLCVBar]] = None,
        count: int = 0,
    ) -> None:
        self.symbol    = symbol
        self.inst_type = inst_type
        self.interval  = interval
        self.bars      = bars or []
        self.count     = count


# ---------------------------------------------------------------------------
# Section 2 — TCBS Market EOD
# ---------------------------------------------------------------------------

class TCBSMarketEODRequest:
    def __init__(
        self,
        symbol: str = "", exchange: str = "",
        from_date: str = "", to_date: str = "", limit: int = 0,
    ) -> None:
        self.symbol    = symbol
        self.exchange  = exchange
        self.from_date = from_date
        self.to_date   = to_date
        self.limit     = limit


class TCBSMarketEODBar:
    def __init__(
        self,
        trading_date: str = "", symbol: str = "", exchange: str = "",
        open: float = 0.0, high: float = 0.0, low: float = 0.0, close: float = 0.0,
        total_vol: float = 0.0, total_val: float = 0.0,
        buy_foreign_qty: float = 0.0, sell_foreign_qty: float = 0.0,
        net_foreign_qty: float = 0.0, change_pct: float = 0.0,
        ref_price: float = 0.0, ceil_price: float = 0.0, floor_price: float = 0.0,
    ) -> None:
        self.trading_date    = trading_date
        self.symbol          = symbol
        self.exchange        = exchange
        self.open            = open
        self.high            = high
        self.low             = low
        self.close           = close
        self.total_vol       = total_vol
        self.total_val       = total_val
        self.buy_foreign_qty = buy_foreign_qty
        self.sell_foreign_qty = sell_foreign_qty
        self.net_foreign_qty = net_foreign_qty
        self.change_pct      = change_pct
        self.ref_price       = ref_price
        self.ceil_price      = ceil_price
        self.floor_price     = floor_price


class TCBSMarketEODResponse:
    def __init__(
        self,
        symbol: str = "", exchange: str = "",
        bars: Optional[List[TCBSMarketEODBar]] = None,
        count: int = 0,
    ) -> None:
        self.symbol   = symbol
        self.exchange = exchange
        self.bars     = bars or []
        self.count    = count


# ---------------------------------------------------------------------------
# Section 3 — TCBS BSA History
# ---------------------------------------------------------------------------

class TCBSBSARequest:
    def __init__(
        self,
        ticker: str = "", investor_type: str = "",
        from_date: str = "", to_date: str = "", limit: int = 0,
    ) -> None:
        self.ticker        = ticker
        self.investor_type = investor_type
        self.from_date     = from_date
        self.to_date       = to_date
        self.limit         = limit


class TCBSBSAPoint:
    def __init__(
        self,
        trading_date: str = "", ticker: str = "", investor_type: str = "",
        bup: float = 0.0, sdp: float = 0.0, bsr: float = 0.0,
    ) -> None:
        self.trading_date  = trading_date
        self.ticker        = ticker
        self.investor_type = investor_type
        self.bup           = bup
        self.sdp           = sdp
        self.bsr           = bsr


class TCBSBSAResponse:
    def __init__(
        self,
        ticker: str = "", investor_type: str = "",
        points: Optional[List[TCBSBSAPoint]] = None,
        count: int = 0,
    ) -> None:
        self.ticker        = ticker
        self.investor_type = investor_type
        self.points        = points or []
        self.count         = count


# ---------------------------------------------------------------------------
# Section 4 — Metadata
# ---------------------------------------------------------------------------

class DataRangeRequest:
    def __init__(
        self,
        source: str = "", symbol: str = "",
        inst_type: str = "", investor_type: str = "",
    ) -> None:
        self.source        = source
        self.symbol        = symbol
        self.inst_type     = inst_type
        self.investor_type = investor_type


class DataRangeResponse:
    def __init__(
        self,
        source: str = "", symbol: str = "",
        oldest_date: str = "", newest_date: str = "",
        total_count: int = 0, last_updated_at: str = "",
    ) -> None:
        self.source          = source
        self.symbol          = symbol
        self.oldest_date     = oldest_date
        self.newest_date     = newest_date
        self.total_count     = total_count
        self.last_updated_at = last_updated_at
