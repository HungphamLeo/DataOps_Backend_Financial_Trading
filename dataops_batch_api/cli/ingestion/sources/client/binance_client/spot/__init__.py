"""
spot/__init__.py — Binance Spot subpackage.

Phạm trù: Spot Market — Public (Market Data) + Private (Account & Trading)

Exports:
    SpotMarketDataMixin  : Public market data (klines, depth, ticker, book_ticker, ...)
    SpotAccountMixin     : Private account & trading (account, orders, place, cancel)
"""
from cli.ingestion.sources.client.binance_client.spot.market_data import SpotMarketDataMixin
from cli.ingestion.sources.client.binance_client.spot.account import SpotAccountMixin

__all__ = ["SpotMarketDataMixin", "SpotAccountMixin"]
