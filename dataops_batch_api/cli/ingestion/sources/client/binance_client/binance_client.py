"""
binance_client.py — BinanceSpotClient assembled client (thin facade).

Phạm trù: Client Facade (Assembly)

Class được tổng hợp từ domain mixins theo mixin pattern:

    BinanceSpotClient:
        SpotMarketDataMixin   ← spot/market_data.py    Spot Market Data (public)
        SpotAccountMixin      ← spot/account.py        Spot Account & Trading (private)
        BinanceClientBase     ← http.py                HTTP Core: session, throttle, from_env

Base URL:
    Spot:        https://api.binance.com

Dự án chỉ dùng Binance Spot — không hỗ trợ UM Futures.

SDK: binance-connector v3.13.0 (binance.spot.Spot)
    - Tự động sign private requests khi api_key + api_secret được set.

Usage:
    from cli.ingestion.sources.client.binance_client import BinanceSpotClient

    # Spot — public (không cần key)
    spot = BinanceSpotClient.from_env()
    klines = spot.get_klines("BTCUSDT", "1h", limit=200)
    ticker = spot.get_ticker_24hr("BTCUSDT")

    # Spot — private (cần BINANCE_API_KEY + BINANCE_SECRET_KEY)
    account = spot.get_account()
    trades  = spot.get_my_trades("BTCUSDT")
"""
from __future__ import annotations

from cli.ingestion.sources.client.binance_client.http import (
    BinanceClientBase,
    _SPOT_BASE_URL,
    _TESTNET_SPOT_BASE_URL,
)
from cli.ingestion.sources.client.binance_client.spot.market_data import SpotMarketDataMixin
from cli.ingestion.sources.client.binance_client.spot.account import SpotAccountMixin


__all__ = ["BinanceSpotClient"]


class BinanceSpotClient(
    SpotMarketDataMixin,
    SpotAccountMixin,
    BinanceClientBase,
):
    """
    Binance Spot REST client đầy đủ — assembled từ domain mixins.

    Domains:
        Market Data (public) : get_klines, get_exchange_info, get_depth,
                               get_ticker_price, get_ticker_24hr, get_book_ticker,
                               get_avg_price, get_agg_trades, get_trades
        Account (private)    : get_account, get_my_trades, get_order, get_orders,
                               get_open_orders, place_order, cancel_order,
                               cancel_open_orders

    Usage:
        spot = BinanceSpotClient.from_env()
        klines = spot.get_klines("BTCUSDT", "1h", limit=200)
    """

    def __init__(self, **kwargs):
        kwargs.setdefault("base_url", _SPOT_BASE_URL)
        super().__init__(**kwargs)

    @classmethod
    def from_env(cls) -> "BinanceSpotClient":
        """
        Factory từ env vars — Spot base_url.

        Env vars:
            BINANCE_API_KEY, BINANCE_SECRET_KEY, BINANCE_TESTNET, BINANCE_RATE_LIMIT
        """
        import os
        is_testnet = os.getenv("BINANCE_TESTNET", "false").lower() == "true"
        base_url = _TESTNET_SPOT_BASE_URL if is_testnet else _SPOT_BASE_URL
        return cls(
            base_url=base_url,
            api_key=os.getenv("BINANCE_API_KEY", ""),
            secret_key=os.getenv("BINANCE_SECRET_KEY", ""),
            is_testnet=is_testnet,
            rate_limit_interval=float(os.getenv("BINANCE_RATE_LIMIT", "0.1")),
        )
