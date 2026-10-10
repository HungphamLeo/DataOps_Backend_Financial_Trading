"""
__init__.py — Public API cho package binance_client.

Phạm trù: Package Entry Point / Backward Compatibility

Exports chính:
    BinanceSpotClient    : Spot Market — Market Data + Account & Trading

Usage (không cần import đường dẫn nội bộ):
    from cli.ingestion.sources.client.binance_client import BinanceSpotClient
    spot    = BinanceSpotClient.from_env()

Backward compat:
    BinanceRestClient tham chiếu cũ → alias sang BinanceSpotClient.
"""
from cli.ingestion.sources.client.binance_client.binance_client import (
    BinanceSpotClient
)

# Backward-compat alias — code cũ dùng BinanceRestClient vẫn chạy được
BinanceRestClient = BinanceSpotClient

__all__ = ["BinanceSpotClient", "BinanceRestClient"]
