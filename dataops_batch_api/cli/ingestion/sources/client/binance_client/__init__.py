"""
__init__.py — Public API cho package binance_client.

Phạm trù: Package Entry Point / Backward Compatibility

Exports chính:
    BinanceSpotClient    : Spot Market — Market Data + Account & Trading
<<<<<<< HEAD
    BinanceFuturesClient : USD-M Futures — Market Data + Account & Trading

Usage (không cần import đường dẫn nội bộ):
    from cli.ingestion.sources.client.binance_client import BinanceSpotClient, BinanceFuturesClient

    spot    = BinanceSpotClient.from_env()
    futures = BinanceFuturesClient.from_env()

Cấu trúc nội bộ (mixin pattern):
    BinanceClientBase           ← http.py             HTTP Core: Spot SDK, throttle, sign, from_env
    ├── BinanceSpotClient
    │   ├── SpotMarketDataMixin ← spot/market_data.py  Spot Market Data (public)
    │   └── SpotAccountMixin    ← spot/account.py      Spot Account & Trading (private)
    └── BinanceFuturesClient
        ├── FuturesMarketDataMixin ← future/market_data.py  UM Futures Market Data (public)
        └── FuturesAccountMixin    ← future/account.py      UM Futures Account (private)
=======

Usage (không cần import đường dẫn nội bộ):
    from cli.ingestion.sources.client.binance_client import BinanceSpotClient
    spot    = BinanceSpotClient.from_env()
>>>>>>> 20261010_data_processing

Backward compat:
    BinanceRestClient tham chiếu cũ → alias sang BinanceSpotClient.
"""
from cli.ingestion.sources.client.binance_client.binance_client import (
<<<<<<< HEAD
    BinanceSpotClient,
    BinanceFuturesClient,
=======
    BinanceSpotClient
>>>>>>> 20261010_data_processing
)

# Backward-compat alias — code cũ dùng BinanceRestClient vẫn chạy được
BinanceRestClient = BinanceSpotClient

<<<<<<< HEAD
__all__ = ["BinanceSpotClient", "BinanceFuturesClient", "BinanceRestClient"]
=======
__all__ = ["BinanceSpotClient", "BinanceRestClient"]
>>>>>>> 20261010_data_processing
