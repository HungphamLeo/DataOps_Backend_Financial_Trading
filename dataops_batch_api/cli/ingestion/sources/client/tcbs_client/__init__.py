"""
__init__.py — Public API cho package tcbs_client.

Phạm trù: Package Entry Point / Backward Compatibility

Export chính:
    TCBSRestClient : Client tổng hợp đầy đủ tất cả domain mixins.
    TCBSConstants  : Constants/config loader (re-export từ tcbs_constants_loader).

Usage (không thay đổi so với code cũ):
    from cli.ingestion.sources.client.tcbs_client import TCBSRestClient, TCBSConstants

    client = TCBSRestClient.from_env()
    tickers = client.get_ticker_commons(index="HOSE")
    bsa     = client.get_intraday_bsa("TCB", investor_type="all")
    hist    = client.get_intraday_history("VCB", page=0, size=100)

Cấu trúc nội bộ (mixin pattern):
    TCBSClientBase            ← http.py        (HTTP Core: session, _get, from_env)
        + TCBSAuthMixin       ← auth.py         (Auth: authenticate, set_token, …)
        + TCBSMarketDataMixin ← market_data.py  (Market Data: ticker_commons, snaps, …)
        + TCBSIntradayAnalyticsMixin ← intraday_analytics.py (BSA, tick history)
        + TCBSSecuritiesMixin ← securities.py   (Securities listing)
        + TCBSBatchHelpersMixin ← batch_helpers.py (Batch/pagination helpers)
    = TCBSRestClient          ← tcbs_client.py  (Assembled class)
"""
from cli.ingestion.sources.client.tcbs_client.tcbs_client import TCBSRestClient
<<<<<<< HEAD
from cli.flows.loader.tcbs_constants_loader import TCBSConstants  # noqa: F401
=======
from cli.ingestion.sources.tcbs_constants_loader import TCBSConstants  # noqa: F401
>>>>>>> 20261010_data_processing

__all__ = ["TCBSRestClient", "TCBSConstants"]
