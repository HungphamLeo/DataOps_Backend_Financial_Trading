"""
tcbs_client.py — TCBSRestClient: assembled client (thin facade).

Phạm trù: Client Facade (Assembly)

Class này tổng hợp tất cả domain mixins thành 1 client hoàn chỉnh
theo mixin pattern. Không chứa business logic — toàn bộ logic
nằm trong các file domain riêng biệt.

Cấu trúc mixin:
    TCBSClientBase            ← http.py             HTTP Core: session, _get, from_env
    TCBSAuthMixin             ← auth.py              Auth / JWT
    TCBSMarketDataMixin       ← market_data.py       Market Data (public endpoints)
    TCBSIntradayAnalyticsMixin← intraday_analytics.py BSA + Tick History
    TCBSSecuritiesMixin       ← securities.py        Securities Listing (private)
    TCBSBatchHelpersMixin     ← batch_helpers.py     Batch / Pagination Helpers

Usage (backward compatible — không thay đổi gì):
    from cli.ingestion.sources.client.tcbs_client import TCBSRestClient, TCBSConstants

    client = TCBSRestClient.from_env()
    tickers = client.get_ticker_commons(index="HOSE")
    bsa     = client.get_intraday_bsa("TCB", investor_type="all")
    hist    = client.get_intraday_history("VCB", page=0, size=100)

References:
    - TCBS iFlash Open API v1.0.0 — https://openapi.tcbs.com.vn
    - openapi-v1.0.0.json
"""
from __future__ import annotations

from cli.ingestion.sources.tcbs_constants_loader import TCBSConstants  # noqa: F401

from cli.ingestion.sources.client.tcbs_client.http import TCBSClientBase
from cli.ingestion.sources.client.tcbs_client.auth import TCBSAuthMixin
from cli.ingestion.sources.client.tcbs_client.market_data import TCBSMarketDataMixin
from cli.ingestion.sources.client.tcbs_client.intraday_analytics import TCBSIntradayAnalyticsMixin
from cli.ingestion.sources.client.tcbs_client.securities import TCBSSecuritiesMixin
<<<<<<< HEAD
=======
from cli.ingestion.sources.client.tcbs_client.margin import TCBSMarginMixin
>>>>>>> 20261010_data_processing
from cli.ingestion.sources.client.tcbs_client.batch_helpers import TCBSBatchHelpersMixin

# Re-export TCBSConstants để backward-compatible với code cũ import từ tcbs_client
__all__ = ["TCBSRestClient", "TCBSConstants"]


class TCBSRestClient(
    TCBSAuthMixin,
    TCBSMarketDataMixin,
    TCBSIntradayAnalyticsMixin,
    TCBSSecuritiesMixin,
<<<<<<< HEAD
=======
    TCBSMarginMixin,
>>>>>>> 20261010_data_processing
    TCBSBatchHelpersMixin,
    TCBSClientBase,
):
    """
    TCBS iFlash Open API REST client đầy đủ — assembled từ domain mixins.

    Domains:
        Auth       : authenticate(), set_token(), _is_token_valid()
        Market Data: get_ticker_commons(), get_ticker_snaps(),
                     get_derivatives(), get_put_through_snaps()
        Analytics  : get_intraday_bsa(), get_intraday_bsa_ext(),
                     get_intraday_bsa_month(), get_intraday_history()
        Securities : get_securities()
<<<<<<< HEAD
=======
        Margin     : get_account_risk(), get_pricing_policy(), get_margin_info()
>>>>>>> 20261010_data_processing
        Batch      : get_ticker_commons_batch(), get_intraday_history_all_pages()

    Usage:
        client = TCBSRestClient.from_env()

        # Market data (không cần auth)
        tickers = client.get_ticker_commons(index="HOSE")
        snaps   = client.get_ticker_snaps(index="VN30")
        derivs  = client.get_derivatives()

        # Analytics
        bsa     = client.get_intraday_bsa("TCB", investor_type="all")
        hist    = client.get_intraday_history("VCB", page=0, size=100)
        all_ticks = client.get_intraday_history_all_pages("VCB", max_pages=5)

        # Cần auth
        client.authenticate(otp="123456")
        securities = client.get_securities(filter_expr="exchange=HOSE")
    """
