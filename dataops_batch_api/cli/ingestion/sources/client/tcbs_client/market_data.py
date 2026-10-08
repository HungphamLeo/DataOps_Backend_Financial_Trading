"""
market_data.py — TCBS Market Data mixin (Public Endpoints).

Phạm trù: Market Data
    - get_ticker_commons()     : Bảng giá với orderbook 3 bước giá (bid/offer × 3).
    - get_ticker_snaps()       : Foreign room + market snapshot.
    - get_derivatives()        : Bảng giá thị trường phái sinh (VN30F contracts).
    - get_put_through_snaps()  : Giao dịch thỏa thuận (put-through transactions).

Tất cả endpoints này là PUBLIC — không cần JWT token.
Retry: 3 lần, exponential backoff (1s → 15s).

TCBS iFlash API endpoints (từ TCBSConstants):
    ticker_commons     : /tartarus/v1/tickerCommons
    ticker_snaps       : /tartarus/v1/tickerSnaps
    derivatives        : /tartarus/v1/derivatives
    put_through_snaps  : /tartarus/v1/putThroughSnaps
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from tenacity import retry, stop_after_attempt, wait_exponential

from log.logger_setup import logger_manager
from cli.ingestion.sources.tcbs_constants_loader import TCBSConstants
from cli.ingestion.sources.client.tcbs_client._retry import _log_retry

logger = logger_manager.get_logger(__name__)


class TCBSMarketDataMixin:
    """
    Phạm trù Market Data — public endpoints không cần JWT.

    Kế thừa bởi TCBSRestClient. Dùng self._get() từ TCBSClientBase.
    """

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        reraise=True,
        after=_log_retry,
    )
    def get_ticker_commons(
        self,
        tickers: Optional[List[str]] = None,
        index: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Lấy bảng giá cổ phiếu với orderbook 3 bước giá (bid/offer × 3).

        QUAN TRỌNG: Chỉ dùng tickers HOẶC index, không dùng cả hai.

        Args:
            tickers: Danh sách mã, e.g. ["VCB", "VHM", "TCB"].
            index:   Tên basket index, e.g. "HOSE", "VN30", "HNX".

        Returns:
            Dict với keys: tradingDate, data (list of ticker objects).

        Raises:
            ValueError: nếu cả tickers và index đều None, hoặc cả hai đều được truyền.
            requests.HTTPError: HTTP error sau khi hết retry.
        """
        if tickers is None and index is None:
            raise ValueError(
                "get_ticker_commons: phải truyền ít nhất tickers hoặc index."
            )
        if tickers is not None and index is not None:
            raise ValueError(
                "get_ticker_commons: không dùng tickers và index cùng lúc."
            )

        params: Dict[str, Any] = {}
        if tickers:
            params["tickers"] = ",".join(tickers)
        elif index:
            params["index"] = TCBSConstants.index_code(index)

        path = TCBSConstants.endpoint("ticker_commons")
        logger.debug("get_ticker_commons — params=%s", params)

        try:
            result = self._get(path, params=params)  # type: ignore[attr-defined]
            data = result.get("data", [])
            logger.debug(
                "get_ticker_commons — returned %d tickers, tradingDate=%s",
                len(data) if isinstance(data, list) else "N/A",
                result.get("tradingDate"),
            )
            return result
        except Exception as exc:
            logger.error("get_ticker_commons failed — params=%s: %s", params, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        reraise=True,
        after=_log_retry,
    )
    def get_ticker_snaps(
        self,
        index: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Lấy foreign room + market snapshot (matchPrice, volume, change, foreign buy/sell/room).

        Args:
            index: Basket index name, e.g. "HOSE", "VN30". None → toàn thị trường.

        Returns:
            Dict với keys: tradingDate, data (list).

        Raises:
            requests.HTTPError: HTTP error.
        """
        params: Dict[str, Any] = {}
        if index:
            params["index"] = TCBSConstants.index_code(index)

        path = TCBSConstants.endpoint("ticker_snaps")
        logger.debug("get_ticker_snaps — index=%s", index)

        try:
            result = self._get(path, params=params)  # type: ignore[attr-defined]
            data = result.get("data", [])
            logger.debug(
                "get_ticker_snaps — returned %d records",
                len(data) if isinstance(data, list) else "N/A",
            )
            return result
        except Exception as exc:
            logger.error("get_ticker_snaps failed — index=%s: %s", index, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        reraise=True,
        after=_log_retry,
    )
    def get_derivatives(self) -> Dict[str, Any]:
        """
        Lấy bảng giá thị trường phái sinh (VN30F contracts).

        Returns:
            Dict với keys: data (list).

        Raises:
            requests.HTTPError: HTTP error.
        """
        path = TCBSConstants.endpoint("derivatives")
        logger.debug("get_derivatives — fetching derivative market board")

        try:
            result = self._get(path)  # type: ignore[attr-defined]
            data = result.get("data", [])
            logger.debug(
                "get_derivatives — returned %d contracts",
                len(data) if isinstance(data, list) else "N/A",
            )
            return result
        except Exception as exc:
            logger.error("get_derivatives failed: %s", exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        reraise=True,
        after=_log_retry,
    )
    def get_put_through_snaps(self) -> Dict[str, Any]:
        """
        Lấy thông tin giao dịch thỏa thuận (put-through transactions).

        Returns:
            Dict với put-through data từ TCBS.

        Raises:
            requests.HTTPError: HTTP error.
        """
        path = TCBSConstants.endpoint("put_through_snaps")
        logger.debug("get_put_through_snaps — fetching put-through data")

        try:
            result = self._get(path)  # type: ignore[attr-defined]
            logger.debug("get_put_through_snaps — success")
            return result
        except Exception as exc:
            logger.error("get_put_through_snaps failed: %s", exc)
            raise
