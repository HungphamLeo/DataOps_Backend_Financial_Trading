"""
future/market_data.py — Binance USD-M Futures Market Data mixin (Public Endpoints).

Phạm trù: UM Futures / Market Data
    - get_klines()        : OHLCV klines (/fapi/v1/klines). row[7] = quoteAssetVolume.
    - get_exchange_info() : Symbol info, contract specs, filters (/fapi/v1/exchangeInfo).
    - get_depth()         : Order book snapshot (/fapi/v1/depth).
    - get_ticker_price()  : Latest mark/index price (/fapi/v1/ticker/price).
    - get_ticker_24hr()   : 24h rolling window stats (/fapi/v1/ticker/24hr).
    - get_book_ticker()   : Best bid/ask (/fapi/v1/ticker/bookTicker).
    - get_funding_rate()  : Funding rate history (/fapi/v1/fundingRate).
    - get_mark_price()    : Mark price + funding rate snapshot (/fapi/v1/premiumIndex).
    - get_open_interest() : Open interest tại thời điểm hiện tại (/fapi/v1/openInterest).
    - get_agg_trades()    : Aggregated trade list (/fapi/v1/aggTrades).

Tất cả endpoints PUBLIC — không cần API key.
Retry: 5 lần cho klines, 3 lần cho các endpoint khác.

UM Futures specifics (khác Spot):
    - Base URL: https://fapi.binance.com
    - Symbol: "BTCUSDT" (không có PERP suffix — khác COINM)
    - Funding rate: mỗi 8 giờ (0:00, 8:00, 16:00 UTC)
    - Open interest: tổng contracts đang mở, thể hiện market sentiment
    - Mark price: giá tham chiếu cho liquidation, P&L tính theo mark price

Kline row format (giống Spot):
    [0]  openTime           Unix ms
    [7]  quoteAssetVolume   string ← vol_ccy dùng trong Bronze pipeline
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from log.logger_setup import logger_manager
from cli.ingestion.sources.client.binance_client._retry import _is_retryable, _log_retry

logger = logger_manager.get_logger(__name__)


class FuturesMarketDataMixin:
    """
    Phạm trù UM Futures / Market Data — public endpoints không cần API key.

    Kế thừa bởi BinanceFuturesClient. Dùng self._sdk (binance.spot.Spot với fapi base_url)
    và self._throttle().

    Note: binance-connector SDK dùng Spot class cho cả Futures — chỉ khác base_url.
    """

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
        after=_log_retry,
    )
    def get_klines(
        self,
        symbol: str,
        interval: str = "1m",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
    ) -> List[List[Any]]:
        """
        Lấy OHLCV klines từ UM Futures.

        Giống Spot klines nhưng trên fapi.binance.com — dữ liệu tương lai (futures).

        Args:
            symbol:     e.g. "BTCUSDT", "ETHUSDT" — UM Futures symbol.
            interval:   Timeframe: "1m","3m","5m","15m","30m","1h","4h","1d","1w","1M".
            start_time: Unix ms — startTime.
            end_time:   Unix ms — endTime (dùng cho backfill).
            limit:      Max 1500 (default 500) — Futures giới hạn cao hơn Spot.

        Returns:
            List of 12-element arrays. row[7] = quoteAssetVolume (vol_ccy).

        Raises:
            requests.HTTPError: HTTP error — retry tự động.
            ValueError: Binance API error response.
        """
        self._throttle()  # type: ignore[attr-defined]
        params: Dict[str, Any] = {"limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time

        logger.debug(
            "Futures.get_klines — symbol=%s, interval=%s, limit=%d",
            symbol, interval, limit,
        )
        try:
            result = self._sdk.klines(symbol, interval, **params)  # type: ignore[attr-defined]
            if isinstance(result, dict) and result.get("code"):
                raise ValueError(
                    f"Binance Futures API error: {result.get('msg')} (code={result.get('code')})"
                )
            logger.debug("Futures.get_klines — %s returned %d candles", symbol, len(result))
            return result
        except Exception as exc:
            logger.error("Futures.get_klines failed — symbol=%s interval=%s: %s", symbol, interval, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_exchange_info(self) -> Dict[str, Any]:
        """
        Lấy thông tin sàn UM Futures: symbols, contract specs, filters, rate limits.

        Returns:
            Dict với keys: timezone, serverTime, rateLimits, symbols (list).
            Mỗi symbol có thêm: contractType, deliveryDate, onboardDate,
            pricePrecision, quantityPrecision, baseAssetPrecision, quotePrecision.
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Futures.get_exchange_info")
        try:
            result = self._sdk.exchange_info()  # type: ignore[attr-defined]
            logger.debug(
                "Futures.get_exchange_info — returned %d symbols",
                len(result.get("symbols", [])),
            )
            return result
        except Exception as exc:
            logger.error("Futures.get_exchange_info failed: %s", exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_depth(
        self,
        symbol: str,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """
        Lấy order book snapshot UM Futures.

        Args:
            symbol: e.g. "BTCUSDT".
            limit:  Valid: 5, 10, 20, 50, 100, 500, 1000.

        Returns:
            Dict với keys: lastUpdateId, E (msg time), T (trans time),
            bids (list[price, qty]), asks (list[price, qty]).
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Futures.get_depth — symbol=%s, limit=%d", symbol, limit)
        try:
            result = self._sdk.depth(symbol, limit=limit)  # type: ignore[attr-defined]
            logger.debug(
                "Futures.get_depth — %s bids=%d asks=%d",
                symbol, len(result.get("bids", [])), len(result.get("asks", [])),
            )
            return result
        except Exception as exc:
            logger.error("Futures.get_depth failed — symbol=%s: %s", symbol, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_ticker_price(
        self,
        symbol: Optional[str] = None,
        symbols: Optional[List[str]] = None,
    ) -> Any:
        """
        Lấy giá mark price hoặc index price hiện tại.

        Args:
            symbol:  1 symbol → dict.
            symbols: Nhiều symbols → list of dict.
            None    → tất cả symbols.

        Returns:
            Dict hoặc list of dict với: symbol, price, time.
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Futures.get_ticker_price — symbol=%s", symbol or symbols or "all")
        try:
            result = self._sdk.ticker_price(symbol=symbol, symbols=symbols)  # type: ignore[attr-defined]
            return result
        except Exception as exc:
            logger.error("Futures.get_ticker_price failed — symbol=%s: %s", symbol, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_ticker_24hr(
        self,
        symbol: Optional[str] = None,
        symbols: Optional[List[str]] = None,
    ) -> Any:
        """
        Lấy 24h rolling window statistics UM Futures.

        Returns:
            Dict hoặc list với: priceChange, priceChangePercent, lastPrice,
            volume, quoteVolume, highPrice, lowPrice, openPrice, count.
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Futures.get_ticker_24hr — symbol=%s", symbol or symbols or "all")
        try:
            result = self._sdk.ticker_24hr(symbol=symbol, symbols=symbols)  # type: ignore[attr-defined]
            return result
        except Exception as exc:
            logger.error("Futures.get_ticker_24hr failed — symbol=%s: %s", symbol, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_book_ticker(
        self,
        symbol: Optional[str] = None,
        symbols: Optional[List[str]] = None,
    ) -> Any:
        """
        Lấy best bid/ask price và quantity (top of book) UM Futures.

        Returns:
            Dict hoặc list với: symbol, bidPrice, bidQty, askPrice, askQty, time.
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Futures.get_book_ticker — symbol=%s", symbol or symbols or "all")
        try:
            result = self._sdk.book_ticker(symbol=symbol, symbols=symbols)  # type: ignore[attr-defined]
            return result
        except Exception as exc:
            logger.error("Futures.get_book_ticker failed — symbol=%s: %s", symbol, exc)
            raise

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        reraise=True,
        after=_log_retry,
    )
    def get_funding_rate(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Lấy lịch sử funding rate (/fapi/v1/fundingRate).

        Funding rate xảy ra mỗi 8 giờ (0:00, 8:00, 16:00 UTC).
        Rate dương = long trả short; rate âm = short trả long.

        Args:
            symbol:     e.g. "BTCUSDT" — UM Futures symbol.
            start_time: Unix ms.
            end_time:   Unix ms.
            limit:      Max 1000 (default 100).

        Returns:
            List of dict: symbol, fundingTime (ms), fundingRate (string),
            markPrice (string, optional).

        Raises:
            requests.HTTPError: HTTP error.
        """
        self._throttle()  # type: ignore[attr-defined]
        params: Dict[str, Any] = {"limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time

        logger.debug("Futures.get_funding_rate — symbol=%s, limit=%d", symbol, limit)
        try:
            # Dùng query trực tiếp qua SDK send_request
            result = self._sdk.query(  # type: ignore[attr-defined]
                "/fapi/v1/fundingRate",
                payload={"symbol": symbol, **params},
            )
            if isinstance(result, dict) and result.get("code"):
                raise ValueError(
                    f"Binance Futures API error: {result.get('msg')} (code={result.get('code')})"
                )
            logger.debug("Futures.get_funding_rate — %s returned %d records", symbol, len(result))
            return result
        except Exception as exc:
            logger.error("Futures.get_funding_rate failed — symbol=%s: %s", symbol, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_mark_price(
        self,
        symbol: Optional[str] = None,
    ) -> Any:
        """
        Lấy mark price + funding rate snapshot hiện tại (/fapi/v1/premiumIndex).

        Mark price là giá tham chiếu để:
            - Tính P&L unrealized
            - Trigger liquidation
            - Settlement cuối kỳ

        Args:
            symbol: 1 symbol → dict. None → tất cả symbols (list).

        Returns:
            Dict hoặc list với: symbol, markPrice, indexPrice, estimatedSettlePrice,
            lastFundingRate, nextFundingTime, interestRate, time.
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Futures.get_mark_price — symbol=%s", symbol or "all")
        try:
            params: Dict[str, Any] = {}
            if symbol:
                params["symbol"] = symbol
            result = self._sdk.query(  # type: ignore[attr-defined]
                "/fapi/v1/premiumIndex",
                payload=params,
            )
            return result
        except Exception as exc:
            logger.error("Futures.get_mark_price failed — symbol=%s: %s", symbol, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_open_interest(self, symbol: str) -> Dict[str, Any]:
        """
        Lấy open interest hiện tại của 1 symbol (/fapi/v1/openInterest).

        Open interest = tổng số contracts đang mở (chưa settle).
        Tăng = thị trường đang mở thêm position (bullish/bearish pressure mới).
        Giảm = đóng position (giảm áp lực).

        Args:
            symbol: e.g. "BTCUSDT".

        Returns:
            Dict với: symbol, openInterest (string), time (ms).
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Futures.get_open_interest — symbol=%s", symbol)
        try:
            result = self._sdk.query(  # type: ignore[attr-defined]
                "/fapi/v1/openInterest",
                payload={"symbol": symbol},
            )
            logger.debug(
                "Futures.get_open_interest — %s OI=%s",
                symbol, result.get("openInterest"),
            )
            return result
        except Exception as exc:
            logger.error("Futures.get_open_interest failed — symbol=%s: %s", symbol, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_agg_trades(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """
        Lấy compressed/aggregated trade list UM Futures.

        Args:
            symbol:     e.g. "BTCUSDT".
            start_time: Unix ms.
            end_time:   Unix ms.
            limit:      Max 1000 (default 500).

        Returns:
            List of dict: a (agg trade id), p (price), q (qty), f, l,
            T (timestamp ms), m (buyer maker).
        """
        self._throttle()  # type: ignore[attr-defined]
        params: Dict[str, Any] = {"limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time

        logger.debug("Futures.get_agg_trades — symbol=%s, limit=%d", symbol, limit)
        try:
            result = self._sdk.agg_trades(symbol, **params)  # type: ignore[attr-defined]
            logger.debug("Futures.get_agg_trades — %s returned %d records", symbol, len(result))
            return result
        except Exception as exc:
            logger.error("Futures.get_agg_trades failed — symbol=%s: %s", symbol, exc)
            raise
