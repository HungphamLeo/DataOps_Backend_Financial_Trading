"""
spot/market_data.py — Binance Spot Market Data mixin (Public Endpoints).

Phạm trù: Spot / Market Data
    - get_klines()        : OHLCV candles (/api/v3/klines). row[7] = quoteAssetVolume.
    - get_exchange_info() : Symbol info, filters, rate limits (/api/v3/exchangeInfo).
    - get_depth()         : Order book snapshot (/api/v3/depth).
    - get_ticker_price()  : Latest price(s) (/api/v3/ticker/price).
    - get_ticker_24hr()   : 24h rolling stats (/api/v3/ticker/24hr).
    - get_book_ticker()   : Best bid/ask price+qty (/api/v3/ticker/bookTicker).
    - get_avg_price()     : Current average price (/api/v3/avgPrice).
    - get_agg_trades()    : Compressed/aggregated trade list (/api/v3/aggTrades).
    - get_trades()        : Recent trades (/api/v3/trades).

Tất cả endpoints là PUBLIC — không cần API key.
Retry: 5 lần, exponential backoff (1s → 30s).

Kline row format (index-based):
    [0]  openTime           Unix ms
    [1]  open               string
    [2]  high               string
    [3]  low                string
    [4]  close              string
    [5]  volume             string  (base asset, e.g. BTC)
    [6]  closeTime          Unix ms
    [7]  quoteAssetVolume   string  ← vol_ccy dùng trong Bronze pipeline
    [8]  numTrades          int
    [9]  takerBuyBaseVolume string
    [10] takerBuyQuoteVolume string
    [11] ignore             string
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from tenacity import retry, stop_after_attempt, wait_exponential

from log.logger_setup import logger_manager
from cli.ingestion.sources.client.binance_client._retry import _log_retry

logger = logger_manager.get_logger(__name__)


class SpotMarketDataMixin:
    """
    Phạm trù Spot / Market Data — public endpoints không cần API key.

    Kế thừa bởi BinanceSpotClient. Dùng self._sdk (binance.spot.Spot) và self._throttle().
    """

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
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
        Lấy OHLCV klines (candlestick) từ Binance Spot.

        Args:
            symbol:     e.g. "BTCUSDT", "ETHUSDT" — uppercase, không có dấu gạch.
            interval:   Timeframe: "1s","1m","3m","5m","15m","30m","1h","4h","1d","1w","1M".
            start_time: Unix ms — startTime (lấy candles từ đây).
            end_time:   Unix ms — endTime (lấy candles đến đây, dùng cho backfill).
            limit:      Số candles tối đa / request. Max = 1000 (default 500).

        Returns:
            List of 12-element arrays. QUAN TRỌNG: row[7] = quoteAssetVolume (vol_ccy).

        Raises:
            requests.HTTPError: HTTP 4xx/5xx — retry tự động.
            ValueError: Binance API error response.
        """
        self._throttle()  # type: ignore[attr-defined]
        params: Dict[str, Any] = {"limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time

        logger.debug(
            "Spot.get_klines — symbol=%s, interval=%s, limit=%d",
            symbol, interval, limit,
        )
        try:
            result = self._sdk.klines(symbol, interval, **params)  # type: ignore[attr-defined]
            if isinstance(result, dict) and result.get("code"):
                raise ValueError(
                    f"Binance API error: {result.get('msg')} (code={result.get('code')})"
                )
            logger.debug("Spot.get_klines — %s returned %d candles", symbol, len(result))
            return result
        except Exception as exc:
            logger.error("Spot.get_klines failed — symbol=%s interval=%s: %s", symbol, interval, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_exchange_info(
        self,
        symbol: Optional[str] = None,
        symbols: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Lấy thông tin sàn: symbols, filters, rate limits, trading rules.

        Args:
            symbol:  Filter cho 1 symbol cụ thể (e.g. "BTCUSDT").
            symbols: Filter cho nhiều symbols (e.g. ["BTCUSDT", "ETHUSDT"]).
            None / None → trả về toàn bộ exchangeInfo.

        Returns:
            Dict với keys: timezone, serverTime, rateLimits, symbols (list).
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Spot.get_exchange_info — symbol=%s", symbol or symbols or "all")
        try:
            result = self._sdk.exchange_info(symbol=symbol, symbols=symbols)  # type: ignore[attr-defined]
            logger.debug(
                "Spot.get_exchange_info — returned %d symbols",
                len(result.get("symbols", [])),
            )
            return result
        except Exception as exc:
            logger.error("Spot.get_exchange_info failed: %s", exc)
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
        Lấy order book snapshot (bid/ask levels).

        Args:
            symbol: e.g. "BTCUSDT".
            limit:  Số levels. Valid: 5, 10, 20, 50, 100, 500, 1000, 5000.

        Returns:
            Dict với keys: lastUpdateId, bids (list[price, qty]), asks (list[price, qty]).
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Spot.get_depth — symbol=%s, limit=%d", symbol, limit)
        try:
            result = self._sdk.depth(symbol, limit=limit)  # type: ignore[attr-defined]
            logger.debug(
                "Spot.get_depth — %s bids=%d asks=%d",
                symbol, len(result.get("bids", [])), len(result.get("asks", [])),
            )
            return result
        except Exception as exc:
            logger.error("Spot.get_depth failed — symbol=%s: %s", symbol, exc)
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
        Lấy giá hiện tại (latest price).

        Args:
            symbol:  1 symbol → trả về dict {"symbol": ..., "price": ...}.
            symbols: nhiều symbols → trả về list of dict.
            None / None → tất cả symbols.

        Returns:
            Dict hoặc list of dict.
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Spot.get_ticker_price — symbol=%s", symbol or symbols or "all")
        try:
            result = self._sdk.ticker_price(symbol=symbol, symbols=symbols)  # type: ignore[attr-defined]
            return result
        except Exception as exc:
            logger.error("Spot.get_ticker_price failed — symbol=%s: %s", symbol, exc)
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
        ticker_type: str = "FULL",
    ) -> Any:
        """
        Lấy 24h rolling window price change statistics.

        Args:
            symbol:      1 symbol.
            symbols:     Nhiều symbols.
            ticker_type: "FULL" (default) | "MINI" — MINI bỏ stats không cần thiết.

        Returns:
            Dict hoặc list of dict với: priceChange, priceChangePercent, lastPrice,
            volume, quoteVolume, highPrice, lowPrice, openPrice, count, ...
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Spot.get_ticker_24hr — symbol=%s type=%s", symbol or symbols or "all", ticker_type)
        try:
            result = self._sdk.ticker_24hr(symbol=symbol, symbols=symbols, type=ticker_type)  # type: ignore[attr-defined]
            return result
        except Exception as exc:
            logger.error("Spot.get_ticker_24hr failed — symbol=%s: %s", symbol, exc)
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
        Lấy best bid/ask price và quantity (top of order book).

        Args:
            symbol:  1 symbol.
            symbols: Nhiều symbols.
            None    → tất cả symbols.

        Returns:
            Dict hoặc list of dict với: symbol, bidPrice, bidQty, askPrice, askQty.
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Spot.get_book_ticker — symbol=%s", symbol or symbols or "all")
        try:
            result = self._sdk.book_ticker(symbol=symbol, symbols=symbols)  # type: ignore[attr-defined]
            return result
        except Exception as exc:
            logger.error("Spot.get_book_ticker failed — symbol=%s: %s", symbol, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_avg_price(self, symbol: str) -> Dict[str, Any]:
        """
        Lấy current average price (tính từ last 5 phút).

        Args:
            symbol: e.g. "BTCUSDT".

        Returns:
            Dict với keys: mins (int), price (string).
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Spot.get_avg_price — symbol=%s", symbol)
        try:
            result = self._sdk.avg_price(symbol)  # type: ignore[attr-defined]
            logger.debug("Spot.get_avg_price — %s price=%s", symbol, result.get("price"))
            return result
        except Exception as exc:
            logger.error("Spot.get_avg_price failed — symbol=%s: %s", symbol, exc)
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
        Lấy compressed/aggregated trade list (gộp cùng price+time → 1 record).

        Hữu ích hơn get_trades() vì loại bỏ duplicate ticks, giảm data volume.

        Args:
            symbol:     e.g. "BTCUSDT".
            start_time: Unix ms — từ thời điểm này.
            end_time:   Unix ms — đến thời điểm này.
            limit:      Max 1000 (default 500).

        Returns:
            List of dict. Mỗi record: a (agg trade id), p (price), q (qty),
            f (first trade id), l (last trade id), T (timestamp ms),
            m (buyer maker), M (best price match).
        """
        self._throttle()  # type: ignore[attr-defined]
        params: Dict[str, Any] = {"limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time

        logger.debug("Spot.get_agg_trades — symbol=%s, limit=%d", symbol, limit)
        try:
            result = self._sdk.agg_trades(symbol, **params)  # type: ignore[attr-defined]
            logger.debug("Spot.get_agg_trades — %s returned %d records", symbol, len(result))
            return result
        except Exception as exc:
            logger.error("Spot.get_agg_trades failed — symbol=%s: %s", symbol, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_trades(
        self,
        symbol: str,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """
        Lấy recent trade list (raw, không gộp).

        Args:
            symbol: e.g. "BTCUSDT".
            limit:  Max 1000 (default 500).

        Returns:
            List of dict. Mỗi record: id, price, qty, quoteQty, time,
            isBuyerMaker, isBestMatch.
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Spot.get_trades — symbol=%s, limit=%d", symbol, limit)
        try:
            result = self._sdk.trades(symbol, limit=limit)  # type: ignore[attr-defined]
            logger.debug("Spot.get_trades — %s returned %d trades", symbol, len(result))
            return result
        except Exception as exc:
            logger.error("Spot.get_trades failed — symbol=%s: %s", symbol, exc)
            raise
