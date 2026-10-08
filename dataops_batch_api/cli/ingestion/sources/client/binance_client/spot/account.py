"""
spot/account.py — Binance Spot Account & Trading mixin (Private Endpoints).

Phạm trù: Spot / Account & Trading
    - get_account()      : Thông tin tài khoản — balances, permissions, commission rates.
    - get_my_trades()    : Lịch sử giao dịch theo symbol.
    - get_order()        : Truy vấn trạng thái 1 lệnh cụ thể.
    - get_orders()       : Tất cả lệnh (all orders — history) theo symbol.
    - get_open_orders()  : Các lệnh đang mở (chưa khớp/cancel).
    - place_order()      : Đặt lệnh market hoặc limit.
    - cancel_order()     : Huỷ lệnh theo orderId hoặc origClientOrderId.
    - cancel_open_orders(): Huỷ toàn bộ lệnh đang mở của 1 symbol.

Tất cả endpoints PRIVATE — cần API Key + HMAC-SHA256 signature.
SDK tự động sign khi api_key + api_secret được set trong BinanceClientBase.
Retry: 3 lần, exponential backoff (1s → 10s).

QUAN TRỌNG — phạm vi sử dụng trong dự án này:
    - Account + my_trades: dùng cho analytics, P&L tracking.
    - Order management: hỗ trợ đầy đủ nhưng chỉ dùng trong account proxy context.
    - Batch API (dataops_batch_api) chủ yếu dùng public endpoints (klines).
    - Private endpoints dùng bởi dataops_backend_api (account proxy qua Backend).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from tenacity import retry, stop_after_attempt, wait_exponential

from log.logger_setup import logger_manager
from cli.ingestion.sources.client.binance_client._retry import _log_retry

logger = logger_manager.get_logger(__name__)


class SpotAccountMixin:
    """
    Phạm trù Spot / Account & Trading — private endpoints cần API Key + Signature.

    Kế thừa bởi BinanceSpotClient. Dùng self._sdk (binance.spot.Spot) và self._throttle().
    SDK tự động ký nếu api_key + api_secret đã được set.
    """

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_account(self, **kwargs: Any) -> Dict[str, Any]:
        """
        Lấy thông tin tài khoản Spot: balances, permissions, commission rates.

        Returns:
            Dict với keys:
                makerCommission, takerCommission, buyerCommission, sellerCommission,
                canTrade, canWithdraw, canDeposit, updateTime,
                accountType, balances (list of {asset, free, locked}), permissions.

        Raises:
            RuntimeError: nếu SDK không có api_key/api_secret.
            requests.HTTPError: HTTP 4xx/5xx.
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Spot.get_account — fetching account info")
        try:
            result = self._sdk.account(**kwargs)  # type: ignore[attr-defined]
            balances = [b for b in result.get("balances", []) if float(b.get("free", 0)) > 0 or float(b.get("locked", 0)) > 0]
            logger.debug(
                "Spot.get_account — %d non-zero balances",
                len(balances),
            )
            return result
        except Exception as exc:
            logger.error("Spot.get_account failed: %s", exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_my_trades(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        from_id: Optional[int] = None,
        limit: int = 500,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """
        Lấy lịch sử giao dịch (filled orders) theo symbol.

        Args:
            symbol:     Mã giao dịch, e.g. "BTCUSDT".
            start_time: Unix ms — từ thời điểm này.
            end_time:   Unix ms — đến thời điểm này.
            from_id:    Trade ID bắt đầu (pagination).
            limit:      Max 1000 (default 500).

        Returns:
            List of dict. Mỗi record: id, orderId, orderListId, price, qty,
            quoteQty, commission, commissionAsset, time, isBuyer, isMaker, isBestMatch.

        Raises:
            requests.HTTPError: HTTP error.
        """
        self._throttle()  # type: ignore[attr-defined]
        params: Dict[str, Any] = {"limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        if from_id is not None:
            params["fromId"] = from_id
        params.update(kwargs)

        logger.debug("Spot.get_my_trades — symbol=%s, limit=%d", symbol, limit)
        try:
            result = self._sdk.my_trades(symbol, **params)  # type: ignore[attr-defined]
            logger.debug("Spot.get_my_trades — %s returned %d trades", symbol, len(result))
            return result
        except Exception as exc:
            logger.error("Spot.get_my_trades failed — symbol=%s: %s", symbol, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        orig_client_order_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Truy vấn trạng thái 1 lệnh cụ thể.

        Phải truyền ít nhất 1 trong: order_id hoặc orig_client_order_id.

        Args:
            symbol:               Mã giao dịch.
            order_id:             Binance order ID.
            orig_client_order_id: Client order ID khi đặt lệnh.

        Returns:
            Dict với: symbol, orderId, status, type, side, price, origQty,
            executedQty, cumulativeQuoteQty, time, updateTime, ...

        Raises:
            ValueError: nếu không truyền orderId hoặc origClientOrderId.
        """
        if order_id is None and orig_client_order_id is None:
            raise ValueError("get_order: phải truyền order_id hoặc orig_client_order_id.")

        self._throttle()  # type: ignore[attr-defined]
        params: Dict[str, Any] = {}
        if order_id is not None:
            params["orderId"] = order_id
        if orig_client_order_id is not None:
            params["origClientOrderId"] = orig_client_order_id
        params.update(kwargs)

        logger.debug("Spot.get_order — symbol=%s, orderId=%s", symbol, order_id)
        try:
            result = self._sdk.get_order(symbol, **params)  # type: ignore[attr-defined]
            logger.debug(
                "Spot.get_order — %s orderId=%s status=%s",
                symbol, order_id, result.get("status"),
            )
            return result
        except Exception as exc:
            logger.error("Spot.get_order failed — symbol=%s orderId=%s: %s", symbol, order_id, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_orders(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """
        Lấy tất cả lệnh (history: NEW, FILLED, CANCELED, ...) theo symbol.

        Args:
            symbol:     Mã giao dịch.
            start_time: Unix ms — từ thời điểm này.
            end_time:   Unix ms — đến thời điểm này.
            limit:      Max 1000 (default 500).

        Returns:
            List of order dicts, sắp xếp theo time ASC.
        """
        self._throttle()  # type: ignore[attr-defined]
        params: Dict[str, Any] = {"limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        params.update(kwargs)

        logger.debug("Spot.get_orders — symbol=%s, limit=%d", symbol, limit)
        try:
            result = self._sdk.get_orders(symbol, **params)  # type: ignore[attr-defined]
            logger.debug("Spot.get_orders — %s returned %d orders", symbol, len(result))
            return result
        except Exception as exc:
            logger.error("Spot.get_orders failed — symbol=%s: %s", symbol, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_open_orders(
        self,
        symbol: Optional[str] = None,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """
        Lấy danh sách lệnh đang mở (chưa khớp, chưa cancel).

        Args:
            symbol: Lọc theo symbol. None → tất cả symbols (weight cao hơn).

        Returns:
            List of open order dicts.

        Note:
            Không có symbol → weight = 40 (heavy). Có symbol → weight = 3.
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Spot.get_open_orders — symbol=%s", symbol or "all")
        try:
            result = self._sdk.get_open_orders(symbol=symbol, **kwargs)  # type: ignore[attr-defined]
            logger.debug("Spot.get_open_orders — %s: %d open orders", symbol or "all", len(result))
            return result
        except Exception as exc:
            logger.error("Spot.get_open_orders failed — symbol=%s: %s", symbol, exc)
            raise

    def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Optional[float] = None,
        price: Optional[float] = None,
        time_in_force: str = "GTC",
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Đặt lệnh mua/bán Spot.

        KHÔNG có retry — đặt lệnh là action có side effect.
        Caller phải xử lý lỗi và quyết định retry thủ công.

        Args:
            symbol:        e.g. "BTCUSDT".
            side:          "BUY" | "SELL".
            order_type:    "MARKET" | "LIMIT" | "STOP_LOSS_LIMIT" | ...
            quantity:      Số lượng base asset (e.g. 0.001 BTC).
            price:         Giá limit (bắt buộc với LIMIT order).
            time_in_force: "GTC" (default) | "IOC" | "FOK" — chỉ cho LIMIT.

        Returns:
            Dict order response: orderId, status, transactTime, fills, ...

        Raises:
            ValueError: LIMIT order thiếu price hoặc quantity.
            requests.HTTPError: Binance từ chối lệnh.
        """
        if order_type == "LIMIT" and price is None:
            raise ValueError("place_order: LIMIT order cần truyền price.")
        if quantity is None:
            raise ValueError("place_order: cần truyền quantity.")

        self._throttle()  # type: ignore[attr-defined]
        params: Dict[str, Any] = {}
        if quantity is not None:
            params["quantity"] = quantity
        if order_type == "LIMIT":
            params["price"] = str(price)
            params["timeInForce"] = time_in_force
        params.update(kwargs)

        logger.info(
            "Spot.place_order — symbol=%s side=%s type=%s qty=%s price=%s",
            symbol, side, order_type, quantity, price,
        )
        try:
            result = self._sdk.new_order(symbol, side, order_type, **params)  # type: ignore[attr-defined]
            logger.info(
                "Spot.place_order — orderId=%s status=%s",
                result.get("orderId"), result.get("status"),
            )
            return result
        except Exception as exc:
            logger.error(
                "Spot.place_order failed — symbol=%s side=%s type=%s: %s",
                symbol, side, order_type, exc,
            )
            raise

    def cancel_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        orig_client_order_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Huỷ 1 lệnh theo orderId hoặc origClientOrderId.

        KHÔNG có retry — cancel là action có side effect.

        Args:
            symbol:               Mã giao dịch.
            order_id:             Binance order ID.
            orig_client_order_id: Client order ID.

        Returns:
            Dict cancel response.

        Raises:
            ValueError: không truyền orderId hoặc origClientOrderId.
        """
        if order_id is None and orig_client_order_id is None:
            raise ValueError("cancel_order: phải truyền order_id hoặc orig_client_order_id.")

        self._throttle()  # type: ignore[attr-defined]
        params: Dict[str, Any] = {}
        if order_id is not None:
            params["orderId"] = order_id
        if orig_client_order_id is not None:
            params["origClientOrderId"] = orig_client_order_id
        params.update(kwargs)

        logger.info("Spot.cancel_order — symbol=%s orderId=%s", symbol, order_id)
        try:
            result = self._sdk.cancel_order(symbol, **params)  # type: ignore[attr-defined]
            logger.info("Spot.cancel_order — orderId=%s status=%s", order_id, result.get("status"))
            return result
        except Exception as exc:
            logger.error("Spot.cancel_order failed — symbol=%s orderId=%s: %s", symbol, order_id, exc)
            raise

    def cancel_open_orders(self, symbol: str, **kwargs: Any) -> List[Dict[str, Any]]:
        """
        Huỷ toàn bộ lệnh đang mở của 1 symbol.

        KHÔNG có retry — cancel là action có side effect.

        Args:
            symbol: Mã giao dịch.

        Returns:
            List of cancelled order dicts.
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.info("Spot.cancel_open_orders — symbol=%s", symbol)
        try:
            result = self._sdk.cancel_open_orders(symbol, **kwargs)  # type: ignore[attr-defined]
            logger.info("Spot.cancel_open_orders — %s: cancelled %d orders", symbol, len(result))
            return result
        except Exception as exc:
            logger.error("Spot.cancel_open_orders failed — symbol=%s: %s", symbol, exc)
            raise
