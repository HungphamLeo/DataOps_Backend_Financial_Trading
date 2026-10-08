"""
future/account.py — Binance USD-M Futures Account & Trading mixin (Private Endpoints).

Phạm trù: UM Futures / Account & Trading
    - get_account()      : Thông tin tài khoản futures — balances, positions, assets.
    - get_positions()    : Thông tin vị thế đang mở (unrealized P&L, entry price, leverage).
    - get_my_trades()    : Lịch sử giao dịch futures theo symbol.
    - get_order()        : Truy vấn trạng thái 1 lệnh futures cụ thể.
    - get_orders()       : Tất cả lệnh futures (history) theo symbol.
    - get_open_orders()  : Các lệnh futures đang mở.
    - place_order()      : Đặt lệnh futures market hoặc limit.
    - cancel_order()     : Huỷ lệnh futures.

Tất cả endpoints PRIVATE — cần API Key + HMAC-SHA256 signature.
Retry: 3 lần cho query, KHÔNG retry cho write (place/cancel).

UM Futures Account specifics (khác Spot):
    - Margin mode: Cross hoặc Isolated (per symbol)
    - Leverage: 1x – 125x (per symbol)
    - Position side: BOTH (one-way) | LONG/SHORT (hedge mode)
    - Balance: tính theo USDT (không phải base asset)
    - Unrealized PNL: P&L chưa realize tính theo mark price

QUAN TRỌNG — phạm vi dự án:
    - get_account + get_positions: dùng cho analytics, risk monitoring.
    - Order management: hỗ trợ đầy đủ nhưng production trading cần thêm risk controls.
    - dataops_batch_api chủ yếu dùng public endpoints (klines, funding_rate).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from tenacity import retry, stop_after_attempt, wait_exponential

from log.logger_setup import logger_manager
from cli.ingestion.sources.client.binance_client._retry import _log_retry

logger = logger_manager.get_logger(__name__)


class FuturesAccountMixin:
    """
    Phạm trù UM Futures / Account & Trading — private endpoints cần API Key + Signature.

    Kế thừa bởi BinanceFuturesClient. Dùng self._sdk (binance.spot.Spot với fapi base_url).
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
        Lấy thông tin tài khoản UM Futures.

        Returns:
            Dict với keys:
                feeTier, canTrade, canDeposit, canWithdraw, updateTime,
                totalInitialMargin, totalMaintMargin, totalWalletBalance,
                totalUnrealizedProfit, totalMarginBalance,
                assets (list of {asset, walletBalance, unrealizedProfit, ...}),
                positions (list of all current positions).

        Raises:
            requests.HTTPError: HTTP error.
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Futures.get_account — fetching account info")
        try:
            result = self._sdk.account(**kwargs)  # type: ignore[attr-defined]
            positions_open = [p for p in result.get("positions", []) if float(p.get("positionAmt", 0)) != 0]
            logger.debug(
                "Futures.get_account — totalWalletBalance=%s, %d open positions",
                result.get("totalWalletBalance"), len(positions_open),
            )
            return result
        except Exception as exc:
            logger.error("Futures.get_account failed: %s", exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_positions(
        self,
        symbol: Optional[str] = None,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """
        Lấy thông tin vị thế đang mở (position risk).

        Args:
            symbol: Filter 1 symbol. None → tất cả symbols có position.

        Returns:
            List of dict với: symbol, positionAmt, entryPrice, markPrice,
            unRealizedProfit, liquidationPrice, leverage, marginType,
            isolatedMargin, positionSide, notional, maxNotionalValue.

        Note:
            positionAmt > 0 = long position, < 0 = short position, = 0 = no position.
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Futures.get_positions — symbol=%s", symbol or "all")
        try:
            params: Dict[str, Any] = {}
            if symbol:
                params["symbol"] = symbol
            params.update(kwargs)
            # positionRisk endpoint
            result = self._sdk.query(  # type: ignore[attr-defined]
                "/fapi/v2/positionRisk",
                payload=params,
                signed=True,
            )
            open_positions = [p for p in result if float(p.get("positionAmt", 0)) != 0]
            logger.debug(
                "Futures.get_positions — %d total, %d open",
                len(result), len(open_positions),
            )
            return result
        except Exception as exc:
            logger.error("Futures.get_positions failed — symbol=%s: %s", symbol, exc)
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
        Lấy lịch sử giao dịch futures theo symbol.

        Args:
            symbol:     Mã futures, e.g. "BTCUSDT".
            start_time: Unix ms.
            end_time:   Unix ms.
            from_id:    Trade ID bắt đầu (pagination).
            limit:      Max 1000 (default 500).

        Returns:
            List of dict: symbol, id, orderId, side, price, qty, realizedPnl,
            marginAsset, quoteQty, commission, commissionAsset, time,
            positionSide, buyer, maker.
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

        logger.debug("Futures.get_my_trades — symbol=%s, limit=%d", symbol, limit)
        try:
            result = self._sdk.my_trades(symbol, **params)  # type: ignore[attr-defined]
            logger.debug("Futures.get_my_trades — %s returned %d trades", symbol, len(result))
            return result
        except Exception as exc:
            logger.error("Futures.get_my_trades failed — symbol=%s: %s", symbol, exc)
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
        Truy vấn trạng thái 1 lệnh futures cụ thể.

        Args:
            symbol:               Mã futures.
            order_id:             Binance order ID.
            orig_client_order_id: Client order ID.

        Returns:
            Dict với: orderId, symbol, status, type, side, price, origQty,
            executedQty, avgPrice, positionSide, time, updateTime.

        Raises:
            ValueError: thiếu orderId hoặc origClientOrderId.
        """
        if order_id is None and orig_client_order_id is None:
            raise ValueError("Futures.get_order: phải truyền order_id hoặc orig_client_order_id.")

        self._throttle()  # type: ignore[attr-defined]
        params: Dict[str, Any] = {}
        if order_id is not None:
            params["orderId"] = order_id
        if orig_client_order_id is not None:
            params["origClientOrderId"] = orig_client_order_id
        params.update(kwargs)

        logger.debug("Futures.get_order — symbol=%s, orderId=%s", symbol, order_id)
        try:
            result = self._sdk.get_order(symbol, **params)  # type: ignore[attr-defined]
            logger.debug(
                "Futures.get_order — %s orderId=%s status=%s",
                symbol, order_id, result.get("status"),
            )
            return result
        except Exception as exc:
            logger.error("Futures.get_order failed — symbol=%s orderId=%s: %s", symbol, order_id, exc)
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
        Lấy tất cả lệnh futures (history) theo symbol.

        Args:
            symbol:     Mã futures.
            start_time: Unix ms.
            end_time:   Unix ms.
            limit:      Max 1000 (default 500).

        Returns:
            List of order dicts, ASC theo time.
        """
        self._throttle()  # type: ignore[attr-defined]
        params: Dict[str, Any] = {"limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        params.update(kwargs)

        logger.debug("Futures.get_orders — symbol=%s, limit=%d", symbol, limit)
        try:
            result = self._sdk.get_orders(symbol, **params)  # type: ignore[attr-defined]
            logger.debug("Futures.get_orders — %s returned %d orders", symbol, len(result))
            return result
        except Exception as exc:
            logger.error("Futures.get_orders failed — symbol=%s: %s", symbol, exc)
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
        Lấy danh sách lệnh futures đang mở (chưa fill, chưa cancel).

        Args:
            symbol: Filter theo symbol. None → tất cả.

        Returns:
            List of open order dicts.
        """
        self._throttle()  # type: ignore[attr-defined]
        logger.debug("Futures.get_open_orders — symbol=%s", symbol or "all")
        try:
            result = self._sdk.get_open_orders(symbol=symbol, **kwargs)  # type: ignore[attr-defined]
            logger.debug("Futures.get_open_orders — %s: %d open orders", symbol or "all", len(result))
            return result
        except Exception as exc:
            logger.error("Futures.get_open_orders failed — symbol=%s: %s", symbol, exc)
            raise

    def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Optional[float] = None,
        price: Optional[float] = None,
        position_side: str = "BOTH",
        time_in_force: str = "GTC",
        reduce_only: bool = False,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Đặt lệnh UM Futures.

        KHÔNG có retry — đặt lệnh là action có side effect.

        Args:
            symbol:        e.g. "BTCUSDT".
            side:          "BUY" | "SELL".
            order_type:    "MARKET" | "LIMIT" | "STOP_MARKET" | "TAKE_PROFIT_MARKET" | ...
            quantity:      Số lượng contract.
            price:         Giá limit (bắt buộc với LIMIT).
            position_side: "BOTH" (one-way mode) | "LONG" | "SHORT" (hedge mode).
            time_in_force: "GTC" | "IOC" | "FOK" | "GTX" — chỉ cho LIMIT.
            reduce_only:   True = chỉ reduce position, không mở mới.

        Returns:
            Dict order response: orderId, symbol, status, avgPrice, executedQty, ...

        Raises:
            ValueError: LIMIT order thiếu price.
        """
        if order_type == "LIMIT" and price is None:
            raise ValueError("Futures.place_order: LIMIT order cần truyền price.")
        if quantity is None:
            raise ValueError("Futures.place_order: cần truyền quantity.")

        self._throttle()  # type: ignore[attr-defined]
        params: Dict[str, Any] = {
            "quantity": quantity,
            "positionSide": position_side,
        }
        if order_type == "LIMIT":
            params["price"] = str(price)
            params["timeInForce"] = time_in_force
        if reduce_only:
            params["reduceOnly"] = "true"
        params.update(kwargs)

        logger.info(
            "Futures.place_order — symbol=%s side=%s type=%s qty=%s price=%s reduceOnly=%s",
            symbol, side, order_type, quantity, price, reduce_only,
        )
        try:
            result = self._sdk.new_order(symbol, side, order_type, **params)  # type: ignore[attr-defined]
            logger.info(
                "Futures.place_order — orderId=%s status=%s avgPrice=%s",
                result.get("orderId"), result.get("status"), result.get("avgPrice"),
            )
            return result
        except Exception as exc:
            logger.error(
                "Futures.place_order failed — symbol=%s side=%s type=%s: %s",
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
        Huỷ 1 lệnh futures.

        KHÔNG có retry — cancel là action có side effect.

        Args:
            symbol:               Mã futures.
            order_id:             Binance order ID.
            orig_client_order_id: Client order ID.

        Returns:
            Dict cancel response với status.

        Raises:
            ValueError: thiếu orderId hoặc origClientOrderId.
        """
        if order_id is None and orig_client_order_id is None:
            raise ValueError("Futures.cancel_order: phải truyền order_id hoặc orig_client_order_id.")

        self._throttle()  # type: ignore[attr-defined]
        params: Dict[str, Any] = {}
        if order_id is not None:
            params["orderId"] = order_id
        if orig_client_order_id is not None:
            params["origClientOrderId"] = orig_client_order_id
        params.update(kwargs)

        logger.info("Futures.cancel_order — symbol=%s orderId=%s", symbol, order_id)
        try:
            result = self._sdk.cancel_order(symbol, **params)  # type: ignore[attr-defined]
            logger.info("Futures.cancel_order — orderId=%s status=%s", order_id, result.get("status"))
            return result
        except Exception as exc:
            logger.error("Futures.cancel_order failed — symbol=%s orderId=%s: %s", symbol, order_id, exc)
            raise
