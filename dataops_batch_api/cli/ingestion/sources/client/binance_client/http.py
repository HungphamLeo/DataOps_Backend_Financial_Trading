"""
http.py — BinanceClientBase: khởi tạo SDK client, signing helper, throttle, from_env.

Phạm trù: HTTP Core
    - BinanceClientBase.__init__()         : Khởi tạo binance-connector Spot instance + rate limit state.
    - BinanceClientBase._sign()            : HMAC-SHA256 hex signature (dùng cho custom raw requests nếu cần).
    - BinanceClientBase._build_signed_params(): Thêm timestamp + signature vào params dict.
    - BinanceClientBase._throttle()        : Rate-limit enforcement (100ms min interval).
    - BinanceClientBase.from_env()         : Factory method từ environment variables (12-Factor).

Design:
    - Wrap binance-connector SDK (binance.spot.Spot) — không dùng requests trực tiếp.
    - Spot và UM Futures dùng cùng Spot class nhưng khác base_url:
        * Spot     : https://api.binance.com
        * UM Futures: https://fapi.binance.com
    - SDK tự handle HMAC signing khi api_key + api_secret được set.
    - _sign() và _build_signed_params() giữ lại để backward-compat nếu cần gọi raw.
    - BinanceConstants đọc base_url từ binance_constants.yaml — không hardcode.

Env vars cho from_env():
    BINANCE_API_KEY     : API Key (header X-MBX-APIKEY)
    BINANCE_SECRET_KEY  : Secret Key (HMAC-SHA256 signing)
    BINANCE_TESTNET     : "true" → dùng testnet URLs (default: "false")
    BINANCE_RATE_LIMIT  : Min interval seconds giữa requests (default: "0.1")
"""
from __future__ import annotations

import hashlib
import hmac
import os
import time
from typing import Optional

from binance.spot import Spot

from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

# ─── Base URLs (mirror từ binance_constants.yaml) ─────────────────────────────
_SPOT_BASE_URL         = "https://api.binance.com"
_TESTNET_SPOT_BASE_URL = "https://testnet.binance.vision"



class BinanceClientBase:
    """
    Phạm trù HTTP Core — base class wrap binance-connector SDK.

    Quản lý:
        - SDK Spot instance (dùng cho cả spot và futures qua base_url)
        - Rate limiting (100ms min interval)
        - HMAC-SHA256 signing (backward-compat helper)
        - Factory method from_env()

    Subclass override:
        - BinanceSpotClient  : _sdk dùng spot base_url
        - BinanceFuturesClient: _sdk dùng fapi base_url
    """

    def __init__(
        self,
        base_url: str = _SPOT_BASE_URL,
        api_key: str = "",
        secret_key: str = "",
        is_testnet: bool = False,
        rate_limit_interval: float = 0.1,
        timeout: int = 15,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.secret_key = secret_key
        self.is_testnet = is_testnet
        self._rate_limit_interval = rate_limit_interval
        self._last_request_time: float = 0.0
        self.timeout = timeout

        # Khởi tạo binance-connector SDK Spot instance
        # api_secret = None nếu không có (public-only mode)
        self._sdk = Spot(
            api_key=api_key or None,
            api_secret=secret_key or None,
            base_url=self.base_url,
            timeout=timeout,
        )

        logger.debug(
            "BinanceClientBase initialized — base_url=%s, testnet=%s, rate_limit=%.3fs",
            self.base_url, self.is_testnet, self._rate_limit_interval,
        )

    # ── Rate limiting ──────────────────────────────────────────────────────────

    def _throttle(self) -> None:
        """Đợi đủ thời gian giữa 2 request để tránh rate limit."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._rate_limit_interval:
            time.sleep(self._rate_limit_interval - elapsed)
        self._last_request_time = time.time()

    # ── Auth / Signing helpers (backward-compat) ──────────────────────────────

    def _sign(self, query_string: str) -> str:
        """
        Tạo chữ ký HMAC-SHA256 theo chuẩn Binance (hex-encoded).

        SDK tự ký tự động khi api_secret được set.
        Method này giữ lại để dùng nếu cần custom raw signing.

        Args:
            query_string: toàn bộ query string (không có leading '?')

        Returns:
            Hex-encoded HMAC-SHA256 signature.
        """
        mac = hmac.new(
            self.secret_key.encode("utf-8"),
            query_string.encode("utf-8"),
            digestmod=hashlib.sha256,
        )
        return mac.hexdigest()

    def _build_signed_params(self, params: dict) -> dict:
        """
        Thêm timestamp và signature vào params cho private endpoints.

        SDK tự ký tự động — dùng method này chỉ khi cần custom signing ngoài SDK.
        """
        params["timestamp"] = str(int(time.time() * 1000))
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        params["signature"] = self._sign(query_string)
        return params

    # ── Factory ───────────────────────────────────────────────────────────────

    @classmethod
    def from_env(cls, is_futures: bool = False) -> "BinanceClientBase":
        """
        Factory method — tạo client từ environment variables (12-Factor App).

        Env vars:
            BINANCE_API_KEY    : API Key (optional cho public endpoints)
            BINANCE_SECRET_KEY : Secret Key (required cho private endpoints)
            BINANCE_TESTNET    : "true" → dùng testnet URLs
            BINANCE_RATE_LIMIT : Interval giây giữa requests (default: 0.1)

        Args:
            is_futures: True → dùng UM Futures base_url (fapi.binance.com)

        Returns:
            Configured BinanceClientBase (hoặc subclass).
        """
        is_testnet = os.getenv("BINANCE_TESTNET", "false").lower() == "true"

        base_url = _TESTNET_SPOT_BASE_URL if is_testnet else _SPOT_BASE_URL

        return cls(
            base_url=base_url,
            api_key=os.getenv("BINANCE_API_KEY", ""),
            secret_key=os.getenv("BINANCE_SECRET_KEY", ""),
            is_testnet=is_testnet,
            rate_limit_interval=float(os.getenv("BINANCE_RATE_LIMIT", "0.1")),
        )

    # ── Connectivity ──────────────────────────────────────────────────────────

    def ping(self) -> bool:
        """
        Kiểm tra kết nối tới Binance API.

        Returns:
            True nếu thành công.

        Raises:
            Exception: nếu không kết nối được.
        """
        self._throttle()
        self._sdk.ping()
        return True

    def server_time(self) -> Optional[int]:
        """
        Lấy server time từ Binance (Unix ms).

        Returns:
            Server time Unix ms, hoặc None nếu lỗi.
        """
        self._throttle()
        data = self._sdk.time()
        return data.get("serverTime")
