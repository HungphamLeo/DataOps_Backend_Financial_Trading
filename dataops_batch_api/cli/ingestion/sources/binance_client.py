"""
binance_client.py — Binance REST API Client

Hỗ trợ cả Public (không cần auth) và Private (HMAC-SHA256 signed) endpoints.

Authentication flow cho private endpoints:
    1. Tạo query string với timestamp (Unix ms)
    2. HMAC-SHA256 sign toàn bộ query string với secret_key
    3. Gắn signature vào query params: &signature=...
    4. Gắn API key vào header: X-MBX-APIKEY

Rate limits (Binance public API):
    - /api/v3/klines: 1200 requests / 1 phút per IP
    - Client này enforce 100ms min interval giữa các requests (đọc từ config)
    - Retry: 5 lần với exponential backoff (1s → 2s → 4s → max 30s)
    - Tenacity retry hook log WARNING mỗi lần retry, ERROR khi hết attempt

References:
    - Binance API docs: https://binance-docs.github.io/apidocs/spot/en/
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from tenacity import (
    RetryCallState,
    retry,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


def _log_retry(retry_state: RetryCallState) -> None:
    """
    Tenacity retry callback — log WARNING mỗi lần retry để trace được lý do.
    Được truyền vào decorator qua after= parameter.
    """
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    logger.warning(
        "BinanceRestClient retry attempt %d — fn=%s, error=%s",
        retry_state.attempt_number,
        retry_state.fn.__name__ if retry_state.fn else "unknown",
        exc,
    )


class BinanceRestClient:
    """
    Binance REST API client với rate limiting, retry và structured logging.

    Tham số rate_limit_interval và retry được inject từ exchange_config.yaml
    thông qua caller (bronze_flow / backfill_flow), không hardcode tại đây.

    Logging:
        - DEBUG: mỗi request gửi đi (method, path, params)
        - WARNING: mỗi lần retry (qua _log_retry callback)
        - ERROR: khi request thất bại sau hết tất cả retry attempts
    """

    def __init__(
        self,
        base_url: str = "https://api.binance.com/api/v3",
        api_key: str = "",
        secret_key: str = "",
        is_testnet: bool = False,
        rate_limit_interval: float = 0.1,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.secret_key = secret_key
        self.is_testnet = is_testnet
        self._rate_limit_interval = rate_limit_interval
        self._last_request_time: float = 0.0

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "binance-vps-platform/1.0",
            "Accept": "application/json",
        })
        if self.api_key:
            self.session.headers.update({"X-MBX-APIKEY": self.api_key})
        logger.debug(
            "BinanceRestClient initialized — base_url=%s, testnet=%s, rate_limit=%.3fs",
            self.base_url, self.is_testnet, self._rate_limit_interval,
        )

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _sign(self, query_string: str) -> str:
        """
        Tạo chữ ký HMAC-SHA256 theo chuẩn Binance.

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

    def _build_signed_params(self, params: Dict[str, str]) -> Dict[str, str]:
        """
        Thêm timestamp và signature vào params cho private endpoints.
        - Public endpoints: không cần gọi hàm này.
        - Private endpoints: phải có timestamp + signature.
        """
        params["timestamp"] = str(int(time.time() * 1000))
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        params["signature"] = self._sign(query_string)
        return params

    # ── Rate limiting ──────────────────────────────────────────────────────────

    def _throttle(self) -> None:
        """Đợi đủ thời gian giữa 2 request để tránh rate limit."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._rate_limit_interval:
            time.sleep(self._rate_limit_interval - elapsed)
        self._last_request_time = time.time()

    # ── Public API ─────────────────────────────────────────────────────────────

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        reraise=True,
        after=_log_retry,
    )
    def get_candles(
        self,
        inst_id: str,
        bar: str = "1m",
        after: Optional[int] = None,
        before: Optional[int] = None,
        limit: int = 300,
    ) -> List[List[Any]]:
        """
        Lấy OHLCV candles từ /api/v3/klines.

        Args:
            inst_id: e.g. "BTCUSDT", "ETHUSDT"
            bar: timeframe "1m","5m","1h","1d"
            after:  Unix ms — startTime (lấy candles từ timestamp này trở đi)
            before: Unix ms — endTime (lấy candles đến timestamp này)
            limit: max 1000 candles/request (mặc định 300)

        Returns:
            list of [openTime, open, high, low, close, volume, closeTime,
                     quoteAssetVolume, numTrades, takerBuyBaseVolume,
                     takerBuyQuoteVolume, ignore]

        Raises:
            requests.HTTPError: HTTP 4xx/5xx — được retry tự động qua tenacity.
            ValueError: Binance API trả về lỗi — re-raise sau retry.
        """
        self._throttle()
        params: Dict[str, str] = {
            "symbol": inst_id,
            "interval": bar,
            "limit": str(limit),
        }
        if after is not None:
            params["startTime"] = str(after)
        if before is not None:
            params["endTime"] = str(before)

        logger.debug("get_candles — inst_id=%s, bar=%s, limit=%d", inst_id, bar, limit)
        try:
            resp = self.session.get(
                f"{self.base_url}/klines",
                params=params,
                timeout=15,
            )
            resp.raise_for_status()

            candles = resp.json()
            if isinstance(candles, dict) and candles.get("code"):
                raise ValueError(
                    f"Binance API error: {candles.get('msg')} (code={candles.get('code')})"
                )
            logger.debug("get_candles — %s returned %d candles", inst_id, len(candles))
            return candles
        except Exception as exc:
            logger.error(
                "get_candles failed — inst_id=%s, bar=%s: %s",
                inst_id, bar, exc,
            )
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_instruments(self, inst_type: str = "SPOT") -> List[Dict[str, Any]]:
        """
        Lấy danh sách tất cả instruments từ /api/v3/exchangeInfo.

        Args:
            inst_type: "SPOT" | "FUTURES" | "PERP" (dùng để filter symbols)

        Returns:
            list of symbol dict từ Binance exchangeInfo

        Raises:
            requests.HTTPError: HTTP error — retry tự động.
            ValueError: Binance API error.
        """
        self._throttle()
        logger.debug("get_instruments — inst_type=%s", inst_type)
        try:
            resp = self.session.get(
                f"{self.base_url}/exchangeInfo",
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            instruments = data.get("symbols", [])
            logger.debug("get_instruments — returned %d instruments", len(instruments))
            return instruments
        except Exception as exc:
            logger.error("get_instruments failed — inst_type=%s: %s", inst_type, exc)
            raise

    @classmethod
    def from_env(cls) -> "BinanceRestClient":
        """Factory method tạo client từ biến môi trường."""
        return cls(
            api_key=os.getenv("BINANCE_API_KEY", ""),
            secret_key=os.getenv("BINANCE_SECRET_KEY", ""),
        )
