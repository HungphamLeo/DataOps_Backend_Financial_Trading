"""
okx_client.py — OKX REST API Client

Hỗ trợ cả Public (không cần auth) và Private (HMAC-SHA256 signed) endpoints.

Authentication flow cho private endpoints:
    1. Lấy UTC timestamp theo ISO 8601 (e.g. "2024-01-15T09:30:00.123Z")
    2. Concatenate: timestamp + METHOD + request_path + body
    3. HMAC-SHA256 sign với secret_key → Base64 encode
    4. Gắn vào headers: OK-ACCESS-KEY, OK-ACCESS-SIGN, OK-ACCESS-TIMESTAMP, OK-ACCESS-PASSPHRASE

Rate limits (OKX public API):
    - /api/v5/market/history-candles: 20 req / 2s per IP
    - Client này enforce 100ms min interval giữa các requests (đọc từ config)
    - Retry: 5 lần với exponential backoff (1s → 2s → 4s → max 30s)

References:
    - OKX API docs: https://www.okx.com/docs-v5/en/
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential


class OKXRestClient:
    """
    OKX REST API client với rate limiting và retry tích hợp.

    Tham số rate_limit_interval và retry được inject từ okx_config.yaml
    thông qua caller (bronze_flow / backfill_flow), không hardcode tại đây.
    """

    def __init__(
        self,
        base_url: str = "https://www.okx.com/api/v5",
        api_key: str = "",
        secret_key: str = "",
        passphrase: str = "",
        is_simulate: bool = False,
        rate_limit_interval: float = 0.1,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.is_simulate = is_simulate
        self._rate_limit_interval = rate_limit_interval
        self._last_request_time: float = 0.0

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "okx-vps-platform/1.0",
            "Accept": "application/json",
        })

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _sign(self, timestamp: str, method: str, request_path: str, body: str = "") -> str:
        """
        Tạo chữ ký HMAC-SHA256 theo chuẩn OKX.

        Args:
            timestamp: ISO 8601 UTC, e.g. "2024-01-15T09:30:00.123Z"
            method: HTTP method viết hoa, e.g. "GET"
            request_path: path + query string, e.g. "/api/v5/market/history-candles?instId=BTC-USDT"
            body: JSON body string cho POST, "" cho GET

        Returns:
            Base64-encoded HMAC-SHA256 signature.
        """
        message = timestamp + method.upper() + request_path + body
        mac = hmac.new(
            self.secret_key.encode("utf-8"),
            message.encode("utf-8"),
            digestmod=hashlib.sha256,
        )
        return base64.b64encode(mac.digest()).decode("utf-8")

    def _build_headers(self, method: str, request_path: str, body: str = "") -> Dict[str, str]:
        """
        Build HTTP headers cho OKX request.
        - Public (api_key rỗng): chỉ Content-Type và Accept.
        - Private: thêm 4 OKX auth headers.
        - Simulated trading: thêm x-simulated-trading: 1.
        """
        ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.is_simulate:
            headers["x-simulated-trading"] = "1"
        if self.api_key and self.secret_key:
            headers["OK-ACCESS-KEY"] = self.api_key
            headers["OK-ACCESS-SIGN"] = self._sign(ts, method, request_path, body)
            headers["OK-ACCESS-TIMESTAMP"] = ts
            headers["OK-ACCESS-PASSPHRASE"] = self.passphrase
        return headers

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
        Lấy OHLCV candles từ /api/v5/market/history-candles.

        Args:
            inst_id: e.g. "BTC-USDT", "ETH-USDT-SWAP"
            bar: timeframe "1m","5m","1H","1D"
            after: Unix ms — lấy candles SAU timestamp này (cursor-based pagination)
            before: Unix ms — lấy candles TRƯỚC timestamp này
            limit: max 300 candles/request

        Returns:
            list of [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
        """
        self._throttle()
        path = "/api/v5/market/history-candles"
        params: Dict[str, str] = {"instId": inst_id, "bar": bar, "limit": str(limit)}
        if after is not None:
            params["after"] = str(after)
        if before is not None:
            params["before"] = str(before)

        resp = self.session.get(
            f"{self.base_url}/market/history-candles",
            params=params,
            headers=self._build_headers("GET", path),
            timeout=15,
        )
        resp.raise_for_status()

        data = resp.json()
        if data.get("code") != "0":
            raise ValueError(f"OKX API error: {data.get('msg')} (code={data.get('code')})")
        return data.get("data", [])

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def get_instruments(self, inst_type: str = "SPOT") -> List[Dict[str, Any]]:
        """Lấy danh sách tất cả instruments theo loại (SPOT | SWAP | FUTURES)."""
        self._throttle()
        path = "/api/v5/public/instruments"
        resp = self.session.get(
            f"{self.base_url}/public/instruments",
            params={"instType": inst_type},
            headers=self._build_headers("GET", path),
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != "0":
            raise ValueError(f"OKX instruments error: {data.get('msg')}")
        return data.get("data", [])

    @classmethod
    def from_env(cls) -> "OKXRestClient":
        """Factory method tạo client từ biến môi trường."""
        return cls(
            api_key=os.getenv("OKX_API_KEY", ""),
            secret_key=os.getenv("OKX_SECRET_KEY", ""),
            passphrase=os.getenv("OKX_PASSPHRASE", ""),
        )
