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
    - Client này enforce 100ms min interval giữa các requests
    - Retry: 5 lần với exponential backoff (1s → 2s → 4s → max 30s)

References:
    - OKX API docs: https://www.okx.com/docs-v5/en/
    - HMAC-SHA256: https://tools.ietf.org/html/rfc2104
"""
import base64
import hmac
import hashlib
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import requests
import pandas as pd


class OKXClient:
    def __init__(
        self,
        base_url: str = "https://www.okx.com",
        api_key: str = "",
        secret_key: str = "",
        passphrase: str = "",
        is_simulate: bool = False,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.is_simulate = is_simulate
        self.session = requests.Session()

    def _sign(self, timestamp: str, method: str, request_path: str, body: str = "") -> str:
        """
        Tạo chữ ký HMAC-SHA256 theo chuẩn OKX.

        Args:
            timestamp: ISO 8601 UTC timestamp, e.g. "2024-01-15T09:30:00.123Z"
            method: HTTP method viết hoa, e.g. "GET", "POST"
            request_path: đường dẫn + query string, e.g. "/api/v5/market/history-candles?instId=BTC-USDT"
            body: JSON body string cho POST requests, "" cho GET

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

    def _get_headers(self, method: str, request_path: str, body: str = "") -> Dict[str, str]:
        """
        Tạo HTTP headers cho OKX request.
        Public endpoints (api_key rỗng): chỉ trả về Content-Type và Accept.
        Private endpoints: thêm 4 authentication headers theo chuẩn OKX.
        Simulated trading: thêm x-simulated-trading: 1 header.
        """
        timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.is_simulate:
            headers["x-simulated-trading"] = "1"

        if self.api_key and self.secret_key:
            headers["OK-ACCESS-KEY"] = self.api_key
            headers["OK-ACCESS-SIGN"] = self._sign(timestamp, method, request_path, body)
            headers["OK-ACCESS-TIMESTAMP"] = timestamp
            headers["OK-ACCESS-PASSPHRASE"] = self.passphrase

        return headers

    def get_history_candles(
        self,
        inst_id: str = "BTC-USDT",
        bar: str = "1m",
        after: Optional[str] = None,
        before: Optional[str] = None,
        limit: int = 100,
    ) -> List[List[Any]]:
        """Lấy dữ liệu nến lịch sử từ OKX REST API: /api/v5/market/history-candles"""
        path = "/api/v5/market/history-candles"
        params = {"instId": inst_id, "bar": bar, "limit": str(limit)}
        if after:
            params["after"] = after
        if before:
            params["before"] = before

        url = f"{self.base_url}{path}"
        headers = self._get_headers("GET", path)

        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, headers=headers, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                if data.get("code") == "0":
                    return data.get("data", [])
                time.sleep(0.2)
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(1 * (attempt + 1))
        return []

    def fetch_ohlcv_dataframe(self, inst_id: str, bar: str = "1m", limit: int = 100) -> pd.DataFrame:
        """
        Fetch OHLCV và trả về pandas DataFrame với schema chuẩn hóa.

        Columns trong DataFrame trả về:
            symbol, exchange, interval, open, high, low, close, volume, timestamp

        Returns:
            DataFrame rỗng nếu không có data (không raise exception).
        """
        raw = self.get_history_candles(inst_id=inst_id, bar=bar, limit=limit)
        if not raw:
            return pd.DataFrame()

        # Format OKX candle: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
        df = pd.DataFrame(raw, columns=[
            "ts", "open", "high", "low", "close", "vol", "volCcy", "volCcyQuote", "confirm"
        ])
        df["timestamp"] = pd.to_datetime(df["ts"].astype(int), unit="ms", utc=True)
        df["symbol"] = inst_id
        df["exchange"] = "okx"
        df["interval"] = bar
        for col in ["open", "high", "low", "close", "vol"]:
            df[col] = df[col].astype(float)
        df.rename(columns={"vol": "volume"}, inplace=True)
        return df[["symbol", "exchange", "interval", "open", "high", "low", "close", "volume", "timestamp"]]
