"""
vps_client.py — VPS Securities REST API Client

Lấy market data từ sàn chứng khoán VPS (HOSE only, scope hiện tại).
Không cần authentication cho public market data endpoints.

References:
    - REUSE pattern từ trading-bot-finance-service/pkg/sources/vps/vps.go (~60% adapt)
    - VPS API base: https://bgapidatafeed.vps.com.vn
"""
from __future__ import annotations

import os
from typing import Dict, List

import requests
from tenacity import retry, stop_after_attempt, wait_exponential


class VPSRestClient:
    """
    VPS Securities REST API client với retry tích hợp.

    Scope hiện tại: chỉ HOSE (HNX/UPCOM removed từ constants).
    VPS không có historical data API → chỉ snapshot hiện tại.
    Để accumulate data, chạy script này mỗi 1 phút trong giờ giao dịch (9:00–14:45).
    """

    def __init__(
        self,
        base_url: str = "https://bgapidatafeed.vps.com.vn",
        request_timeout: int = 10,
    ):
        self.base_url = base_url.rstrip("/")
        self.request_timeout = request_timeout
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def get_stock_list(self, exchange: str = "HOSE") -> List[Dict]:
        """
        Lấy danh sách tất cả cổ phiếu trên sàn.

        Args:
            exchange: "HOSE" (default, scope hiện tại)

        Returns:
            list of {stockCode, stockName, ...}
        """
        resp = self.session.get(
            f"{self.base_url}/v1/hq/getAllStock",
            params={"exchange": exchange},
            timeout=self.request_timeout,
        )
        resp.raise_for_status()
        return resp.json()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def get_market_data(self, symbols: List[str], batch_size: int = 200) -> List[Dict]:
        """
        Lấy market data realtime cho danh sách cổ phiếu.

        VPS API nhận tối đa ~200 symbols/request → tự động chunk thành batches.

        Args:
            symbols: danh sách mã cổ phiếu, e.g. ["VCB", "VHM", "FPT"]
            batch_size: số symbols tối đa mỗi request (mặc định 200)

        Returns:
            list of market data dict từ VPS API
        """
        all_data: List[Dict] = []
        for i in range(0, len(symbols), batch_size):
            chunk = symbols[i : i + batch_size]
            resp = self.session.get(
                f"{self.base_url}/v1/hq/getStockData",
                params={"listShareCode": ",".join(chunk)},
                timeout=self.request_timeout,
            )
            resp.raise_for_status()
            all_data.extend(resp.json())
        return all_data

    @classmethod
    def from_env(cls) -> "VPSRestClient":
        """Factory method tạo client từ biến môi trường."""
        return cls(
            base_url=os.getenv("VPS_BASE_URL", "https://bgapidatafeed.vps.com.vn"),
        )
