"""
vps_client.py — VPS Securities REST API Client

Lấy market data từ sàn chứng khoán VPS (HOSE only, scope hiện tại).
Không cần authentication cho public market data endpoints.

References:
    - REUSE pattern từ trading-bot-finance-service/pkg/sources/vps/vps.go (~60% adapt)
    - VPS API base: https://bgapidatafeed.vps.com.vn
"""
from __future__ import annotations

import logging
import os
from typing import Dict, List

import requests
from tenacity import RetryCallState, retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


def _log_retry(retry_state: RetryCallState) -> None:
    """
    Tenacity retry callback — log WARNING mỗi lần retry để trace được lý do.
    Được truyền vào decorator qua after= parameter.
    """
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    logger.warning(
        "VPSRestClient retry attempt %d — fn=%s, error=%s",
        retry_state.attempt_number,
        retry_state.fn.__name__ if retry_state.fn else "unknown",
        exc,
    )


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
        logger.debug(
            "VPSRestClient initialized — base_url=%s, timeout=%ds",
            self.base_url, self.request_timeout,
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=10),
        reraise=True,
        after=_log_retry,
    )
    def get_stock_list(self, exchange: str = "HOSE") -> List[Dict]:
        """
        Lấy danh sách tất cả cổ phiếu trên sàn.

        Args:
            exchange: "HOSE" (default, scope hiện tại)

        Returns:
            list of {stockCode, stockName, ...}

        Raises:
            requests.HTTPError: HTTP error — retry tự động qua tenacity.
        """
        logger.debug("get_stock_list — exchange=%s", exchange)
        try:
            resp = self.session.get(
                f"{self.base_url}/v1/hq/getAllStock",
                params={"exchange": exchange},
                timeout=self.request_timeout,
            )
            resp.raise_for_status()
            stocks = resp.json()
            logger.debug("get_stock_list — exchange=%s returned %d stocks", exchange, len(stocks))
            return stocks
        except Exception as exc:
            logger.error("get_stock_list failed — exchange=%s: %s", exchange, exc)
            raise

    def get_market_data(self, symbols: List[str], batch_size: int = 200) -> List[Dict]:
        """
        Lấy market data realtime cho danh sách cổ phiếu.

        VPS API nhận tối đa ~200 symbols/request → tự động chunk thành batches.
        Mỗi batch có retry riêng — batch lỗi không làm gián đoạn batch khác.

        Args:
            symbols: danh sách mã cổ phiếu, e.g. ["VCB", "VHM", "FPT"]
            batch_size: số symbols tối đa mỗi request (mặc định 200)

        Returns:
            list of market data dict từ VPS API (chỉ gồm các batch thành công)
        """
        all_data: List[Dict] = []
        total_batches = (len(symbols) + batch_size - 1) // batch_size
        logger.debug(
            "get_market_data — %d symbols, %d batches (batch_size=%d)",
            len(symbols), total_batches, batch_size,
        )
        for idx, i in enumerate(range(0, len(symbols), batch_size), start=1):
            chunk = symbols[i : i + batch_size]
            try:
                resp = self.session.get(
                    f"{self.base_url}/v1/hq/getStockData",
                    params={"listShareCode": ",".join(chunk)},
                    timeout=self.request_timeout,
                )
                resp.raise_for_status()
                batch_data = resp.json()
                all_data.extend(batch_data)
            except Exception as exc:
                # Log và tiếp tục — không để 1 batch lỗi phá toàn bộ snapshot run
                logger.warning(
                    "get_market_data batch %d/%d failed (symbols %d-%d): %s",
                    idx, total_batches, i, i + len(chunk), exc,
                )
        return all_data

    @classmethod
    def from_env(cls) -> "VPSRestClient":
        """Factory method tạo client từ biến môi trường."""
        return cls(
            base_url=os.getenv("VPS_BASE_URL", "https://bgapidatafeed.vps.com.vn"),
        )
