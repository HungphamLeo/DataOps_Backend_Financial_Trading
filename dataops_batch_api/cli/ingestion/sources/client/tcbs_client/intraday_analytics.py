"""
intraday_analytics.py — TCBS Intraday Analytics mixin (BSA + Tick History).

Phạm trù: Intraday Analytics / BSA (Buy-Sell Analysis)
    - get_intraday_bsa()       : Cung/cầu trong ngày theo loại nhà đầu tư (sheep/wolf/shark/all).
    - get_intraday_bsa_ext()   : Cung/cầu intraday theo granularity 15 phút.
    - get_intraday_bsa_month() : Cung/cầu tháng theo mã cổ phiếu.
    - get_intraday_history()   : Lịch sử khớp lệnh tick-by-tick (paginated).

Tất cả endpoints này là PUBLIC — không cần JWT token.
Retry: 3 lần, exponential backoff (1s → 15s).

TCBS iFlash API endpoints (từ TCBSConstants):
    intraday_bsa              : /tartarus/v1/{ticker}/intradayBsa
    intraday_bsa_ext          : /tartarus/v1/{ticker}/intradayBsaExt
    intraday_bsa_month        : /tartarus/v1/{ticker}/intradayBsaMonth
    intraday_history_paging   : /tartarus/v1/{ticker}/intradayHistoryPaging

Investor types (BSA):
    sheep  — nhà đầu tư nhỏ lẻ
    wolf   — nhà đầu tư vừa
    shark  — tổ chức lớn / quỹ
    all    — tổng hợp tất cả loại
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from tenacity import retry, stop_after_attempt, wait_exponential

from log.logger_setup import logger_manager
from cli.ingestion.sources.tcbs_constants_loader import TCBSConstants
from cli.ingestion.sources.client.tcbs_client._retry import _log_retry

logger = logger_manager.get_logger(__name__)


class TCBSIntradayAnalyticsMixin:
    """
    Phạm trù Intraday Analytics / BSA — Buy-Sell Analysis và tick history.

    Kế thừa bởi TCBSRestClient. Dùng self._get() từ TCBSClientBase.
    """

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        reraise=True,
        after=_log_retry,
    )
    def get_intraday_bsa(
        self,
        ticker: str,
        investor_type: str = "all",
    ) -> Dict[str, Any]:
        """
        Lấy dữ liệu cung/cầu trong ngày (Buy/Sell Analysis) cho 1 mã cổ phiếu.

        Phân loại nhà đầu tư: sheep (nhỏ lẻ), wolf (vừa), shark (tổ chức lớn), all.

        Args:
            ticker:        Mã cổ phiếu, e.g. "TCB", "VCB".
            investor_type: Loại nhà đầu tư từ bsa_investor_types trong YAML.

        Returns:
            Dict với keys: ticker, data (list of BSA records).

        Raises:
            ValueError: investor_type không hợp lệ.
            requests.HTTPError: HTTP error.
        """
        TCBSConstants.validate_bsa_investor_type(investor_type)

        path = TCBSConstants.endpoint("intraday_bsa").replace("{ticker}", ticker)
        params: Dict[str, Any] = {"type": investor_type}

        logger.debug("get_intraday_bsa — ticker=%s, investor_type=%s", ticker, investor_type)

        try:
            result = self._get(path, params=params)  # type: ignore[attr-defined]
            data = result.get("data", [])
            logger.debug(
                "get_intraday_bsa — ticker=%s returned %d BSA records",
                ticker,
                len(data) if isinstance(data, list) else "N/A",
            )
            return result
        except Exception as exc:
            logger.error(
                "get_intraday_bsa failed — ticker=%s, investor_type=%s: %s",
                ticker, investor_type, exc,
            )
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        reraise=True,
        after=_log_retry,
    )
    def get_intraday_bsa_ext(self, ticker: str) -> Dict[str, Any]:
        """
        Lấy dữ liệu cung/cầu intraday theo khung 15 phút.

        Args:
            ticker: Mã cổ phiếu, e.g. "TCB".

        Returns:
            Dict với BSA ext data theo granularity 15 phút.

        Raises:
            requests.HTTPError: HTTP error.
        """
        path = TCBSConstants.endpoint("intraday_bsa_ext").replace("{ticker}", ticker)
        logger.debug("get_intraday_bsa_ext — ticker=%s", ticker)

        try:
            result = self._get(path)  # type: ignore[attr-defined]
            logger.debug("get_intraday_bsa_ext — ticker=%s success", ticker)
            return result
        except Exception as exc:
            logger.error("get_intraday_bsa_ext failed — ticker=%s: %s", ticker, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        reraise=True,
        after=_log_retry,
    )
    def get_intraday_bsa_month(self, ticker: str) -> Dict[str, Any]:
        """
        Lấy dữ liệu cung/cầu theo tháng cho 1 mã cổ phiếu.

        Args:
            ticker: Mã cổ phiếu, e.g. "TCB".

        Returns:
            Dict với BSA monthly aggregated data.

        Raises:
            requests.HTTPError: HTTP error.
        """
        path = TCBSConstants.endpoint("intraday_bsa_month").replace("{ticker}", ticker)
        logger.debug("get_intraday_bsa_month — ticker=%s", ticker)

        try:
            result = self._get(path)  # type: ignore[attr-defined]
            logger.debug("get_intraday_bsa_month — ticker=%s success", ticker)
            return result
        except Exception as exc:
            logger.error("get_intraday_bsa_month failed — ticker=%s: %s", ticker, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        reraise=True,
        after=_log_retry,
    )
    def get_intraday_history(
        self,
        ticker: str,
        page: Optional[int] = None,
        size: Optional[int] = None,
        head_index: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Lấy lịch sử khớp lệnh intraday (tick-by-tick) cho 1 mã cổ phiếu.

        Pagination dùng page/size + headIndex (reverse paging).
        Defaults đọc từ tcbs_constants.yaml section intraday_history.

        Args:
            ticker:     Mã cổ phiếu, e.g. "VCB", "TCB".
            page:       Số trang, bắt đầu từ 0.
            size:       Số records mỗi trang (max 100 theo spec).
            head_index: Head index cho reverse paging (-1 = từ đầu).

        Returns:
            Dict với keys: page, size, headIndex, numberOfItems, total,
            ticker, data (list of tick records).
            Mỗi tick: p (price), v (volume), cp (change), a (action),
            ba (buy acc), sa (sell acc), hl (highlight >200M), t (time).

        Raises:
            requests.HTTPError: HTTP error.
        """
        hist_cfg = TCBSConstants.intraday_history_config()
        resolved_page       = page       if page       is not None else hist_cfg.get("default_page", 0)
        resolved_size       = size       if size       is not None else hist_cfg.get("default_size", 100)
        resolved_head_index = head_index if head_index is not None else hist_cfg.get("default_head_index", -1)

        max_size = hist_cfg.get("max_size", 100)
        if resolved_size > max_size:
            logger.warning(
                "get_intraday_history: size=%d exceeds max=%d — clamping to %d",
                resolved_size, max_size, max_size,
            )
            resolved_size = max_size

        path = TCBSConstants.endpoint("intraday_history_paging").replace("{ticker}", ticker)
        params: Dict[str, Any] = {
            "page":      resolved_page,
            "size":      resolved_size,
            "headIndex": resolved_head_index,
        }

        logger.debug(
            "get_intraday_history — ticker=%s, page=%d, size=%d, headIndex=%d",
            ticker, resolved_page, resolved_size, resolved_head_index,
        )

        try:
            result = self._get(path, params=params)  # type: ignore[attr-defined]
            logger.debug(
                "get_intraday_history — ticker=%s: %s items returned",
                ticker, result.get("numberOfItems", "N/A"),
            )
            return result
        except Exception as exc:
            logger.error(
                "get_intraday_history failed — ticker=%s page=%d: %s",
                ticker, resolved_page, exc,
            )
            raise
