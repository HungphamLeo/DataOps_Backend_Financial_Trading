"""
margin.py — TCBS Margin & Risk mixin (Private Endpoints).

Phạm trù: Margin / Risk
    - get_account_risk()    : Rtt, dư nợ gốc, lãi, risk policy (initial/maintenance/liquidation).
    - get_pricing_policy()  : Chính sách giá (margin pricing policy) của tài khoản.
    - get_margin_info()     : Lịch sử vay margin (paginated).

Tất cả endpoint là PRIVATE — yêu cầu JWT token.

TCBS iFlash API endpoints (từ TCBSConstants):
    account_risk   : /hydros/v1/account/{accountNo}/risk
    pricing_policy : /hydros/v1/account/{accountNo}/pricing-policy
    margin_info    : /erebos/v2/digital/margin-info

Lưu ý: `/hydros/v1/margin-limits` KHÔNG tồn tại trong openapi-v1.0.0.json.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from log.logger_setup import logger_manager
from cli.ingestion.sources.tcbs_constants_loader import TCBSConstants
from cli.ingestion.sources.client.tcbs_client._retry import _is_retryable, _log_retry

logger = logger_manager.get_logger(__name__)


class TCBSMarginMixin:
    """Phạm trù Margin / Risk — private endpoints, kế thừa bởi TCBSRestClient."""

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
        after=_log_retry,
    )
    def get_account_risk(self, account_no: str) -> Any:
        """
        Lấy Rtt, dư nợ gốc/lãi và risk policy margin của tài khoản margin.

        Args:
            account_no: Số tài khoản margin.

        Returns:
            Response JSON (list hoặc dict có key "data").
        """
        path = TCBSConstants.endpoint("account_risk").format(accountNo=account_no)
        logger.debug("get_account_risk — account=%s", account_no)
        try:
            return self._get(path, require_auth=True)  # type: ignore[attr-defined]
        except Exception as exc:
            logger.error("get_account_risk failed — account=%s: %s", account_no, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
        after=_log_retry,
    )
    def get_pricing_policy(self, account_no: str) -> Any:
        """Lấy margin pricing policy của tài khoản."""
        path = TCBSConstants.endpoint("pricing_policy").format(accountNo=account_no)
        logger.debug("get_pricing_policy — account=%s", account_no)
        try:
            return self._get(path, require_auth=True)  # type: ignore[attr-defined]
        except Exception as exc:
            logger.error("get_pricing_policy failed — account=%s: %s", account_no, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
        after=_log_retry,
    )
    def get_margin_info(
        self,
        page: int = 0,
        size: int = 100,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Lấy lịch sử vay margin (paginated)."""
        query: Dict[str, Any] = {"page": page, "size": size}
        if params:
            query.update(params)
        path = TCBSConstants.endpoint("margin_info")
        logger.debug("get_margin_info — params=%s", query)
        try:
            return self._get(path, params=query, require_auth=True)  # type: ignore[attr-defined]
        except Exception as exc:
            logger.error("get_margin_info failed — params=%s: %s", query, exc)
            raise
