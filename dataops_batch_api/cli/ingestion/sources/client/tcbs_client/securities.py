"""
securities.py — TCBS Securities Information mixin (Private Endpoint).

Phạm trù: Securities Information / Listing
    - get_securities() : Thông tin niêm yết chứng khoán (stock, bond, derivative listing).

Endpoint này là PRIVATE — yêu cầu JWT token (require_auth=True mặc định).
Retry: 3 lần, exponential backoff (1s → 15s).

TCBS iFlash API endpoint (từ TCBSConstants):
    securities : /iboard/v1/securities

Fields có thể filter và select:
    filter_expr : "field=value", e.g. "exchange=HOSE", "symbol=TCB", "type=stock"
    fields      : comma-separated field names, e.g. "symbol,exchange,name"
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from log.logger_setup import logger_manager
from cli.ingestion.sources.tcbs_constants_loader import TCBSConstants
from cli.ingestion.sources.client.tcbs_client._retry import _is_retryable, _log_retry

logger = logger_manager.get_logger(__name__)


class TCBSSecuritiesMixin:
    """
    Phạm trù Securities Information — niêm yết chứng khoán (private endpoint).

    Kế thừa bởi TCBSRestClient. Dùng self._get() từ TCBSClientBase.
    Yêu cầu JWT token đã được set qua authenticate() hoặc set_token().
    """

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
        after=_log_retry,
    )
    def get_securities(
        self,
        filter_expr: Optional[str] = None,
        fields: Optional[str] = None,
        require_auth: bool = True,
    ) -> Any:
        """
        Lấy thông tin niêm yết chứng khoán (stock, bond, derivative listing data).

        Args:
            filter_expr: Filter expression dạng "field=value", e.g. "symbol=TCB".
            fields:      Comma-separated field names cần trả về. None → tất cả.
            require_auth: True (default) — endpoint này yêu cầu JWT token.

        Returns:
            Response JSON từ TCBS.

        Raises:
            RuntimeError: nếu require_auth=True và token chưa được set.
            requests.HTTPError: HTTP error.
        """
        path = TCBSConstants.endpoint("securities")
        params: Dict[str, Any] = {}
        if filter_expr:
            params["filter"] = filter_expr
        if fields:
            params["fields"] = fields

        logger.debug(
            "get_securities — filter=%s, fields=%s, auth=%s",
            filter_expr, fields, require_auth,
        )

        try:
            result = self._get(path, params=params, require_auth=require_auth)  # type: ignore[attr-defined]
            logger.debug("get_securities — success")
            return result
        except Exception as exc:
            logger.error("get_securities failed — filter=%s: %s", filter_expr, exc)
            raise
