"""
_retry.py — Shared Tenacity retry callback cho TCBSRestClient.

Phạm trù: Shared Utility
    - _log_retry:    Tenacity after= callback, log WARNING mỗi lần retry.
    - _is_retryable: Trả về False với HTTP 401/403 — hard auth/block errors, không retry.
"""
from __future__ import annotations

import requests
from tenacity import RetryCallState

from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

# HTTP status codes không nên retry — lỗi cố định từ client hoặc server block.
_NON_RETRYABLE_STATUS = frozenset({400, 401, 403, 404, 451})


def _is_retryable(exc: BaseException) -> bool:
    """
    Trả về True nếu exception đáng retry, False nếu là hard HTTP error.

    Kiểm tra requests.HTTPError.response.status_code.
    400 = Bad Request (endpoint sai / params thiếu — không thể tự sửa khi retry).
    401 = Unauthorized (chưa có JWT token / token hết hạn — cần re-auth, không retry).
    403 = Forbidden / geo-block.
    404 = Not Found (endpoint không tồn tại — không retry).
    451 = Legal block.
    Retry những lỗi này là vô nghĩa và chỉ làm chậm test suite.
    """
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        if exc.response.status_code in _NON_RETRYABLE_STATUS:
            return False
    return True


def _log_retry(retry_state: RetryCallState) -> None:
    """
    Tenacity retry callback — log WARNING mỗi lần retry để trace được lý do.
    Truyền vào decorator qua after= parameter.
    """
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    logger.warning(
        "TCBSRestClient retry attempt %d — fn=%s, error=%s",
        retry_state.attempt_number,
        retry_state.fn.__name__ if retry_state.fn else "unknown",
        exc,
    )
