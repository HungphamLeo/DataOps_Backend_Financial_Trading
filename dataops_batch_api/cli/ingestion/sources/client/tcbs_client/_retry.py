"""
_retry.py — Shared Tenacity retry callback cho TCBSRestClient.

Phạm trù: Shared Utility
"""
from __future__ import annotations

from tenacity import RetryCallState

from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)


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
