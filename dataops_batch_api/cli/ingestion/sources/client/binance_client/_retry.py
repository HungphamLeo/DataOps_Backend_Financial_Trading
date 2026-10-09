"""
_retry.py — Shared Tenacity retry callback cho BinanceClient.

Phạm trù: Shared Utility
    - _log_retry:       Tenacity after= callback, log WARNING mỗi lần retry.
    - _is_retryable:    Trả về False với các HTTP error không thể retry (403, 451, …).

Dùng chung cho cả Spot và UM Futures client.
"""
from __future__ import annotations

from tenacity import RetryCallState

from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

# HTTP status codes không nên retry — hard block từ server/CDN.
_NON_RETRYABLE_STATUS = frozenset({403, 451})


def _is_retryable(exc: BaseException) -> bool:
    """
    Trả về True nếu exception đáng retry, False nếu là hard HTTP error.

    Kiểm tra binance.error.ClientError.status_code.
    403 = geo-block / WAF (CloudFront); 451 = legal block.
    Không retry các status này vì server sẽ luôn từ chối.
    """
    try:
        from binance.error import ClientError  # lazy import tránh circular
        if isinstance(exc, ClientError) and exc.status_code in _NON_RETRYABLE_STATUS:
            return False
    except ImportError:
        pass
    return True


def _log_retry(retry_state: RetryCallState) -> None:
    """
    Tenacity retry callback — log WARNING mỗi lần retry để trace được lý do.
    Truyền vào decorator qua after= parameter.
    """
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    logger.warning(
        "BinanceClient retry attempt %d — fn=%s, error=%s",
        retry_state.attempt_number,
        retry_state.fn.__name__ if retry_state.fn else "unknown",
        exc,
    )
