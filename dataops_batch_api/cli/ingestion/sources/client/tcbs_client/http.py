"""
http.py — TCBSRestClient base class: khởi tạo session, _get helper, from_env factory.

Phạm trù: HTTP Core
    - TCBSClientBase.__init__() : Khởi tạo session, headers, logging.
    - TCBSClientBase._get()     : Internal GET request helper với retry tích hợp.
    - TCBSClientBase.from_env() : Factory method từ environment variables (12-Factor).

Design notes:
    - Không chứa business logic — chỉ là HTTP transport layer.
    - Tất cả method domain (market_data, intraday, securities) kế thừa class này.
    - Auth header inject qua _get_auth_headers() được cung cấp bởi TCBSAuthMixin.

Env vars cho from_env():
    TCBS_BASE_URL            : Base URL (default: https://openapi.tcbs.com.vn)
    TCBS_API_KEY             : API Key từ TCInvest app
    TCBS_JWT_TOKEN           : Pre-obtained JWT token (optional, bỏ qua authenticate())
    TCBS_TIMEOUT             : Request timeout seconds (default: 15)
    TCBS_RETRY               : Retry attempts (default: 3)
    TCBS_TOKEN_THRESHOLD_SEC : Refresh threshold seconds (default: 7200 = 2h)
"""
from __future__ import annotations

import os
import threading
from typing import Any, Dict, Optional

import requests

from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)


class TCBSClientBase:
    """
    Phạm trù HTTP Core — base class quản lý session, config và request helper.

    Lớp này không chứa business logic. Các domain mixin (auth, market_data, …)
    kế thừa lớp này thông qua TCBSRestClient (multiple inheritance / mixin pattern).
    """

    def __init__(
        self,
        base_url: str = "https://openapi.tcbs.com.vn",
        api_key: str = "",
        request_timeout: int = 15,
        retry_attempts: int = 3,
        retry_max_wait: int = 15,
        token_refresh_threshold_sec: int = 300,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.request_timeout = request_timeout
        self.retry_attempts = retry_attempts
        self.retry_max_wait = retry_max_wait
        self.token_refresh_threshold_sec = token_refresh_threshold_sec

        # Auth state — dùng bởi TCBSAuthMixin
        self._jwt_token: Optional[str] = None
        self._token_obtained_at: float = 0.0
        self._token_lock = threading.Lock()

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

        logger.debug(
            "TCBSRestClient initialized — base_url=%s, timeout=%ds, retry=%d",
            self.base_url, self.request_timeout, self.retry_attempts,
        )

    # ── Internal request helper ───────────────────────────────────────────────

    def _get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        require_auth: bool = False,
    ) -> Any:
        """
        Thực hiện GET request với error handling.

        Args:
            path:         URL path tương đối, e.g. "/tartarus/v1/tickerCommons"
            params:       Query params dict
            require_auth: True → thêm Authorization header (cần _get_auth_headers() từ AuthMixin)

        Returns:
            Parsed JSON response.

        Raises:
            requests.HTTPError:      HTTP 4xx/5xx.
            requests.ConnectionError: mạng không khả dụng.
        """
        url = self.base_url + path
        headers = self._get_auth_headers() if require_auth else {}  # type: ignore[attr-defined]

        try:
            resp = self.session.get(
                url,
                params=params,
                headers=headers,
                timeout=self.request_timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as exc:
            logger.error(
                "HTTP error — GET %s params=%s status=%s: %s",
                path, params,
                exc.response.status_code if exc.response else "N/A",
                exc,
            )
            raise
        except requests.ConnectionError as exc:
            logger.error("Connection error — GET %s: %s", path, exc)
            raise
        except Exception as exc:
            logger.error("Unexpected error — GET %s: %s", path, exc)
            raise

    # ── Factory ───────────────────────────────────────────────────────────────

    @classmethod
    def from_env(cls) -> "TCBSClientBase":
        """
        Factory method — tạo client từ environment variables (12-Factor App pattern).

        Env vars:
            TCBS_BASE_URL:             Base URL (default: https://openapi.tcbs.com.vn)
            TCBS_API_KEY:              API Key từ TCInvest app (required cho auth)
            TCBS_JWT_TOKEN:            Pre-obtained JWT token (optional)
            TCBS_TIMEOUT:              Request timeout seconds (default: 15)
            TCBS_RETRY:                Retry attempts (default: 3)
            TCBS_TOKEN_THRESHOLD_SEC:  Refresh threshold seconds (default: 7200 = 2h)

        Auth note:
            TCBS Access Token có TTL = 8 giờ (28800s).
            token_refresh_threshold_sec = 7200s → cảnh báo/refresh khi còn < 2h.
            Nếu TCBS_JWT_TOKEN được set, dùng ngay (bỏ qua authenticate()).
            Nếu không, phải gọi client.authenticate(otp) với Smart OTP từ TCInvest app.

        Returns:
            Configured TCBSRestClient instance (cls có thể là subclass).
        """
        client = cls(
            base_url=os.getenv("TCBS_BASE_URL", "https://openapi.tcbs.com.vn"),
            api_key=os.getenv("TCBS_API_KEY", ""),
            request_timeout=int(os.getenv("TCBS_TIMEOUT", "15")),
            retry_attempts=int(os.getenv("TCBS_RETRY", "3")),
            token_refresh_threshold_sec=int(os.getenv("TCBS_TOKEN_THRESHOLD_SEC", "7200")),
        )

        preset_token = os.getenv("TCBS_JWT_TOKEN", "")
        if preset_token:
            client.set_token(preset_token)  # type: ignore[attr-defined]
            logger.info(
                "TCBSRestClient.from_env: JWT token loaded from TCBS_JWT_TOKEN env var"
            )

        return client
