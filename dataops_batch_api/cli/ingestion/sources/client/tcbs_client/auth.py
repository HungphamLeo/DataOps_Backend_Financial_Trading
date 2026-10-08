"""
auth.py — TCBS JWT Authentication mixin.

Phạm trù: Auth / JWT
    - authenticate()       : Lấy JWT token qua apiKey + Smart OTP.
    - set_token()          : Set JWT token thủ công từ bên ngoài.
    - _update_auth_header(): Cập nhật Authorization header trên session.
    - _is_token_valid()    : Kiểm tra token còn hạn không.
    - _get_auth_headers()  : Trả về Authorization header dict.

Auth flow TCBS:
    1. POST /gaia/v1/oauth2/openapi/token với { apiKey, otp }
    2. Nhận JWT token → Bearer header cho mọi private request.
    3. Token TTL = 8h (28800s); threshold cảnh báo = 2h (7200s).
    4. Thread-safe dùng threading.Lock.

Note:
    Mixin này giả sử class tổng hợp (TCBSRestClient) cung cấp:
        - self.api_key          (str)
        - self.base_url         (str)
        - self.request_timeout  (int)
        - self.token_refresh_threshold_sec (int)
        - self._jwt_token       (Optional[str])
        - self._token_obtained_at (float)
        - self._token_lock      (threading.Lock)
        - self.session          (requests.Session)
"""
from __future__ import annotations

import time
from typing import Dict

import requests

from log.logger_setup import logger_manager
from cli.ingestion.sources.tcbs_constants_loader import TCBSConstants

logger = logger_manager.get_logger(__name__)


class TCBSAuthMixin:
    """
    Phạm trù Auth / JWT — mixin cung cấp toàn bộ logic xác thực TCBS.
    """

    # ── Public auth methods ───────────────────────────────────────────────────

    def authenticate(self, otp: str) -> str:
        """
        Lấy JWT token từ TCBS bằng apiKey + Smart OTP (iOTP từ TCInvest app).

        QUAN TRỌNG: OTP có TTL ngắn (~30s) và chỉ dùng được 1 lần.
        Gọi method này ngay trước khi cần dùng token.

        Args:
            otp: Smart OTP code từ TCInvest app (thường là 6 chữ số).

        Returns:
            JWT token string.

        Raises:
            ValueError: nếu api_key rỗng.
            requests.HTTPError: nếu TCBS từ chối xác thực.
            RuntimeError: nếu response không chứa token.
        """
        if not self.api_key:
            raise ValueError(
                "TCBS api_key is empty. "
                "Set TCBS_API_KEY env var hoặc truyền api_key vào constructor."
            )

        endpoint = self.base_url + TCBSConstants.endpoint("token")
        payload = {
            "apiKey": self.api_key,
            "otp":    otp,
        }

        logger.info("Authenticating with TCBS — endpoint=%s", endpoint)
        try:
            resp = self.session.post(
                endpoint,
                json=payload,
                timeout=self.request_timeout,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.HTTPError as exc:
            logger.error(
                "TCBS authentication failed — HTTP %s: %s",
                exc.response.status_code if exc.response else "N/A",
                exc,
            )
            raise
        except Exception as exc:
            logger.error("TCBS authentication error: %s", exc)
            raise

        token = data.get("token")
        if not token:
            raise RuntimeError(
                f"TCBS auth response missing 'token' field. Response: {data}"
            )

        with self._token_lock:
            self._jwt_token = token
            self._token_obtained_at = time.time()

        logger.info("TCBS authentication successful — token obtained")
        return token

    def set_token(self, token: str) -> None:
        """
        Set JWT token thủ công (dùng khi token được quản lý bên ngoài).

        Args:
            token: JWT token string đã lấy từ TCBS.
        """
        with self._token_lock:
            self._jwt_token = token
            self._token_obtained_at = time.time()
        self._update_auth_header()
        logger.debug("TCBSRestClient: JWT token set manually")

    # ── Internal auth helpers ─────────────────────────────────────────────────

    def _update_auth_header(self) -> None:
        """Cập nhật Authorization header trên session."""
        if self._jwt_token:
            bearer_prefix = TCBSConstants.raw().get("headers", {}).get(
                "bearer_prefix", "Bearer "
            )
            self.session.headers["Authorization"] = f"{bearer_prefix}{self._jwt_token}"

    def _is_token_valid(self) -> bool:
        """Kiểm tra token còn hạn dùng không (dựa trên threshold, không decode JWT)."""
        if not self._jwt_token:
            return False
        elapsed = time.time() - self._token_obtained_at
        # Giả sử token TCBS có TTL dài (8h); cảnh báo khi gần hết threshold
        return elapsed < self.token_refresh_threshold_sec

    def _get_auth_headers(self) -> Dict[str, str]:
        """
        Trả về Authorization header dict.

        Raises:
            RuntimeError: nếu token chưa được set (cần gọi authenticate() trước).
        """
        with self._token_lock:
            if not self._jwt_token:
                raise RuntimeError(
                    "JWT token chưa được set. "
                    "Gọi client.authenticate(otp) hoặc client.set_token(token) trước."
                )
            bearer_prefix = TCBSConstants.raw().get("headers", {}).get(
                "bearer_prefix", "Bearer "
            )
            return {"Authorization": f"{bearer_prefix}{self._jwt_token}"}
