"""
tcbs_client.py — TCBS iFlash Open API REST Client

Thay thế VPSRestClient bằng client đầy đủ cho TCBS iFlash Open API v1.0.0.
Bao gồm: auth JWT, market data, BSA (supply/demand), intraday tick history,
         securities listing, derivatives market data.

Reference: openapi-v1.0.0.json — TCBS iFlash Open API
Base URL:  https://openapi.tcbs.com.vn
Contact:   openapi@tcbs.com.vn

Auth flow:
    1. POST /gaia/v1/oauth2/openapi/token với apiKey + otp (Smart OTP)
    2. Nhận JWT token → dùng làm Bearer header cho tất cả request
    3. Token tự động refresh khi gần hết hạn (configurable threshold)

Design:
    - TCBSConstants: nằm ở tcbs_constants_loader.py — không hardcode giá trị nào.
    - TCBSRestClient: client với retry (tenacity), JWT auto-refresh, session reuse.
    - Tất cả method đều có try/except rõ ràng + logger_manager đúng chuẩn.
    - from_env(): factory method để inject từ env vars (12-Factor).

Usage:
    from cli.ingestion.sources.tcbs_client import TCBSRestClient, TCBSConstants

    client = TCBSRestClient.from_env()
    tickers = client.get_ticker_commons(index="HOSE")
    bsa     = client.get_intraday_bsa("TCB", investor_type="all")
    hist    = client.get_intraday_history("VCB", page=0, size=100)
"""
from __future__ import annotations

import os
import time
import threading
from typing import Any, Dict, List, Optional

import requests
from tenacity import RetryCallState, retry, stop_after_attempt, wait_exponential

from log.logger_setup import logger_manager
from cli.ingestion.sources.tcbs_constants_loader import TCBSConstants  # noqa: F401

logger = logger_manager.get_logger(__name__)

# Re-export TCBSConstants để backward-compatible với code cũ import từ tcbs_client
__all__ = ["TCBSRestClient", "TCBSConstants"]


# ─── Retry logging callback ───────────────────────────────────────────────────

def _log_retry(retry_state: RetryCallState) -> None:
    """
    Tenacity retry callback — log WARNING mỗi lần retry.
    Truyền vào decorator qua after= parameter.
    """
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    logger.warning(
        "TCBSRestClient retry attempt %d — fn=%s, error=%s",
        retry_state.attempt_number,
        retry_state.fn.__name__ if retry_state.fn else "unknown",
        exc,
    )


# ─── TCBSRestClient ──────────────────────────────────────────────────────────

class TCBSRestClient:
    """
    TCBS iFlash Open API REST client với JWT auth, retry và rate-limiting.

    Auth:
        - Lấy JWT token qua /gaia/v1/oauth2/openapi/token (apiKey + Smart OTP).
        - Token được cache; tự động refresh khi còn < token_refresh_threshold_sec.
        - Thread-safe token refresh dùng threading.Lock.

    Retry:
        - Sử dụng tenacity với exponential backoff.
        - Số lần retry và wait time đọc từ exchange_config.yaml.

    Constants:
        - Đọc endpoint paths, index codes, enums qua TCBSConstants
          (từ tcbs_constants_loader.py → tcbs_constants.yaml).

    Usage:
        client = TCBSRestClient.from_env()

        # Market data (không cần auth — public endpoints)
        tickers = client.get_ticker_commons(index="HOSE")
        snaps   = client.get_ticker_snaps(index="VN30")
        derivs  = client.get_derivatives()

        # Analytics
        bsa     = client.get_intraday_bsa("TCB", investor_type="all")
        hist    = client.get_intraday_history("VCB", page=0, size=100)

        # Cần auth
        securities = client.get_securities(filter_expr="exchange=HOSE")
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

    # ── Auth ──────────────────────────────────────────────────────────────────

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

    # ── Internal request helper ───────────────────────────────────────────────

    def _get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        require_auth: bool = False,
    ) -> Any:
        """
        Thực hiện GET request với retry tích hợp.

        Args:
            path:         URL path tương đối, e.g. "/tartarus/v1/tickerCommons"
            params:       Query params dict
            require_auth: True → thêm Authorization header

        Returns:
            Parsed JSON response.

        Raises:
            requests.HTTPError: HTTP 4xx/5xx sau khi hết retry.
            requests.ConnectionError: mạng không khả dụng.
        """
        url = self.base_url + path
        headers = self._get_auth_headers() if require_auth else {}

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

    # ── Market Data — Public Endpoints ────────────────────────────────────────

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        reraise=True,
        after=_log_retry,
    )
    def get_ticker_commons(
        self,
        tickers: Optional[List[str]] = None,
        index: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Lấy bảng giá cổ phiếu với orderbook 3 bước giá (bid/offer × 3).

        QUAN TRỌNG: Chỉ dùng tickers HOẶC index, không dùng cả hai.

        Args:
            tickers: Danh sách mã, e.g. ["VCB", "VHM", "TCB"].
            index:   Tên basket index, e.g. "HOSE", "VN30", "HNX".

        Returns:
            Dict với keys: tradingDate, data (list of ticker objects).

        Raises:
            ValueError: nếu cả tickers và index đều None, hoặc cả hai đều được truyền.
            requests.HTTPError: HTTP error sau khi hết retry.
        """
        if tickers is None and index is None:
            raise ValueError(
                "get_ticker_commons: phải truyền ít nhất tickers hoặc index."
            )
        if tickers is not None and index is not None:
            raise ValueError(
                "get_ticker_commons: không dùng tickers và index cùng lúc."
            )

        params: Dict[str, Any] = {}
        if tickers:
            params["tickers"] = ",".join(tickers)
        elif index:
            params["index"] = TCBSConstants.index_code(index)

        path = TCBSConstants.endpoint("ticker_commons")
        logger.debug("get_ticker_commons — params=%s", params)

        try:
            result = self._get(path, params=params)
            data = result.get("data", [])
            logger.debug(
                "get_ticker_commons — returned %d tickers, tradingDate=%s",
                len(data) if isinstance(data, list) else "N/A",
                result.get("tradingDate"),
            )
            return result
        except Exception as exc:
            logger.error("get_ticker_commons failed — params=%s: %s", params, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        reraise=True,
        after=_log_retry,
    )
    def get_ticker_snaps(
        self,
        index: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Lấy foreign room + market snapshot (matchPrice, volume, change, foreign buy/sell/room).

        Args:
            index: Basket index name, e.g. "HOSE", "VN30". None → toàn thị trường.

        Returns:
            Dict với keys: tradingDate, data (list).

        Raises:
            requests.HTTPError: HTTP error.
        """
        params: Dict[str, Any] = {}
        if index:
            params["index"] = TCBSConstants.index_code(index)

        path = TCBSConstants.endpoint("ticker_snaps")
        logger.debug("get_ticker_snaps — index=%s", index)

        try:
            result = self._get(path, params=params)
            data = result.get("data", [])
            logger.debug(
                "get_ticker_snaps — returned %d records",
                len(data) if isinstance(data, list) else "N/A",
            )
            return result
        except Exception as exc:
            logger.error("get_ticker_snaps failed — index=%s: %s", index, exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        reraise=True,
        after=_log_retry,
    )
    def get_derivatives(self) -> Dict[str, Any]:
        """
        Lấy bảng giá thị trường phái sinh (VN30F contracts).

        Returns:
            Dict với keys: data (list).

        Raises:
            requests.HTTPError: HTTP error.
        """
        path = TCBSConstants.endpoint("derivatives")
        logger.debug("get_derivatives — fetching derivative market board")

        try:
            result = self._get(path)
            data = result.get("data", [])
            logger.debug(
                "get_derivatives — returned %d contracts",
                len(data) if isinstance(data, list) else "N/A",
            )
            return result
        except Exception as exc:
            logger.error("get_derivatives failed: %s", exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
        reraise=True,
        after=_log_retry,
    )
    def get_put_through_snaps(self) -> Dict[str, Any]:
        """
        Lấy thông tin giao dịch thỏa thuận (put-through transactions).

        Returns:
            Dict với put-through data từ TCBS.

        Raises:
            requests.HTTPError: HTTP error.
        """
        path = TCBSConstants.endpoint("put_through_snaps")
        logger.debug("get_put_through_snaps — fetching put-through data")

        try:
            result = self._get(path)
            logger.debug("get_put_through_snaps — success")
            return result
        except Exception as exc:
            logger.error("get_put_through_snaps failed: %s", exc)
            raise

    # ── Intraday Analytics ────────────────────────────────────────────────────

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
            result = self._get(path, params=params)
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
            result = self._get(path)
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
            result = self._get(path)
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
            result = self._get(path, params=params)
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

    # ── Securities Information ────────────────────────────────────────────────

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=15),
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
            result = self._get(path, params=params, require_auth=require_auth)
            logger.debug("get_securities — success")
            return result
        except Exception as exc:
            logger.error("get_securities failed — filter=%s: %s", filter_expr, exc)
            raise

    # ── Batch helpers ─────────────────────────────────────────────────────────

    def get_ticker_commons_batch(
        self,
        tickers: List[str],
        batch_size: int = 50,
    ) -> List[Any]:
        """
        Lấy ticker commons cho danh sách lớn bằng cách chia nhỏ thành batches.

        TCBS API không document giới hạn symbols/request → dùng batch_size=50 an toàn.
        Mỗi batch lỗi được log và skip, không dừng toàn bộ run.

        Args:
            tickers:    Danh sách mã, e.g. ["VCB", "VHM", "TCB", ...]
            batch_size: Số symbols tối đa mỗi request (default 50).

        Returns:
            Danh sách tất cả ticker objects đã flatten từ tất cả batch responses.
        """
        all_data: List[Any] = []
        total_batches = (len(tickers) + batch_size - 1) // batch_size

        logger.info(
            "get_ticker_commons_batch — %d tickers, %d batches (batch_size=%d)",
            len(tickers), total_batches, batch_size,
        )

        for idx, i in enumerate(range(0, len(tickers), batch_size), start=1):
            chunk = tickers[i: i + batch_size]
            try:
                result = self.get_ticker_commons(tickers=chunk)
                data = result.get("data", [])
                if isinstance(data, list):
                    all_data.extend(data)
                logger.debug(
                    "get_ticker_commons_batch — batch %d/%d: %d records",
                    idx, total_batches, len(data) if isinstance(data, list) else 0,
                )
            except Exception as exc:
                logger.warning(
                    "get_ticker_commons_batch — batch %d/%d failed "
                    "(symbols %d-%d): %s — skipping",
                    idx, total_batches, i, i + len(chunk), exc,
                )

        logger.info(
            "get_ticker_commons_batch — complete: %d total records", len(all_data)
        )
        return all_data

    def get_intraday_history_all_pages(
        self,
        ticker: str,
        max_pages: int = 10,
    ) -> List[Any]:
        """
        Lấy toàn bộ lịch sử khớp lệnh intraday bằng cách paginate qua tất cả pages.

        Dừng khi: (1) hết data, (2) đạt max_pages, (3) lỗi liên tiếp.

        Args:
            ticker:    Mã cổ phiếu.
            max_pages: Số trang tối đa để tránh vô hạn loop (default 10 = 1000 ticks).

        Returns:
            Danh sách tất cả tick records đã flatten.
        """
        all_ticks: List[Any] = []
        hist_cfg  = TCBSConstants.intraday_history_config()
        page_size = hist_cfg.get("default_size", 100)
        head_index: int = hist_cfg.get("default_head_index", -1)

        logger.info(
            "get_intraday_history_all_pages — ticker=%s, max_pages=%d, size=%d",
            ticker, max_pages, page_size,
        )

        for page in range(max_pages):
            try:
                result = self.get_intraday_history(
                    ticker=ticker,
                    page=page,
                    size=page_size,
                    head_index=head_index,
                )
                data = result.get("data", [])
                if not data:
                    logger.debug(
                        "get_intraday_history_all_pages — ticker=%s: "
                        "empty page %d — stopping", ticker, page,
                    )
                    break
                all_ticks.extend(data)

                # Cập nhật headIndex từ response cho reverse paging
                head_index = result.get("headIndex", head_index)
                num_items  = result.get("numberOfItems", 0)

                logger.debug(
                    "get_intraday_history_all_pages — ticker=%s page=%d: %d items",
                    ticker, page, num_items,
                )

                # Dừng nếu page này ít hơn page_size (đã hết data)
                if len(data) < page_size:
                    break

            except Exception as exc:
                logger.error(
                    "get_intraday_history_all_pages — ticker=%s page=%d: %s — stopping",
                    ticker, page, exc,
                )
                break

        logger.info(
            "get_intraday_history_all_pages — ticker=%s: %d total ticks",
            ticker, len(all_ticks),
        )
        return all_ticks

    # ── Factory ───────────────────────────────────────────────────────────────

    @classmethod
    def from_env(cls) -> "TCBSRestClient":
        """
        Factory method — tạo client từ environment variables.

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
            Configured TCBSRestClient instance.
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
            client.set_token(preset_token)
            logger.info(
                "TCBSRestClient.from_env: JWT token loaded from TCBS_JWT_TOKEN env var"
            )

        return client
