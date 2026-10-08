"""
tcbs_constants_loader.py — TCBS API Constants Loader

Tách TCBSConstants ra file riêng để phân tách trách nhiệm:
    - tcbs_constants_loader.py : load/expose YAML constants (không có HTTP logic)
    - tcbs_client.py           : HTTP client, JWT auth, retry logic

Single source of truth cho tất cả TCBS endpoint paths, enums, codes,
field mappings và rate limits.

Usage:
    from cli.ingestion.sources.tcbs_constants_loader import TCBSConstants

    path = TCBSConstants.endpoint("ticker_commons")  # "/tartarus/v1/tickerCommons"
    code = TCBSConstants.index_code("HOSE")           # 1
    TCBSConstants.validate_price_type("LO")           # raises ValueError nếu invalid
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import yaml

from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

_YAML_PATH = Path(__file__).parent / "tcbs_constants.yaml"


class TCBSConstants:
    """
    Load và expose tất cả constants từ tcbs_constants.yaml.

    Singleton — YAML được load 1 lần duy nhất lúc import.
    Cung cấp accessor methods và validation helpers.

    Tuyệt đối không hardcode bất kỳ giá trị enum, code, endpoint nào
    vào các file khác — luôn đọc qua class này.
    """

    _data: Dict[str, Any] = {}
    _loaded: bool = False

    @classmethod
    def _ensure_loaded(cls) -> None:
        """Load YAML lần đầu — subsequent calls là no-op."""
        if cls._loaded:
            return
        if not _YAML_PATH.exists():
            raise FileNotFoundError(
                f"tcbs_constants.yaml not found at {_YAML_PATH}. "
                "File này là required — không thể chạy TCBS pipeline."
            )
        try:
            with open(_YAML_PATH, "r", encoding="utf-8") as fh:
                cls._data = yaml.safe_load(fh) or {}
            cls._loaded = True
            logger.debug("TCBSConstants loaded from %s", _YAML_PATH)
        except yaml.YAMLError as exc:
            raise RuntimeError(f"Malformed tcbs_constants.yaml: {exc}") from exc

    # ── Endpoints ─────────────────────────────────────────────────────────────

    @classmethod
    def endpoint(cls, name: str) -> str:
        """
        Trả về API path cho endpoint name.

        Args:
            name: key trong section endpoints, e.g. "ticker_commons"

        Returns:
            Path string, e.g. "/tartarus/v1/tickerCommons"

        Raises:
            KeyError: nếu endpoint name không tồn tại trong YAML.
        """
        cls._ensure_loaded()
        endpoints = cls._data.get("endpoints", {})
        if name not in endpoints:
            raise KeyError(
                f"Unknown TCBS endpoint '{name}'. "
                f"Available: {list(endpoints.keys())}"
            )
        return endpoints[name]

    # ── Index Codes ───────────────────────────────────────────────────────────

    @classmethod
    def index_code(cls, index_name: str) -> int:
        """
        Trả về numeric code cho stock index basket.

        Args:
            index_name: e.g. "HOSE", "VN30", "HNX"

        Returns:
            int code để truyền vào ?index= param

        Raises:
            KeyError: index_name không hợp lệ.
        """
        cls._ensure_loaded()
        codes = cls._data.get("index_codes", {})
        upper = index_name.upper()
        if upper not in codes:
            raise KeyError(
                f"Unknown index '{index_name}'. "
                f"Available: {list(codes.keys())}"
            )
        return codes[upper]

    # ── Validation helpers ────────────────────────────────────────────────────

    @classmethod
    def validate_price_type(cls, price_type: str) -> str:
        """Validate và trả về price_type cho lệnh cổ phiếu. Raises ValueError nếu invalid."""
        cls._ensure_loaded()
        valid = cls._data.get("price_types", {})
        if price_type not in valid:
            raise ValueError(
                f"Invalid price_type '{price_type}'. "
                f"Valid values: {list(valid.keys())}"
            )
        return price_type

    @classmethod
    def validate_exec_type(cls, exec_type: str) -> str:
        """Validate execType (NB/NS) cho lệnh cổ phiếu."""
        cls._ensure_loaded()
        valid = cls._data.get("exec_types", {})
        if exec_type not in valid:
            raise ValueError(
                f"Invalid exec_type '{exec_type}'. Valid: {list(valid.keys())}"
            )
        return exec_type

    @classmethod
    def validate_bsa_investor_type(cls, investor_type: str) -> str:
        """Validate BSA investor type (sheep/wolf/shark/all)."""
        cls._ensure_loaded()
        valid = cls._data.get("bsa_investor_types", {})
        if investor_type not in valid:
            raise ValueError(
                f"Invalid BSA investor_type '{investor_type}'. "
                f"Valid: {list(valid.keys())}"
            )
        return investor_type

    @classmethod
    def valid_session_states(cls) -> List[str]:
        """Trả về danh sách session states hợp lệ."""
        cls._ensure_loaded()
        return list(cls._data.get("session_states", {}).keys())

    @classmethod
    def intraday_history_config(cls) -> Dict[str, Any]:
        """Trả về config pagination cho intraday history."""
        cls._ensure_loaded()
        return cls._data.get("intraday_history", {})

    @classmethod
    def raw(cls) -> Dict[str, Any]:
        """Trả về toàn bộ raw data từ YAML — dùng khi cần truy cập section tuỳ ý."""
        cls._ensure_loaded()
        return cls._data
