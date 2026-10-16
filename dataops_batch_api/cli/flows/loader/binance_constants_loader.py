"""
binance_constants.py — Binance API Constants Loader

Load và expose tất cả constants từ binance_constants.yaml.
Single source of truth cho endpoints, enums, field indices, rate limits của Binance.

Không hardcode bất kỳ giá trị nào trong source code — đọc qua BinanceConstants class.

Usage:
<<<<<<< HEAD
    from cli.ingestion.sources.binance_constants import BinanceConstants

    base = BinanceConstants.base_url("spot")          # "https://api.binance.com/api/v3"
    path = BinanceConstants.public_endpoint("klines") # "/klines"
    idx  = BinanceConstants.kline_field_index("close") # 4
=======
    from cli.flows.loader.binance_constants_loader import BinanceConstants

    base = BinanceConstants.base_url("spot")           # "https://api.binance.com/api/v3"
    path = BinanceConstants.public_endpoint("klines")  # "/klines"
    idx  = BinanceConstants.kline_field_index("close") # 4

Note:
    cli.ingestion.sources.binance_constants là backward-compat re-export.
    Import trực tiếp từ đây (cli.flows.loader.binance_constants_loader) khi viết code mới.
>>>>>>> 20261010_data_processing
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)


def _find_yaml_path() -> Path:
    candidates = [
        Path(__file__).parent.parent.parent / "ingestion" / "sources" / "config" / "binance_constants.yaml",
        Path(__file__).parent / "binance_constants.yaml",
        Path(__file__).parent.parent / "config" / "binance_constants.yaml",
        Path(__file__).parent.parent.parent / "config" / "binance_constants.yaml",
        Path("cli/ingestion/sources/config/binance_constants.yaml"),
    ]
    for p in candidates:
        if p.exists():
            return p
    return candidates[0]


class BinanceConstants:
    """
    Load và expose tất cả constants từ binance_constants.yaml.

    Singleton — YAML được load 1 lần duy nhất lúc import.
    Cung cấp accessor methods và validation helpers.

    Tuyệt đối không hardcode bất kỳ giá trị endpoint, enum,
    field index nào vào các file khác — luôn đọc qua class này.
    """

    _data: Dict[str, Any] = {}
    _loaded: bool = False

    @classmethod
    def _ensure_loaded(cls) -> None:
        """Load YAML lần đầu — subsequent calls là no-op."""
        if cls._loaded:
            return
        yaml_path = _find_yaml_path()
        if not yaml_path.exists():
            raise FileNotFoundError(
                f"binance_constants.yaml not found at {yaml_path}. "
                "File này là required — không thể chạy Binance pipeline."
            )
        try:
            with open(yaml_path, "r", encoding="utf-8") as fh:
                cls._data = yaml.safe_load(fh) or {}
            cls._loaded = True
            logger.debug("BinanceConstants loaded from %s", yaml_path)
        except yaml.YAMLError as exc:
            raise RuntimeError(f"Malformed binance_constants.yaml: {exc}") from exc

    # ── Base URLs ─────────────────────────────────────────────────────────────

    @classmethod
    def base_url(cls, market: str = "spot") -> str:
        """
        Trả về base URL cho market.

        Args:
            market: "spot" | "futures_usdm" | "futures_coinm" |
                    "testnet_spot" | "testnet_futures"

        Returns:
            Base URL string, e.g. "https://api.binance.com/api/v3"
        """
        cls._ensure_loaded()
        urls = cls._data.get("base_urls", {})
        if market not in urls:
            raise KeyError(f"base_url '{market}' not found in binance_constants.yaml")
        return urls[market]

    # ── Endpoints ─────────────────────────────────────────────────────────────

    @classmethod
    def public_endpoint(cls, name: str) -> str:
        """
        Trả về path cho public endpoint.

        Args:
            name: key trong section public_endpoints, e.g. "klines"

        Returns:
            Path string, e.g. "/klines"
        """
        cls._ensure_loaded()
        endpoints = cls._data.get("public_endpoints", {})
        if name not in endpoints:
            raise KeyError(f"public_endpoint '{name}' not found in binance_constants.yaml")
        return endpoints[name]

    @classmethod
    def private_endpoint(cls, name: str) -> str:
        """Trả về path cho private endpoint (cần HMAC-SHA256 auth)."""
        cls._ensure_loaded()
        endpoints = cls._data.get("private_endpoints", {})
        if name not in endpoints:
            raise KeyError(f"private_endpoint '{name}' not found in binance_constants.yaml")
        return endpoints[name]

    # ── Kline field indices ───────────────────────────────────────────────────

    @classmethod
    def kline_field_index(cls, field: str) -> int:
        """
        Trả về index trong kline response array cho tên field.

        QUAN TRỌNG: vol_ccy = kline_field_index("quote_asset_volume") = 7
                    KHÔNG phải index 6 (close_time).

        Args:
            field: key trong section kline_fields, e.g. "quote_asset_volume"

        Returns:
            int index, e.g. 7
        """
        cls._ensure_loaded()
        fields = cls._data.get("kline_fields", {})
        if field not in fields:
            raise KeyError(f"kline_field '{field}' not found in binance_constants.yaml")
        return int(fields[field])

    # ── Rate limits ───────────────────────────────────────────────────────────

    @classmethod
    def rate_limit(cls, key: str) -> Any:
        """Trả về giá trị rate limit theo key."""
        cls._ensure_loaded()
        limits = cls._data.get("rate_limits", {})
        if key not in limits:
            raise KeyError(f"rate_limit '{key}' not found in binance_constants.yaml")
        return limits[key]

    # ── Validation helpers ────────────────────────────────────────────────────

    @classmethod
    def validate_interval(cls, interval: str) -> str:
        """
        Validate kline interval theo Binance supported intervals.

        Raises:
            ValueError: nếu interval không hợp lệ.
        """
        cls._ensure_loaded()
        valid = set(cls._data.get("kline_intervals", {}).keys())
        if interval not in valid:
            raise ValueError(f"interval '{interval}' not in supported Binance intervals: {valid}")
        return interval

    @classmethod
    def validate_instrument_type(cls, inst_type: str) -> str:
        """Validate instrument type theo constants."""
        cls._ensure_loaded()
        valid = set(cls._data.get("instrument_types", {}).keys())
        if inst_type not in valid:
            raise ValueError(f"inst_type '{inst_type}' not in {valid}")
        return inst_type
