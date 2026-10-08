"""
load_config.py — Loader cho exchange_config.yaml.

Single function load_exchange_config() — đọc file YAML 1 lần và cache.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

import yaml

_CONFIG_PATH = Path(__file__).parent.parent / "config" / "exchange_config.yaml"


@lru_cache(maxsize=1)
def load_exchange_config() -> Dict[str, Any]:
    """Load exchange_config.yaml. Cached sau lần đầu."""
    if not _CONFIG_PATH.exists():
        raise FileNotFoundError(f"exchange_config.yaml not found at {_CONFIG_PATH}")
    with open(_CONFIG_PATH, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}
