"""
params.py — Cấu hình tham số độc lập cho tất cả Technical Indicators.

Tách biệt hoàn toàn Params khỏi logic tính toán để dễ dàng tối ưu và backtest.
Giá trị mặc định là chuẩn ngành (industry standard defaults).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class SMAParams:
    """Simple Moving Average parameters."""
    period: int = 20


@dataclass
class EMAParams:
    """Exponential Moving Average parameters."""
    period: int = 20


@dataclass
class RSIParams:
    """Relative Strength Index parameters. Period 14 là chuẩn Wilder."""
    period: int = 14


@dataclass
class ATRParams:
    """Average True Range parameters. Period 14 là chuẩn Wilder."""
    period: int = 14


@dataclass
class BBandsParams:
    """Bollinger Bands parameters. num_std=2.0 bao phủ ~95% biến động."""
    period: int = 20
    num_std: float = 2.0


@dataclass
class MACDParams:
    """
    MACD parameters theo chuẩn Gerald Appel (1979).
    fast=12, slow=26, signal=9 là default phổ biến nhất.
    """
    fast_period: int = 12
    slow_period: int = 26
    signal_period: int = 9


@dataclass
class IndicatorConfig:
    """
    Aggregated config cho toàn bộ indicators trong 1 pipeline run.
    Có thể khởi tạo từ dict (đọc từ YAML) bằng from_dict().
    """
    sma:    SMAParams    = field(default_factory=SMAParams)
    ema:    EMAParams    = field(default_factory=EMAParams)
    rsi:    RSIParams    = field(default_factory=RSIParams)
    atr:    ATRParams    = field(default_factory=ATRParams)
    bbands: BBandsParams = field(default_factory=BBandsParams)
    macd:   MACDParams   = field(default_factory=MACDParams)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "IndicatorConfig":
        """Tạo IndicatorConfig từ dict (e.g. đọc từ okx_config.yaml)."""
        return cls(
            sma=SMAParams(**d.get("sma", {})),
            ema=EMAParams(**d.get("ema", {})),
            rsi=RSIParams(**d.get("rsi", {})),
            atr=ATRParams(**d.get("atr", {})),
            bbands=BBandsParams(**d.get("bbands", {})),
            macd=MACDParams(**d.get("macd", {})),
        )
