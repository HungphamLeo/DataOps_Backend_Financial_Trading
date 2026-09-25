"""
Module cấu hình và tham số độc lập cho tất cả Indicators.
Tách biệt hoàn toàn Params khỏi logic tính toán để dễ dàng tối ưu và backtest.
"""
from dataclasses import dataclass, field
from typing import Dict, Any


@dataclass
class SMAParams:
    period: int = 20


@dataclass
class EMAParams:
    period: int = 20


@dataclass
class RSIParams:
    period: int = 14


@dataclass
class ATRParams:
    period: int = 14


@dataclass
class BBandsParams:
    period: int = 20
    num_std: float = 2.0


@dataclass
class MACDParams:
    fast_period: int = 12
    slow_period: int = 26
    signal_period: int = 9


@dataclass
class IndicatorConfig:
    sma: SMAParams = field(default_factory=SMAParams)
    ema: EMAParams = field(default_factory=EMAParams)
    rsi: RSIParams = field(default_factory=RSIParams)
    atr: ATRParams = field(default_factory=ATRParams)
    bbands: BBandsParams = field(default_factory=BBandsParams)
    macd: MACDParams = field(default_factory=MACDParams)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "IndicatorConfig":
        return cls(
            sma=SMAParams(**d.get("sma", {})),
            ema=EMAParams(**d.get("ema", {})),
            rsi=RSIParams(**d.get("rsi", {})),
            atr=ATRParams(**d.get("atr", {})),
            bbands=BBandsParams(**d.get("bbands", {})),
            macd=MACDParams(**d.get("macd", {})),
        )
