"""
calculator.py — Pure Calculation Logic cho Technical Indicators.

Không phụ thuộc vào Database, Data Source, hay I/O.
Chỉ nhận Pandas Series / Numpy Arrays → trả về kết quả.
Dễ test, dễ backtest, dễ tái dùng.
"""
from __future__ import annotations

from typing import Tuple

import numpy as np
import pandas as pd

from .params import ATRParams, BBandsParams, EMAParams, MACDParams, RSIParams, SMAParams


def compute_sma(close: pd.Series, params: SMAParams = SMAParams()) -> pd.Series:
    """
    Simple Moving Average.
    min_periods=period để tránh partial windows tạo ra giá trị misleading.
    """
    return close.rolling(window=params.period, min_periods=params.period).mean()


def compute_ema(close: pd.Series, params: EMAParams = EMAParams()) -> pd.Series:
    """
    Exponential Moving Average.
    adjust=False → dùng recursive formula (standard EMA, không phải expanding-window variant).
    """
    return close.ewm(span=params.period, min_periods=params.period, adjust=False).mean()


def compute_rsi(close: pd.Series, params: RSIParams = RSIParams()) -> pd.Series:
    """
    Relative Strength Index (Wilder's method).
    RSI = 100 - 100 / (1 + RS), RS = AvgGain / AvgLoss
    """
    delta    = close.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=params.period, min_periods=params.period).mean()
    avg_loss = loss.rolling(window=params.period, min_periods=params.period).mean()
    rs       = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def compute_atr(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    params: ATRParams = ATRParams(),
) -> pd.Series:
    """
    Average True Range (Wilder's method).
    True Range = max(High-Low, |High-PrevClose|, |Low-PrevClose|)
    """
    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.rolling(window=params.period, min_periods=params.period).mean()


def compute_bbands(
    close: pd.Series,
    params: BBandsParams = BBandsParams(),
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    Bollinger Bands.

    Returns:
        (middle, upper, lower) — tất cả là pd.Series với index giữ nguyên
    """
    middle = compute_sma(close, SMAParams(period=params.period))
    std    = close.rolling(window=params.period, min_periods=params.period).std()
    upper  = middle + params.num_std * std
    lower  = middle - params.num_std * std
    return middle, upper, lower


def compute_macd(
    close: pd.Series,
    params: MACDParams = MACDParams(),
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    MACD (Moving Average Convergence/Divergence).

    Returns:
        (macd_line, signal_line, histogram)
        - macd_line  = fast_ema - slow_ema
        - signal     = EMA(macd_line, signal_period)
        - histogram  = macd_line - signal
    """
    fast_ema   = compute_ema(close, EMAParams(period=params.fast_period))
    slow_ema   = compute_ema(close, EMAParams(period=params.slow_period))
    macd_line  = fast_ema - slow_ema
    signal     = compute_ema(macd_line, EMAParams(period=params.signal_period))
    histogram  = macd_line - signal
    return macd_line, signal, histogram
