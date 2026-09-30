"""
processor.py — Pipeline xử lý Indicators trên DataFrame OHLCV.

Nhận DataFrame đã chuẩn hóa → tính toán và gắn tất cả indicators vào.
Đây là layer duy nhất kết nối params + calculator + DataFrame.
"""
from __future__ import annotations

from typing import Optional

import pandas as pd

from .calculator import compute_atr, compute_bbands, compute_ema, compute_macd, compute_rsi, compute_sma
from .params import IndicatorConfig


class IndicatorProcessor:
    """
    Tính toán và gắn toàn bộ technical indicators vào DataFrame.

    Yêu cầu DataFrame đầu vào phải có columns:
        - timestamp (datetime, dùng để sort)
        - close (float)
        - high, low (float) — cần cho ATR

    Output: DataFrame gốc + thêm các cột indicator.
    """

    def __init__(self, config: Optional[IndicatorConfig] = None):
        self.config = config or IndicatorConfig()

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Tính toán tất cả indicators và attach vào DataFrame.

        Args:
            df: OHLCV DataFrame với ít nhất columns [timestamp, close]

        Returns:
            DataFrame mới với thêm cột: sma, ema, rsi, atr, bb_middle, bb_upper,
            bb_lower, macd, macd_signal, macd_hist.
            DataFrame rỗng hoặc < 5 rows được trả về nguyên không xử lý.
        """
        if df.empty or len(df) < 5:
            return df

        df = df.sort_values("timestamp").copy()

        # SMA & EMA
        df["sma"] = compute_sma(df["close"], self.config.sma)
        df["ema"] = compute_ema(df["close"], self.config.ema)

        # RSI
        df["rsi"] = compute_rsi(df["close"], self.config.rsi)

        # ATR — cần high + low
        if "high" in df.columns and "low" in df.columns:
            df["atr"] = compute_atr(df["high"], df["low"], df["close"], self.config.atr)

        # Bollinger Bands
        bb_mid, bb_upper, bb_lower = compute_bbands(df["close"], self.config.bbands)
        df["bb_middle"] = bb_mid
        df["bb_upper"]  = bb_upper
        df["bb_lower"]  = bb_lower

        # MACD
        macd_line, macd_signal, macd_hist = compute_macd(df["close"], self.config.macd)
        df["macd"]        = macd_line
        df["macd_signal"] = macd_signal
        df["macd_hist"]   = macd_hist

        return df
