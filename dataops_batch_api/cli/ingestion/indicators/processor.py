"""
processor.py — Pipeline xử lý Indicators trên DataFrame OHLCV.

Nhận DataFrame đã chuẩn hóa → tính toán và gắn tất cả indicators vào.
Đây là layer duy nhất kết nối params + calculator + DataFrame.
"""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

from .calculator import compute_atr, compute_bbands, compute_ema, compute_macd, compute_rsi, compute_sma
from .params import IndicatorConfig

logger = logging.getLogger(__name__)


def _col(df: pd.DataFrame, name: str) -> pd.Series:
    """Extract một cột từ DataFrame thành pd.Series (giúp type checker hiểu đúng kiểu)."""
    return df[name]  # type: ignore[return-value]


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

        Raises:
            KeyError: nếu DataFrame thiếu cột 'close' hoặc 'timestamp' — log ERROR.
        """
        if df.empty or len(df) < 5:
            logger.debug(
                "IndicatorProcessor.process: skipped — df has %d rows (min 5 required)",
                len(df),
            )
            return df

        logger.debug("IndicatorProcessor.process: computing indicators on %d rows", len(df))
        try:
            df = df.sort_values("timestamp").copy()

            # SMA & EMA
            df["sma"] = compute_sma(_col(df, "close"), self.config.sma)
            df["ema"] = compute_ema(_col(df, "close"), self.config.ema)

            # RSI
            df["rsi"] = compute_rsi(_col(df, "close"), self.config.rsi)

            # ATR — cần high + low
            if "high" in df.columns and "low" in df.columns:
                df["atr"] = compute_atr(
                    _col(df, "high"), _col(df, "low"), _col(df, "close"), self.config.atr,
                )

            # Bollinger Bands
            bb_mid, bb_upper, bb_lower = compute_bbands(_col(df, "close"), self.config.bbands)
            df["bb_middle"] = bb_mid
            df["bb_upper"]  = bb_upper
            df["bb_lower"]  = bb_lower

            # MACD
            macd_line, macd_signal, macd_hist = compute_macd(_col(df, "close"), self.config.macd)
            df["macd"]        = macd_line
            df["macd_signal"] = macd_signal
            df["macd_hist"]   = macd_hist

            logger.debug("IndicatorProcessor.process: done — added 10 indicator columns")
            return df
        except Exception as exc:
            logger.error(
                "IndicatorProcessor.process failed on %d rows: %s",
                len(df), exc, exc_info=True,
            )
            raise
