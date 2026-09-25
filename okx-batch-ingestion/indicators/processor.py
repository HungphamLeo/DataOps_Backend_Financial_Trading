"""
Pipeline xử lý dữ liệu Indicators: Nhận DataFrame OHLCV và tính toán toàn bộ Indicators.
"""
import pandas as pd
from typing import Optional
from .params import IndicatorConfig
from .calculator import compute_sma, compute_ema, compute_rsi, compute_atr, compute_bbands, compute_macd


class IndicatorProcessor:
    def __init__(self, config: Optional[IndicatorConfig] = None):
        self.config = config or IndicatorConfig()

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Thực hiện tính toán và gắn kết các indicator vào DataFrame chuẩn."""
        if df.empty or len(df) < 5:
            return df

        df = df.sort_values("timestamp").copy()

        # SMA & EMA
        df["sma"] = compute_sma(df["close"], self.config.sma)
        df["ema"] = compute_ema(df["close"], self.config.ema)

        # RSI
        df["rsi"] = compute_rsi(df["close"], self.config.rsi)

        # ATR
        if "high" in df.columns and "low" in df.columns:
            df["atr"] = compute_atr(df["high"], df["low"], df["close"], self.config.atr)

        # Bollinger Bands
        bb_mid, bb_upper, bb_lower = compute_bbands(df["close"], self.config.bbands)
        df["bb_middle"] = bb_mid
        df["bb_upper"] = bb_upper
        df["bb_lower"] = bb_lower

        # MACD
        macd_line, macd_signal, macd_hist = compute_macd(df["close"], self.config.macd)
        df["macd"] = macd_line
        df["macd_signal"] = macd_signal
        df["macd_hist"] = macd_hist

        return df
