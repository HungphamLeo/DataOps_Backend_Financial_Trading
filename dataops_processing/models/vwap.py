"""
models/vwap.py — Output data models cho VWAP aggregation.

Định nghĩa schema InfluxDB line protocol và Delta Silver output.
Khớp với backend GetVWAP Flux query:
    filter(fn: (r) => r._measurement == "vwap"
                   and r.symbol == "{symbol}"
                   and r.interval == "{interval}")
    fields: "vwap", "volume", "num_trades"
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class VWAPPoint:
    """
    Output model cho một VWAP aggregation window.

    InfluxDB schema (measurement = "vwap"):
        tags:   symbol, interval
        fields: vwap (float64), volume (float64), num_trades (int64)
        time:   window_end (UTC nanoseconds)

    Delta Silver schema:
        symbol, window_start, window_end, interval,
        vwap, total_volume, num_trades
    """
    symbol:       str
    window_end_ns: int     # nanoseconds UTC — InfluxDB timestamp
    interval:     str      # e.g. "5m"
    vwap:         float
    total_volume: float
    num_trades:   int

    def to_line_protocol(self) -> str:
        """
        Serialize sang InfluxDB line protocol format.

        Format: measurement,tag_key=tag_val field_key=field_val timestamp
        Dùng integer suffix 'i' cho num_trades (InfluxDB integer type).
        """
        return (
            f"vwap,"
            f"symbol={self.symbol},"
            f"interval={self.interval} "
            f"vwap={self.vwap},"
            f"volume={self.total_volume},"
            f"num_trades={self.num_trades}i "
            f"{self.window_end_ns}"
        )


@dataclass(frozen=True)
class VolumeBar:
    """
    Output model cho một Volume Bar (De Prado simplified).

    Delta Silver schema:
        symbol, inst_type, bar_index,
        open, high, low, close,
        total_volume, num_candles,
        bar_open_ts, bar_close_ts,
        ingested_at
    """
    bar_index:    int
    open:         float
    high:         float
    low:          float
    close:        float
    total_volume: float
    num_candles:  int
    bar_open_ts:  int      # Unix milliseconds
    bar_close_ts: int      # Unix milliseconds

    def to_dict(self) -> dict:
        """Trả về dict để tạo Spark Row / createDataFrame."""
        return {
            "bar_index":    self.bar_index,
            "open":         self.open,
            "high":         self.high,
            "low":          self.low,
            "close":        self.close,
            "total_volume": self.total_volume,
            "num_candles":  self.num_candles,
            "bar_open_ts":  self.bar_open_ts,
            "bar_close_ts": self.bar_close_ts,
        }
