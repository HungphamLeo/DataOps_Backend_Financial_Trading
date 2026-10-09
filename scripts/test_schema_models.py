import unittest
from pydantic import ValidationError

from cli.ingestion.schema.models import BinanceCandleRecord


class TestBinanceCandleRecord(unittest.TestCase):
    def _candle(self, **overrides: object) -> dict[str, object]:
        candle: dict[str, object] = {
            "inst_id": "BTCUSDT",
            "inst_type": "SPOT",
            "ts_ms": 1700000000000,
            "open": 65000.0,
            "high": 65500.0,
            "low": 64800.0,
            "close": 65200.0,
            "vol": 12.5,
            "vol_ccy": 815000.0,
            "confirm": 1,
        }
        candle.update(overrides)
        return candle

    def test_accepts_high_greater_than_low(self) -> None:
        candle = BinanceCandleRecord(**self._candle())

        self.assertGreaterEqual(candle.high, candle.low)

    def test_rejects_high_less_than_low(self) -> None:
        with self.assertRaisesRegex(ValidationError, r"high .* < low"):
            BinanceCandleRecord(**self._candle(high=60000.0, low=64000.0))
