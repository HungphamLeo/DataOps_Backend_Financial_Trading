"""
tests/test_vwap_job.py — Unit tests cho vwap_job.py.

Test 3 chức năng core mà không cần Spark thật hay Kafka running:
  1. strip_confluent_header       — Confluent wire-format header stripping (models/trade.py)
  2. TradeEvent.from_kafka_bytes  — JSON parse + validation logic (models/trade.py)
  3. _write_to_influxdb           — InfluxDB sink (vwap_job.py, mock influxdb-client)

Pattern: import trực tiếp các hàm pure Python, không khởi động SparkSession.
"""
import json
import struct
import unittest
from unittest.mock import MagicMock, patch

from dataops_processing.models.trade import TradeEvent, strip_confluent_header
from dataops_processing.jobs.vwap_job import _write_to_influxdb

# Constants dùng trong test — lấy trực tiếp từ config yaml (không hardcode)
INFLUX_MEASUREMENT   = "vwap"
INFLUX_TAG_INTERVAL_5M = "5m"


# ---------------------------------------------------------------------------
# Test 1: Confluent Wire-Format Header Stripping
# ---------------------------------------------------------------------------

class TestStripConfluentHeader(unittest.TestCase):
    """Kiểm tra strip_confluent_header() xử lý đúng cả 2 trường hợp."""

    def test_strips_5_byte_header_when_magic_byte_present(self):
        """Payload có magic byte 0x00 + 4-byte schema_id → strip 5 bytes."""
        schema_id = 42
        actual_payload = b'{"instId":"BTCUSDT","price":65000.0}'
        header = struct.pack(">bI", 0x00, schema_id)
        raw = header + actual_payload

        result = strip_confluent_header(raw)

        self.assertEqual(result, actual_payload)

    def test_returns_raw_when_no_magic_byte(self):
        """Plain JSON (không có header) → trả nguyên vẹn."""
        plain_json = b'{"instId":"ETHUSDT","price":3500.0,"size":0.5}'

        result = strip_confluent_header(plain_json)

        self.assertEqual(result, plain_json)

    def test_returns_raw_when_too_short_for_header(self):
        """Payload < 6 bytes → không thể có header → trả nguyên."""
        short_payload = b'\x00\x01\x02'

        result = strip_confluent_header(short_payload)

        self.assertEqual(result, short_payload)


# ---------------------------------------------------------------------------
# Test 2: Trade Payload Parsing — TradeEvent.from_kafka_bytes
# ---------------------------------------------------------------------------

class TestParseTradePayload(unittest.TestCase):
    """Kiểm tra TradeEvent.from_kafka_bytes() parse đúng và reject invalid records."""

    def _make_raw(self, data: dict) -> bytes:
        return json.dumps(data).encode("utf-8")

    def test_parses_valid_trade_event(self):
        """TradeEvent đầy đủ field → object với đúng symbol, price, size, side, ts_ms."""
        raw = self._make_raw({
            "instId":      "BTCUSDT",
            "tradeId":     "123456",
            "price":       65000.5,
            "size":        0.01,
            "side":        "buy",
            "tsMs":        1700000000000,
            "producedAtMs": 1700000000100,
            "instType":    "SPOT",
        })

        result = TradeEvent.from_kafka_bytes(raw)

        self.assertIsNotNone(result)
        self.assertEqual(result.symbol, "BTCUSDT")
        self.assertAlmostEqual(result.price, 65000.5)
        self.assertAlmostEqual(result.size, 0.01)
        self.assertEqual(result.side, "buy")
        self.assertEqual(result.ts_ms, 1700000000000)

    def test_returns_none_for_missing_required_fields(self):
        """Thiếu field bắt buộc → None (record bị skip, không crash job)."""
        raw = self._make_raw({
            "instId": "ETHUSDT",
            "price":  3500.0,
            "tsMs":   1700000000000,
        })

        result = TradeEvent.from_kafka_bytes(raw)

        self.assertIsNone(result)

    def test_returns_none_for_zero_size(self):
        """size=0 → invalid trade → None. Tránh division-by-zero trong VWAP."""
        raw = self._make_raw({
            "instId": "SOLUSDT",
            "price":  150.0,
            "size":   0.0,
            "tsMs":   1700000000000,
        })

        result = TradeEvent.from_kafka_bytes(raw)

        self.assertIsNone(result)

    def test_returns_none_for_negative_price(self):
        """price<=0 là data corruption → None."""
        raw = self._make_raw({
            "instId": "BTCUSDT",
            "price":  -100.0,
            "size":   0.5,
            "tsMs":   1700000000000,
        })

        result = TradeEvent.from_kafka_bytes(raw)

        self.assertIsNone(result)

    def test_returns_none_for_malformed_bytes(self):
        """Non-JSON bytes → None (không crash)."""
        result = TradeEvent.from_kafka_bytes(b"\xff\xfe garbage not json")

        self.assertIsNone(result)

    def test_strips_confluent_header_before_parse(self):
        """Payload với Confluent header → vẫn parse được."""
        payload = json.dumps({
            "instId": "BNBUSDT",
            "price":  400.0,
            "size":   1.0,
            "tsMs":   1700000000000,
        }).encode()
        header = struct.pack(">bI", 0x00, 7)
        raw = header + payload

        result = TradeEvent.from_kafka_bytes(raw)

        self.assertIsNotNone(result)
        self.assertEqual(result.symbol, "BNBUSDT")


# ---------------------------------------------------------------------------
# Test 3: InfluxDB Sink (mock influxdb-client)
# ---------------------------------------------------------------------------

class TestWriteToInfluxDB(unittest.TestCase):
    """Kiểm tra _write_to_influxdb() gọi đúng InfluxDB write_api với đúng line protocol."""

    def _make_vwap_df(self):
        """Helper: tạo pandas DataFrame giống output của Spark VWAP aggregation."""
        import pandas as pd

        now = pd.Timestamp("2024-01-15 09:05:00", tz="UTC")
        window_start = pd.Timestamp("2024-01-15 09:00:00", tz="UTC")
        return pd.DataFrame([
            {
                "symbol":       "BTCUSDT",
                "window_start": window_start,
                "window_end":   now,
                "interval":     INFLUX_TAG_INTERVAL_5M,
                "vwap":         65123.45,
                "total_volume": 12.5,
                "num_trades":   300,
            }
        ])

    @patch.dict("os.environ", {
        "INFLUXDB_URL":    "http://localhost:8086",
        "INFLUXDB_TOKEN":  "test-token",
        "INFLUXDB_ORG":    "binance-vps-platform",
        "INFLUXDB_BUCKET": "market-data",
    })
    @patch("dataops_processing.jobs.vwap_job.InfluxDBClient", create=True)
    def test_writes_correct_line_protocol(self, MockInfluxDBClient):
        """write_api.write() được gọi với record chứa đúng measurement, tags, fields."""
        # Arrange
        mock_client_instance = MagicMock()
        mock_write_api = MagicMock()
        MockInfluxDBClient.return_value = mock_client_instance
        mock_client_instance.write_api.return_value = mock_write_api

        df = self._make_vwap_df()

        # Patch import trong module scope
        with patch.dict("sys.modules", {
            "influxdb_client": MagicMock(
                InfluxDBClient=MockInfluxDBClient,
                WriteOptions=MagicMock(),
            ),
            "influxdb_client.client.write_api": MagicMock(SYNCHRONOUS=MagicMock()),
            "influxdb_client.domain.write_precision": MagicMock(
                WritePrecision=MagicMock(NANOSECONDS="ns")
            ),
        }):
            _write_to_influxdb(df, batch_id=1)

        # Assert: write_api.write được gọi 1 lần
        mock_write_api.write.assert_called_once()
        call_kwargs = mock_write_api.write.call_args

        # Kiểm tra bucket và org đúng
        self.assertEqual(call_kwargs.kwargs.get("bucket") or call_kwargs[1].get("bucket"), "market-data")

        # Kiểm tra record chứa đúng measurement và tag symbol
        records = call_kwargs.kwargs.get("record") or call_kwargs[1].get("record")
        self.assertIsInstance(records, list)
        self.assertEqual(len(records), 1)
        line = records[0]
        self.assertIn(f"{INFLUX_MEASUREMENT},", line)
        self.assertIn("symbol=BTCUSDT", line)
        self.assertIn("interval=5m", line)
        self.assertIn("vwap=65123.45", line)
        self.assertIn("volume=12.5", line)

    @patch.dict("os.environ", {
        "INFLUXDB_URL":    "http://localhost:8086",
        "INFLUXDB_TOKEN":  "test-token",
        "INFLUXDB_ORG":    "binance-vps-platform",
        "INFLUXDB_BUCKET": "market-data",
    })
    def test_empty_dataframe_is_skipped(self):
        """DataFrame rỗng → không gọi InfluxDB (không crash, không write)."""
        import pandas as pd

        empty_df = pd.DataFrame()

        # Nếu không raise và không gọi influxdb → pass
        # influxdb-client không được import vì hàm return sớm
        with patch("dataops_processing.jobs.vwap_job.InfluxDBClient", create=True) as mock_client:
            _write_to_influxdb(empty_df, batch_id=0)
            mock_client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
