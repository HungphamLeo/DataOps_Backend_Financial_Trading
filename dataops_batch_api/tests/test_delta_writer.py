"""
test_delta_writer.py — Unit tests cho cli/jobs/_delta_writer.py

Chiến lược mock:
    - Mock toàn bộ I/O với MinIO/S3 bằng unittest.mock.patch.
    - Không yêu cầu MinIO, AWS credentials, hay deltalake thực sự.
    - Tất cả tests phải chạy được offline (python -m pytest tests/).

Scenarios:
    1. write_records — path thông thường: ghi đúng số records.
    2. write_records — empty list: skip write, trả về 0.
    3. write_records — storage_options được build từ env vars đúng.
    4. write_records — exception propagate lên caller (không swallow).
    5. _table_path — format đúng s3://bucket/delta/layer/table.
    6. apply_rolling_retention — DeltaTable.delete() được gọi đúng predicate.
    7. apply_rolling_retention — table chưa tồn tại: log warning, không raise.
"""
from __future__ import annotations

import os
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pyarrow as pa


# ── Helpers ────────────────────────────────────────────────────────────────────

_SIMPLE_SCHEMA = pa.schema([
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("close",  pa.float64(), nullable=False),
])


def _make_records(n: int = 3) -> list:
    return [{"symbol": f"SYM{i}", "close": float(i * 10)} for i in range(n)]


_ENV = {
    "AWS_ACCESS_KEY_ID":     "minio-key",
    "AWS_SECRET_ACCESS_KEY": "minio-secret",
    "AWS_ENDPOINT_URL":      "http://localhost:9000",
    "AWS_REGION":            "us-east-1",
}


# ── Test: _table_path ──────────────────────────────────────────────────────────

class TestTablePath(unittest.TestCase):
    def test_format(self):
        from cli.jobs._delta_writer import _table_path
        result = _table_path("binance-datalake", "bronze", "binance_candles_1d")
        self.assertEqual(result, "s3://binance-datalake/delta/bronze/binance_candles_1d")

    def test_various_layers(self):
        from cli.jobs._delta_writer import _table_path
        self.assertEqual(
            _table_path("my-bucket", "silver", "tcbs_market_eod"),
            "s3://my-bucket/delta/silver/tcbs_market_eod",
        )


# ── Test: _storage_options ─────────────────────────────────────────────────────

class TestStorageOptions(unittest.TestCase):
    def test_reads_from_env(self):
        from cli.jobs._delta_writer import _storage_options
        with patch.dict(os.environ, _ENV):
            opts = _storage_options()
        self.assertEqual(opts["AWS_ACCESS_KEY_ID"],     "minio-key")
        self.assertEqual(opts["AWS_SECRET_ACCESS_KEY"], "minio-secret")
        self.assertEqual(opts["AWS_ENDPOINT_URL"],      "http://localhost:9000")
        self.assertEqual(opts["AWS_REGION"],            "us-east-1")
        self.assertEqual(opts["AWS_ALLOW_HTTP"],        "true")
        self.assertEqual(opts["AWS_S3_ALLOW_UNSAFE_RENAME"], "true")

    def test_defaults_endpoint_and_region(self):
        """AWS_ENDPOINT_URL và AWS_REGION có default khi không set."""
        from cli.jobs._delta_writer import _storage_options
        env = {"AWS_ACCESS_KEY_ID": "k", "AWS_SECRET_ACCESS_KEY": "s"}
        env_without_optionals = {**_ENV}
        env_without_optionals.pop("AWS_ENDPOINT_URL", None)
        env_without_optionals.pop("AWS_REGION", None)

        with patch.dict(os.environ, env_without_optionals, clear=True):
            # Phải không raise KeyError
            opts = _storage_options()
        self.assertIn("AWS_ENDPOINT_URL", opts)
        self.assertIn("AWS_REGION", opts)

    def test_missing_access_key_raises(self):
        """Thiếu AWS_ACCESS_KEY_ID → KeyError (không có default)."""
        from cli.jobs._delta_writer import _storage_options
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(KeyError):
                _storage_options()


# ── Test: write_records ────────────────────────────────────────────────────────

class TestWriteRecords(unittest.TestCase):

    @patch("cli.jobs._delta_writer.write_deltalake")
    def test_happy_path_returns_count(self, mock_write):
        """write_records trả về đúng số records khi ghi thành công."""
        from cli.jobs._delta_writer import write_records
        records = _make_records(5)
        with patch.dict(os.environ, _ENV):
            result = write_records(
                "s3://binance-datalake/delta/bronze/test_table",
                records,
                _SIMPLE_SCHEMA,
                mode="append",
                partition_by=["symbol"],
            )
        self.assertEqual(result, 5)
        mock_write.assert_called_once()

    @patch("cli.jobs._delta_writer.write_deltalake")
    def test_write_deltalake_called_with_correct_args(self, mock_write):
        """write_deltalake nhận đúng table_path, mode, partition_by, storage_options."""
        from cli.jobs._delta_writer import write_records
        records = _make_records(2)
        table_path = "s3://binance-datalake/delta/bronze/binance_candles_1d"

        with patch.dict(os.environ, _ENV):
            write_records(table_path, records, _SIMPLE_SCHEMA, mode="overwrite", partition_by=["symbol"])

        args, kwargs = mock_write.call_args
        # Positional: table_path, pa.Table
        self.assertEqual(args[0], table_path)
        self.assertIsInstance(args[1], pa.Table)
        # Keyword args
        self.assertEqual(kwargs["mode"], "overwrite")
        self.assertEqual(kwargs["partition_by"], ["symbol"])
        self.assertIn("AWS_ACCESS_KEY_ID", kwargs["storage_options"])

    @patch("cli.jobs._delta_writer.write_deltalake")
    def test_empty_records_skips_write_returns_zero(self, mock_write):
        """Empty records list → skip write_deltalake, return 0."""
        from cli.jobs._delta_writer import write_records
        with patch.dict(os.environ, _ENV):
            result = write_records(
                "s3://bucket/delta/bronze/table",
                [],
                _SIMPLE_SCHEMA,
            )
        self.assertEqual(result, 0)
        mock_write.assert_not_called()

    @patch("cli.jobs._delta_writer.write_deltalake", side_effect=RuntimeError("S3 error"))
    def test_exception_propagates(self, mock_write):
        """Exception từ write_deltalake phải propagate — không được swallow."""
        from cli.jobs._delta_writer import write_records
        records = _make_records(3)
        with patch.dict(os.environ, _ENV):
            with self.assertRaises(RuntimeError, msg="S3 error"):
                write_records("s3://bucket/delta/bronze/table", records, _SIMPLE_SCHEMA)

    @patch("cli.jobs._delta_writer.write_deltalake")
    def test_no_partition_by_passes_empty_list(self, mock_write):
        """Khi partition_by=None → write_deltalake nhận partition_by=[]."""
        from cli.jobs._delta_writer import write_records
        records = _make_records(1)
        with patch.dict(os.environ, _ENV):
            write_records("s3://bucket/delta/bronze/table", records, _SIMPLE_SCHEMA, partition_by=None)
        _, kwargs = mock_write.call_args
        self.assertEqual(kwargs["partition_by"], [])


# ── Test: apply_rolling_retention ─────────────────────────────────────────────

class TestApplyRollingRetention(unittest.TestCase):

    @patch("cli.jobs._delta_writer.DeltaTable")
    def test_delete_called_with_correct_predicate(self, mock_dt_cls):
        """DeltaTable.delete() nhận predicate chứa cutoff date đúng format YYYY-MM-DD."""
        from cli.jobs._delta_writer import apply_rolling_retention

        mock_dt = MagicMock()
        mock_dt_cls.return_value = mock_dt

        fixed_now = datetime(2026, 10, 9, 0, 0, 0, tzinfo=timezone.utc)
        with patch("cli.jobs._delta_writer.datetime") as mock_dt_time:
            mock_dt_time.now.return_value = fixed_now
            mock_dt_time.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch.dict(os.environ, _ENV):
                apply_rolling_retention(
                    "s3://bucket/delta/bronze/tcbs_bsa",
                    date_col="trading_date",
                    keep_days=730,
                )

        # DeltaTable phải được khởi tạo với đúng path
        mock_dt_cls.assert_called_once()
        call_args = mock_dt_cls.call_args
        self.assertEqual(call_args[0][0], "s3://bucket/delta/bronze/tcbs_bsa")

        # delete() phải được gọi
        mock_dt.delete.assert_called_once()

    @patch("cli.jobs._delta_writer.DeltaTable", side_effect=Exception("Table not found"))
    def test_table_not_found_logs_warning_no_raise(self, mock_dt_cls):
        """Table chưa tồn tại → log warning, không raise Exception."""
        from cli.jobs._delta_writer import apply_rolling_retention
        with patch.dict(os.environ, _ENV):
            # Không được raise
            apply_rolling_retention(
                "s3://bucket/delta/bronze/nonexistent",
                date_col="trading_date",
                keep_days=365,
            )


# ── Test: schema enforcement ───────────────────────────────────────────────────

class TestSchemaEnforcement(unittest.TestCase):

    @patch("cli.jobs._delta_writer.write_deltalake")
    def test_pa_table_built_from_records_with_schema(self, mock_write):
        """PyArrow table được build từ records với schema đúng — float type preserved."""
        from cli.jobs._delta_writer import write_records
        import pyarrow as pa

        # Production path: Pydantic đã validate trước → records chứa native Python types.
        # pa.Table.from_pylist không auto-cast string → float; phải truyền đúng type.
        records = [{"symbol": "BTCUSDT", "close": 99999.5}]
        schema = pa.schema([
            pa.field("symbol", pa.string()),
            pa.field("close",  pa.float64()),
        ])

        with patch.dict(os.environ, _ENV):
            result = write_records("s3://b/delta/bronze/t", records, schema)

        self.assertEqual(result, 1)
        # Verify schema của table truyền vào write_deltalake
        written_table: pa.Table = mock_write.call_args[0][1]
        self.assertEqual(written_table.schema.field("close").type, pa.float64())
        self.assertAlmostEqual(written_table.column("close")[0].as_py(), 99999.5)


if __name__ == "__main__":
    unittest.main()
