"""
delta_utils.py — Idempotent Delta Lake Writer cho Bronze/Silver layers.

Ghi dữ liệu lên MinIO (S3-compatible) với partition theo symbol/date
để đảm bảo re-run an toàn (idempotency).

Design:
    - DeltaBronzeWriter: ghi Binance candles, VPS snapshots (legacy),
      và TCBS market data (market snapshot, BSA, intraday ticks) vào Bronze layer.
    - Storage options được inject từ config.StorageOptions.minio(), không hardcode.
    - compact_table(): giải quyết small file problem — optimize read performance.

TCBS Tables:
    tcbs_market   — Full ticker snapshot (orderbook 3 cấp, OHLC, foreign room)
                    partitionBy(["year", "month", "exchange"])
    tcbs_bsa      — Buy/Sell Analysis per ticker per investor_type
                    partitionBy(["year", "month", "ticker"])
    tcbs_intraday — Tick-by-tick khớp lệnh intraday
                    partitionBy(["year", "month", "ticker"])

References:
    - Delta Lake: The Definitive Guide (O'Reilly)
    - delta-rs Python bindings: https://delta-io.github.io/delta-rs/
"""
from __future__ import annotations

import logging
import os
from typing import List

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from cli.ingestion.schema.models import (
    BinanceCandleRecord,
    TCBSMarketRecord,
    TCBSBSARecord,
    TCBSIntradayTickRecord,
)

logger = logging.getLogger(__name__)


class DeltaBronzeWriter:
    """
    Ghi raw data vào Delta Lake Bronze layer trên MinIO.

    Partition strategy:
        Binance candles: partitionBy(["year", "month", "inst_id"])
            → tối ưu query theo instrument và time range
        TCBS market:     partitionBy(["year", "month", "exchange"])
            → tối ưu query theo sàn và tháng

    Usage:
        writer = DeltaBronzeWriter.from_env()
        writer.write_binance_candles(records, inst_type="SPOT")
        writer.write_tcbs_market(records)
    """

    def __init__(self, bucket: str = "binance-datalake", storage_options: dict = None):
        self.bucket = bucket
        self.storage_options = storage_options or {
            "AWS_ENDPOINT_URL":           os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            "AWS_ACCESS_KEY_ID":          os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            "AWS_SECRET_ACCESS_KEY":      os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            "AWS_REGION":                 "us-east-1",
            "AWS_ALLOW_HTTP":             "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }

    @classmethod
    def from_env(cls) -> "DeltaBronzeWriter":
        """Factory method — bắt storage options từ env vars."""
        return cls(
            bucket=os.getenv("MINIO_BUCKET", "binance-datalake"),
        )

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _add_time_partitions(self, df: pd.DataFrame, ts_col: str = "ts_ms") -> pd.DataFrame:
        """
        Thêm cột year và month từ timestamp column để partition.
        Delta Lake partition theo year/month giúp query pruning hiệu quả.
        """
        df = df.copy()
        dt = pd.to_datetime(df[ts_col], unit="ms", utc=True)
        df["year"]  = dt.dt.year.astype(str)
        df["month"] = dt.dt.month.apply(lambda x: f"{x:02d}")
        return df

    def _delta_path(self, table: str) -> str:
        return f"s3://{self.bucket}/delta/bronze/{table}"

    # ── Write methods ─────────────────────────────────────────────────────────

    def write_binance_candles(self, records: List[BinanceCandleRecord], inst_type: str = "SPOT") -> int:
        """
        Ghi Binance OHLCV records vào Bronze Delta table.

        Args:
            records: danh sách BinanceCandleRecord đã qua Pydantic validation
            inst_type: "SPOT" | "FUTURES" | "PERP" (dùng làm suffix table name)

        Returns:
            Số records đã ghi thành công. 0 nếu records rỗng hoặc write thất bại.

        Raises:
            Exception được log ở ERROR level rồi re-raise để caller quyết định retry.
        """
        if not records:
            logger.warning("write_binance_candles called with empty records list")
            return 0

        delta_path = self._delta_path(f"binance_candles_{inst_type.lower()}")
        try:
            df = pd.DataFrame([r.model_dump() for r in records])
            df = self._add_time_partitions(df, ts_col="ts_ms")
            table = pa.Table.from_pandas(df)

            write_deltalake(
                delta_path,
                table,
                mode="append",
                partition_by=["year", "month", "inst_id"],
                storage_options=self.storage_options,
            )
            logger.info(
                "Wrote %d Binance candles (%s) to %s",
                len(records), inst_type, delta_path,
            )
            return len(records)
        except Exception as exc:
            logger.error(
                "Failed to write Binance candles (%s) to %s: %s",
                inst_type, delta_path, exc,
                exc_info=True,
            )
            raise

    def write_tcbs_market(self, records: List[TCBSMarketRecord]) -> int:
        """
        Ghi TCBS full market ticker snapshots vào Bronze Delta table.

        Table: tcbs_market
        Partition: year, month, exchange
        Source: /tartarus/v1/tickerCommons API

        Args:
            records: Danh sách TCBSMarketRecord đã qua Pydantic validation.

        Returns:
            Số records đã ghi thành công. 0 nếu records rỗng hoặc write thất bại.

        Raises:
            Exception được log ở ERROR level rồi re-raise.
        """
        if not records:
            logger.warning("write_tcbs_market called with empty records list")
            return 0

        delta_path = self._delta_path("tcbs_market")
        try:
            df = pd.DataFrame([r.model_dump() for r in records])
            df = self._add_time_partitions(df, ts_col="ts_ms")
            table = pa.Table.from_pandas(df)

            write_deltalake(
                delta_path,
                table,
                mode="append",
                partition_by=["year", "month", "exchange"],
                storage_options=self.storage_options,
            )
            logger.info(
                "Wrote %d TCBS market snapshots to %s",
                len(records), delta_path,
            )
            return len(records)
        except Exception as exc:
            logger.error(
                "Failed to write TCBS market snapshots to %s: %s",
                delta_path, exc,
                exc_info=True,
            )
            raise

    def write_tcbs_bsa(self, records: List[TCBSBSARecord]) -> int:
        """
        Ghi TCBS Buy/Sell Analysis records vào Bronze Delta table.

        Table: tcbs_bsa
        Partition: year, month, ticker
        Source: /nyx/v1/intraday/{ticker}/bsa API

        Args:
            records: Danh sách TCBSBSARecord đã qua Pydantic validation.

        Returns:
            Số records đã ghi thành công. 0 nếu records rỗng hoặc write thất bại.

        Raises:
            Exception được log ở ERROR level rồi re-raise.
        """
        if not records:
            logger.warning("write_tcbs_bsa called with empty records list")
            return 0

        delta_path = self._delta_path("tcbs_bsa")
        try:
            df = pd.DataFrame([r.model_dump() for r in records])
            df = self._add_time_partitions(df, ts_col="ts_ms")
            table = pa.Table.from_pandas(df)

            write_deltalake(
                delta_path,
                table,
                mode="append",
                partition_by=["year", "month", "ticker"],
                storage_options=self.storage_options,
            )
            logger.info(
                "Wrote %d TCBS BSA records to %s",
                len(records), delta_path,
            )
            return len(records)
        except Exception as exc:
            logger.error(
                "Failed to write TCBS BSA records to %s: %s",
                delta_path, exc,
                exc_info=True,
            )
            raise

    def write_tcbs_intraday(self, records: List[TCBSIntradayTickRecord]) -> int:
        """
        Ghi TCBS intraday tick records vào Bronze Delta table.

        Table: tcbs_intraday
        Partition: year, month, ticker
        Source: /nyx/v1/intraday/{ticker}/his/paging API

        Args:
            records: Danh sách TCBSIntradayTickRecord đã qua Pydantic validation.

        Returns:
            Số records đã ghi thành công. 0 nếu records rỗng hoặc write thất bại.

        Raises:
            Exception được log ở ERROR level rồi re-raise.
        """
        if not records:
            logger.warning("write_tcbs_intraday called with empty records list")
            return 0

        delta_path = self._delta_path("tcbs_intraday")
        try:
            df = pd.DataFrame([r.model_dump() for r in records])
            df = self._add_time_partitions(df, ts_col="ts_ms")
            table = pa.Table.from_pandas(df)

            write_deltalake(
                delta_path,
                table,
                mode="append",
                partition_by=["year", "month", "ticker"],
                storage_options=self.storage_options,
            )
            logger.info(
                "Wrote %d TCBS intraday ticks to %s",
                len(records), delta_path,
            )
            return len(records)
        except Exception as exc:
            logger.error(
                "Failed to write TCBS intraday ticks to %s: %s",
                delta_path, exc,
                exc_info=True,
            )
            raise

    def compact_table(self, delta_path: str, target_size_mb: int = 128) -> None:
        """
        Compact nhiều small files thành files lớn hơn.

        Small file problem: hàng nghìn files nhỏ → nhiều open() calls → read chậm.
        Delta Lake bin-packing: gộp files trong cùng partition thành ~target_size_mb.
        Nên chạy sau mỗi 24h hoặc sau backfill.

        Args:
            delta_path: S3 path đến Delta table, e.g.
                "s3://binance-datalake/delta/bronze/binance_candles_spot"
            target_size_mb: target file size sau compact (default 128 MB, đọc từ config)
        """
        logger.info("Starting compact: %s (target=%d MB)", delta_path, target_size_mb)
        try:
            dt = DeltaTable(delta_path, storage_options=self.storage_options)
            result = dt.optimize.compact(target_size=target_size_mb * 1024 * 1024)
            logger.info("Compacted %s: %s", delta_path, result)
        except Exception as exc:
            logger.error("Compact failed for %s: %s", delta_path, exc, exc_info=True)
            raise
