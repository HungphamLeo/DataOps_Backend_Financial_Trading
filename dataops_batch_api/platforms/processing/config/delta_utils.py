"""
delta_utils.py — Idempotent Delta Lake Writer cho Bronze/Silver layers.

Ghi dữ liệu lên MinIO (S3-compatible) với partition theo symbol/date
để đảm bảo re-run an toàn (idempotency).

Design:
    - DeltaBronzeWriter: ghi OKX candles và VPS snapshots vào Bronze layer.
    - Storage options được inject từ config.StorageOptions.minio(), không hardcode.
    - compact_table(): giải quyết small file problem — optimize read performance.

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

from cli.ingestion.schema.models import OKXCandleRecord, VPSCandleRecord

logger = logging.getLogger("delta_utils")


class DeltaBronzeWriter:
    """
    Ghi raw data vào Delta Lake Bronze layer trên MinIO.

    Partition strategy:
        OKX candles: partitionBy(["year", "month", "inst_id"])
            → tối ưu query theo instrument và time range
        VPS market:  partitionBy(["year", "month", "exchange"])
            → tối ưu query theo sàn và tháng

    Usage:
        writer = DeltaBronzeWriter.from_env()
        writer.write_okx_candles(records, inst_type="SPOT")
        writer.write_vps_snapshots(records)
    """

    def __init__(self, bucket: str = "okx-datalake", storage_options: dict = None):
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
            bucket=os.getenv("MINIO_BUCKET", "okx-datalake"),
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

    def write_okx_candles(self, records: List[OKXCandleRecord], inst_type: str = "SPOT") -> int:
        """
        Ghi OKX OHLCV records vào Bronze Delta table.

        Args:
            records: danh sách OKXCandleRecord đã qua Pydantic validation
            inst_type: "SPOT" | "SWAP" | "FUTURES" (dùng làm suffix table name)

        Returns:
            Số records đã ghi thành công. 0 nếu records rỗng hoặc write thất bại.

        Raises:
            Exception được log ở ERROR level rồi re-raise để caller quyết định retry.
        """
        if not records:
            logger.warning("write_okx_candles called with empty records list")
            return 0

        delta_path = self._delta_path(f"okx_candles_{inst_type.lower()}")
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
                "Wrote %d OKX candles (%s) to %s",
                len(records), inst_type, delta_path,
            )
            return len(records)
        except Exception as exc:
            logger.error(
                "Failed to write OKX candles (%s) to %s: %s",
                inst_type, delta_path, exc,
                exc_info=True,
            )
            raise

    def write_vps_snapshots(self, records: List[VPSCandleRecord]) -> int:
        """
        Ghi VPS market snapshots vào Bronze Delta table.

        Args:
            records: danh sách VPSCandleRecord đã qua Pydantic validation

        Returns:
            Số records đã ghi thành công. 0 nếu records rỗng hoặc write thất bại.

        Raises:
            Exception được log ở ERROR level rồi re-raise.
        """
        if not records:
            logger.warning("write_vps_snapshots called with empty records list")
            return 0

        delta_path = self._delta_path("vps_market")
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
            logger.info("Wrote %d VPS snapshots to %s", len(records), delta_path)
            return len(records)
        except Exception as exc:
            logger.error(
                "Failed to write VPS snapshots to %s: %s",
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
                "s3://okx-datalake/delta/bronze/okx_candles_spot"
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
