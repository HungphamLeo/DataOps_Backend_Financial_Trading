"""
_delta_writer.py — Shared Delta Lake writer helper cho tất cả ETL jobs.

Wrap việc ghi Parquet files vào MinIO (S3A) theo chuẩn Delta Lake.
Dùng deltalake (delta-rs Python binding) — không cần Spark cho batch write nhỏ.

Design:
    - Mỗi table có partition scheme riêng (xem từng hàm write_*)
    - Rolling retention: xóa partition cũ bằng DeltaTable.delete() với predicate
    - s3a:// scheme cho Spark-compat; s3:// cho delta-rs native (dùng s3:// ở đây)
    - Tất cả paths đọc từ exchange_config.yaml — không hardcode

Env vars bắt buộc:
    AWS_ACCESS_KEY_ID     — MinIO access key
    AWS_SECRET_ACCESS_KEY — MinIO secret key
    AWS_ENDPOINT_URL      — http://minio:9000
    AWS_REGION            — us-east-1 (bất kỳ, MinIO không check)
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pyarrow as pa

# Lazy import: allows tests to mock deltalake without requiring the package installed.
# Production code always has deltalake available via requirements.txt.
try:
    from deltalake import DeltaTable, write_deltalake
except ImportError:  # pragma: no cover
    DeltaTable = None      # type: ignore[assignment,misc]
    write_deltalake = None  # type: ignore[assignment]

from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)


def _storage_options() -> Dict[str, str]:
    """Build S3/MinIO storage options cho delta-rs."""
    return {
        "AWS_ACCESS_KEY_ID":     os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        "AWS_ENDPOINT_URL":      os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000"),
        "AWS_REGION":            os.environ.get("AWS_REGION", "us-east-1"),
        "AWS_ALLOW_HTTP":        "true",           # MinIO plain HTTP
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",      # required for delta-rs on MinIO
    }


def _table_path(bucket: str, layer: str, table: str) -> str:
    """s3://{bucket}/delta/{layer}/{table}"""
    return f"s3://{bucket}/delta/{layer}/{table}"


def write_records(
    table_path: str,
    records: List[Dict[str, Any]],
    schema: pa.Schema,
    mode: str = "append",
    partition_by: List[str] | None = None,
) -> int:
    """
    Ghi list of dicts vào Delta table.

    Args:
        table_path:   s3://bucket/delta/layer/table
        records:      List of validated dicts (từ Pydantic .model_dump())
        schema:       PyArrow schema — đảm bảo type consistency
        mode:         "append" (default) | "overwrite"
        partition_by: Danh sách cột partition

    Returns:
        Số records đã ghi.

    Raises:
        Exception: propagate lên caller — job quyết định retry hay alert.
    """
    if not records:
        logger.warning("write_records: empty records list — skipping write to %s", table_path)
        return 0

    table = pa.Table.from_pylist(records, schema=schema)
    write_deltalake(
        table_path,
        table,
        mode=mode,
        partition_by=partition_by or [],
        storage_options=_storage_options(),
    )
    logger.info("write_records: wrote %d records to %s (mode=%s)", len(records), table_path, mode)
    return len(records)


def apply_rolling_retention(
    table_path: str,
    date_col: str,
    keep_days: int,
) -> None:
    """
    Xóa records cũ hơn keep_days ngày theo cột date_col (string "YYYY-MM-DD").

    Dùng cho các table có rolling window (BSA 2 năm, etc.).
    KHÔNG dùng cho append-only tables (binance 1d, tcbs EOD 10 năm).

    Args:
        table_path: s3://bucket/delta/layer/table
        date_col:   Tên cột ngày, e.g. "trading_date"
        keep_days:  Số ngày giữ lại, e.g. 730 = 2 năm
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    try:
        dt = DeltaTable(table_path, storage_options=_storage_options())
        deleted = dt.delete(f"{date_col} < '{cutoff}'")
        logger.info(
            "apply_rolling_retention: deleted rows with %s < %s from %s (metrics=%s)",
            date_col, cutoff, table_path, deleted,
        )
    except Exception as exc:
        # Table chưa tồn tại → bỏ qua (lần đầu chạy)
        logger.warning("apply_rolling_retention skipped for %s: %s", table_path, exc)
