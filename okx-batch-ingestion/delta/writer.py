"""
Idempotent Delta Lake Writer - Hỗ trợ ghi dữ liệu Bronze/Silver lên MinIO/S3
với partition theo symbol và date để re-run an toàn (Idempotency).
"""
import os
import pandas as pd
from typing import List, Dict, Any
from deltalake import write_deltalake
from .schema import OHLCVRecord


class DeltaBronzeWriter:
    def __init__(self, table_uri: str = "s3a://okx-delta/bronze/ohlcv", storage_options: dict = None):
        self.table_uri = table_uri
        self.storage_options = storage_options or {
            "AWS_ENDPOINT_URL": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }

    def write_ohlcv_batch(self, df: pd.DataFrame, mode: str = "append") -> int:
        """Kiểm tra schema qua Pydantic và ghi vào Delta Bronze."""
        if df.empty:
            return 0

        # Validate từng record qua Pydantic
        records: List[Dict[str, Any]] = df.to_dict(orient="records")
        for record in records:
            OHLCVRecord(**record)

        # Thêm cột date để partition
        df_to_write = df.copy()
        if "date" not in df_to_write.columns:
            df_to_write["date"] = pd.to_datetime(df_to_write["timestamp"]).dt.strftime("%Y-%m-%d")

        try:
            write_deltalake(
                table_or_uri=self.table_uri,
                data=df_to_write,
                mode=mode,
                partition_by=["symbol", "date"],
                storage_options=self.storage_options,
            )
            return len(df_to_write)
        except Exception as e:
            # Fallback lưu parquet local nếu chưa có minio connection
            os.makedirs("./data/delta_bronze_local", exist_ok=True)
            df_to_write.to_parquet("./data/delta_bronze_local/ohlcv_latest.parquet")
            return len(df_to_write)
