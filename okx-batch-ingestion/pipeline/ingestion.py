"""
okx-batch-ingestion: Bronze Layer Ingestion
============================================
LEARN: Data Lake Architecture — 3 layers (Medallion Architecture)

  Bronze (Raw)   → Exact copy of source data, immutable, append-only
  Silver (Clean) → Validated, deduplicated, joined (done by PySpark)
  Gold (Serving) → Aggregated, business-ready (done by PySpark)

This script handles BRONZE layer only:
  1. OKX REST API → pull historical OHLCV data
  2. VPS Securities → pull historical market snapshots
  3. Validate with Pydantic (type safety, business rules)
  4. Write to Delta Lake (MinIO S3) using delta-rs

Why Python for batch ingestion?
  - delta-rs: excellent Python bindings for Delta Lake (no Spark needed for writes)
  - pandas: mature data manipulation
  - Pydantic: strong runtime type validation
  - Rich ecosystem: requests, tenacity (retry), tqdm (progress bars)

References:
  - "Designing Data-Intensive Applications" (Kleppmann) Ch.10: Batch Processing
  - "Delta Lake: The Definitive Guide" (O'Reilly)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import pyarrow as pa
import requests
from deltalake import DeltaTable, write_deltalake
from pydantic import BaseModel, Field, validator
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

# ─── Logging Setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("okx-batch-ingestion")

# ─── Configuration ────────────────────────────────────────────────────────────
# LEARN: Đọc từ environment variables — 12-Factor App methodology
MINIO_ENDPOINT  = os.environ.get("MINIO_ENDPOINT",  "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET    = os.environ.get("MINIO_BUCKET",    "okx-datalake")

OKX_BASE_URL    = "https://www.okx.com/api/v5"
VPS_BASE_URL    = os.environ.get("VPS_BASE_URL",    "https://bgapidatafeed.vps.com.vn")

# S3 storage options cho MinIO (compatible với AWS S3 API)
# LEARN: Delta-rs cần credentials để write vào S3-compatible storage
STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL":        MINIO_ENDPOINT,
    "AWS_ACCESS_KEY_ID":       MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY":   MINIO_SECRET_KEY,
    "AWS_REGION":              "us-east-1",
    "AWS_ALLOW_HTTP":          "true",   # MinIO không có SSL trong dev
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",  # Required cho MinIO
}

# ─── Pydantic Models (Data Validation) ────────────────────────────────────────
# LEARN: Pydantic = runtime type validation cho Python
#        Khác với type hints (chỉ static analysis) — Pydantic thực sự validate lúc runtime

class OKXCandleRecord(BaseModel):
    """Validated OHLCV record từ OKX REST API."""
    inst_id:    str
    inst_type:  str                    # SPOT | FUTURES | SWAP
    ts_ms:      int                    # Unix milliseconds
    open:       float
    high:       float
    low:        float
    close:      float
    vol:        float                  # Base currency volume
    vol_ccy:    float                  # Quote currency volume (USDT)
    confirm:    int = 1                # 1 = confirmed (candle closed), 0 = in-progress

    @validator("high")
    def high_gte_low(cls, v, values):
        """Business rule: high >= low"""
        if "low" in values and v < values["low"]:
            raise ValueError(f"high {v} < low {values['low']}")
        return v

    @validator("open", "high", "low", "close", "vol")
    def positive_values(cls, v, field):
        if v < 0:
            raise ValueError(f"{field.name} must be non-negative, got {v}")
        return v


class VPSCandleRecord(BaseModel):
    """Validated market snapshot từ VPS Securities."""
    symbol:          str
    exchange:        str               # HOSE | HNX | UPCOM
    price:           float
    open:            float
    high:            float
    low:             float
    reference_price: float
    ceiling_price:   float
    floor_price:     float
    volume:          float
    total_value:     float
    change_pct:      float
    session_state:   str
    ts_ms:           int

    @validator("price")
    def price_within_bands(cls, v, values):
        """VN market rule: giá không vượt trần/sàn"""
        if "ceiling_price" in values and "floor_price" in values:
            if v > values["ceiling_price"] * 1.001:  # 0.1% tolerance
                raise ValueError(f"price {v} exceeds ceiling {values['ceiling_price']}")
            if v < values["floor_price"] * 0.999:
                raise ValueError(f"price {v} below floor {values['floor_price']}")
        return v


# ─── OKX REST Client ──────────────────────────────────────────────────────────

class OKXRestClient:
    """
    OKX REST API client với rate limiting và retry.
    
    LEARN: tenacity library — declarative retry logic:
      @retry(stop=..., wait=...) = tự động retry với exponential backoff
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "okx-vps-platform/1.0",
            "Accept": "application/json",
        })
        self._last_request_time = 0.0
        # OKX rate limit: 20 requests/2 seconds per endpoint
        self._rate_limit_interval = 0.1  # 100ms between requests

    def _rate_limit(self):
        """Đợi đủ thời gian giữa 2 requests để tránh rate limit."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._rate_limit_interval:
            time.sleep(self._rate_limit_interval - elapsed)
        self._last_request_time = time.time()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        reraise=True,
    )
    def get_candles(
        self,
        inst_id: str,
        bar: str = "1m",
        after: Optional[int] = None,
        before: Optional[int] = None,
        limit: int = 300,
    ) -> list[list]:
        """
        Lấy OHLCV candles từ OKX.
        
        Args:
            inst_id: e.g. "BTC-USDT", "ETH-USDT-SWAP"
            bar: timeframe "1m","5m","1H","1D"
            after: Unix ms — lấy candles SAU timestamp này (pagination)
            before: Unix ms — lấy candles TRƯỚC timestamp này
            limit: max 300 candles mỗi request
        
        Returns:
            list of [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
        """
        self._rate_limit()

        params = {"instId": inst_id, "bar": bar, "limit": str(limit)}
        if after:
            params["after"] = str(after)
        if before:
            params["before"] = str(before)

        resp = self.session.get(
            f"{OKX_BASE_URL}/market/history-candles",
            params=params,
            timeout=15,
        )
        resp.raise_for_status()

        data = resp.json()
        if data.get("code") != "0":
            raise ValueError(f"OKX API error: {data.get('msg')} (code={data.get('code')})")

        return data.get("data", [])

    def get_instruments(self, inst_type: str = "SPOT") -> list[dict]:
        """Lấy danh sách tất cả instruments theo loại."""
        self._rate_limit()
        resp = self.session.get(
            f"{OKX_BASE_URL}/public/instruments",
            params={"instType": inst_type},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != "0":
            raise ValueError(f"OKX instruments error: {data.get('msg')}")
        return data.get("data", [])


# ─── VPS REST Client ──────────────────────────────────────────────────────────

class VPSRestClient:
    """
    VPS Securities REST API client.
    REUSE: ~60% adapted from trading-bot-finance-service/pkg/sources/vps/vps.go
    
    LEARN: VPS API không có authentication cho market data public endpoints
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def get_stock_list(self, exchange: str = "HOSE") -> list[dict]:
        """Lấy danh sách cổ phiếu theo sàn."""
        resp = self.session.get(
            f"{VPS_BASE_URL}/v1/hq/getAllStock",
            params={"exchange": exchange},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def get_market_data(self, symbols: list[str]) -> list[dict]:
        """
        Lấy market data cho danh sách cổ phiếu.
        VPS API nhận tối đa ~200 symbols mỗi request.
        """
        # Chunk thành batches 200 symbols
        all_data = []
        for i in range(0, len(symbols), 200):
            chunk = symbols[i:i+200]
            resp = self.session.get(
                f"{VPS_BASE_URL}/v1/hq/getStockData",
                params={"listShareCode": ",".join(chunk)},
                timeout=15,
            )
            resp.raise_for_status()
            all_data.extend(resp.json())
        return all_data


# ─── Delta Lake Writer ────────────────────────────────────────────────────────

class DeltaLakeWriter:
    """
    Ghi data vào Delta Lake tables trên MinIO.
    
    LEARN: Delta Lake write modes:
      - "append": thêm rows mới (dùng cho streaming, batch incremental)
      - "overwrite": xóa hết và ghi lại (dùng khi reprocess toàn bộ)
      - "error": lỗi nếu table đã tồn tại
      - "ignore": bỏ qua nếu table đã tồn tại
    
    Partition strategy:
      OKX trades: partition by (year, month, inst_id) → tối ưu query theo instrument + time range
      VPS market: partition by (year, month, exchange) → tối ưu query theo sàn + tháng
    """

    def write_okx_candles(self, records: list[OKXCandleRecord], inst_type: str = "SPOT"):
        """Ghi OKX OHLCV data vào Bronze Delta table."""
        if not records:
            logger.warning("write_okx_candles called with empty records list")
            return

        # Chuyển Pydantic models → pandas DataFrame
        df = pd.DataFrame([r.dict() for r in records])

        # Thêm partition columns
        df["dt"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
        df["year"]  = df["dt"].dt.year.astype(str)
        df["month"] = df["dt"].dt.month.apply(lambda x: f"{x:02d}")
        df = df.drop(columns=["dt"])

        # Chuyển sang PyArrow Table (format mà delta-rs yêu cầu)
        # LEARN: PyArrow = columnar in-memory format, rất nhanh khi convert sang Parquet
        table = pa.Table.from_pandas(df)

        delta_path = f"s3://{MINIO_BUCKET}/delta/bronze/okx_candles_{inst_type.lower()}"

        write_deltalake(
            delta_path,
            table,
            mode="append",
            partition_by=["year", "month", "inst_id"],
            storage_options=STORAGE_OPTIONS,
        )
        logger.info(f"Wrote {len(records)} OKX candles to {delta_path}")

    def write_vps_snapshots(self, records: list[VPSCandleRecord]):
        """Ghi VPS market snapshots vào Bronze Delta table."""
        if not records:
            return

        df = pd.DataFrame([r.dict() for r in records])
        df["dt"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
        df["year"]  = df["dt"].dt.year.astype(str)
        df["month"] = df["dt"].dt.month.apply(lambda x: f"{x:02d}")
        df = df.drop(columns=["dt"])

        table = pa.Table.from_pandas(df)
        delta_path = f"s3://{MINIO_BUCKET}/delta/bronze/vps_market"

        write_deltalake(
            delta_path,
            table,
            mode="append",
            partition_by=["year", "month", "exchange"],
            storage_options=STORAGE_OPTIONS,
        )
        logger.info(f"Wrote {len(records)} VPS snapshots to {delta_path}")

    def compact_table(self, delta_path: str, target_size_mb: int = 128):
        """
        Compact nhiều file nhỏ thành file lớn hơn — cải thiện read performance.
        LEARN: Small file problem — nhiều file nhỏ = nhiều open() calls = chậm khi đọc
        Delta Lake bin-packing: gộp files trong cùng 1 partition thành ~target_size_mb
        """
        dt = DeltaTable(delta_path, storage_options=STORAGE_OPTIONS)
        result = dt.optimize.compact(target_size=target_size_mb * 1024 * 1024)
        logger.info(f"Compacted {delta_path}: {result}")


# ─── Backfill Pipeline ────────────────────────────────────────────────────────

class OKXBackfillPipeline:
    """
    Backfill historical OKX OHLCV data cho danh sách instruments.
    
    LEARN: Backfill = lấy dữ liệu lịch sử về để fill data lake từ quá khứ đến hiện tại
    
    Strategy:
      - Paginate OKX API bằng `after` parameter (cursor-based pagination)
      - ThreadPoolExecutor: parallel fetching cho nhiều instruments
      - tqdm: progress bar cho terminal output
    """

    def __init__(self, client: OKXRestClient, writer: DeltaLakeWriter):
        self.client = client
        self.writer = writer

    def backfill_instrument(
        self,
        inst_id: str,
        inst_type: str,
        bar: str,
        start_dt: datetime,
        end_dt: datetime,
    ) -> int:
        """Backfill 1 instrument. Trả về số records đã ghi."""
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms   = int(end_dt.timestamp() * 1000)
        cursor   = end_ms  # OKX pagination: đi ngược từ mới → cũ

        total = 0
        with tqdm(desc=f"  {inst_id}", unit="candles", leave=False) as pbar:
            while cursor > start_ms:
                raw = self.client.get_candles(
                    inst_id=inst_id,
                    bar=bar,
                    before=cursor,
                    limit=300,
                )
                if not raw:
                    break

                valid_records = []
                for row in raw:
                    # OKX format: [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
                    ts_ms = int(row[0])
                    if ts_ms < start_ms:
                        continue
                    try:
                        record = OKXCandleRecord(
                            inst_id=inst_id,
                            inst_type=inst_type,
                            ts_ms=ts_ms,
                            open=float(row[1]),
                            high=float(row[2]),
                            low=float(row[3]),
                            close=float(row[4]),
                            vol=float(row[5]),
                            vol_ccy=float(row[6]),
                            confirm=int(row[8]) if len(row) > 8 else 1,
                        )
                        valid_records.append(record)
                    except Exception as e:
                        logger.warning(f"Validation failed for {inst_id} ts={ts_ms}: {e}")

                if valid_records:
                    self.writer.write_okx_candles(valid_records, inst_type)
                    total += len(valid_records)
                    pbar.update(len(valid_records))

                # Di chuyển cursor về trước (lấy batch tiếp theo)
                oldest_ts = int(raw[-1][0])
                cursor = oldest_ts - 1

                if oldest_ts <= start_ms:
                    break

        return total

    def run(
        self,
        inst_ids: list[str],
        inst_type: str = "SPOT",
        bar: str = "1m",
        days_back: int = 30,
        max_workers: int = 4,
    ):
        """
        Chạy backfill parallel cho nhiều instruments.
        
        LEARN: ThreadPoolExecutor — I/O-bound tasks (HTTP requests) → threading hiệu quả
               ProcessPoolExecutor — CPU-bound tasks (pandas transforms) → multiprocessing
        """
        end_dt   = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days_back)

        logger.info(
            f"Starting backfill: {len(inst_ids)} instruments, "
            f"{days_back} days, bar={bar}, workers={max_workers}"
        )

        total_records = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tất cả tasks
            futures = {
                executor.submit(
                    self.backfill_instrument,
                    inst_id, inst_type, bar, start_dt, end_dt,
                ): inst_id
                for inst_id in inst_ids
            }

            # Collect results với progress bar tổng
            with tqdm(total=len(inst_ids), desc="Instruments", unit="inst") as pbar:
                for future in as_completed(futures):
                    inst_id = futures[future]
                    try:
                        count = future.result()
                        total_records += count
                        logger.info(f"✓ {inst_id}: {count} records")
                    except Exception as e:
                        logger.error(f"✗ {inst_id}: {e}")
                    finally:
                        pbar.update(1)

        logger.info(f"Backfill complete: {total_records:,} total records written")


# ─── CLI Entrypoint ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="OKX VPS Data Lake Batch Ingestion")
    subparsers = parser.add_subparsers(dest="command")

    # okx-backfill subcommand
    okx_parser = subparsers.add_parser("okx-backfill", help="Backfill OKX historical candles")
    okx_parser.add_argument("--inst-type", default="SPOT", choices=["SPOT", "SWAP", "FUTURES"])
    okx_parser.add_argument("--bar",       default="1m",   help="Candle timeframe: 1m,5m,1H,1D")
    okx_parser.add_argument("--days",      default=30,     type=int, help="Days to backfill")
    okx_parser.add_argument("--workers",   default=4,      type=int, help="Parallel workers")
    okx_parser.add_argument(
        "--instruments",
        default="BTC-USDT,ETH-USDT,SOL-USDT,BNB-USDT,XRP-USDT",
        help="Comma-separated instrument IDs",
    )

    # vps-backfill subcommand
    vps_parser = subparsers.add_parser("vps-backfill", help="Backfill VPS Vietnam market data")
    vps_parser.add_argument("--exchange", default="HOSE", choices=["HOSE", "HNX", "UPCOM"])
    vps_parser.add_argument("--days",     default=90,     type=int)

    # compact subcommand
    compact_parser = subparsers.add_parser("compact", help="Compact Delta table small files")
    compact_parser.add_argument("--table",   required=True, help="Delta table S3 path")
    compact_parser.add_argument("--size-mb", default=128,   type=int, help="Target file size MB")

    args = parser.parse_args()

    if args.command == "okx-backfill":
        client   = OKXRestClient()
        writer   = DeltaLakeWriter()
        pipeline = OKXBackfillPipeline(client, writer)

        inst_ids = [s.strip() for s in args.instruments.split(",")]
        pipeline.run(
            inst_ids=inst_ids,
            inst_type=args.inst_type,
            bar=args.bar,
            days_back=args.days,
            max_workers=args.workers,
        )

    elif args.command == "vps-backfill":
        vps_client = VPSRestClient()
        writer     = DeltaLakeWriter()

        logger.info(f"Fetching stock list for {args.exchange}...")
        stocks = vps_client.get_stock_list(args.exchange)
        symbols = [s["stockCode"] for s in stocks]
        logger.info(f"Found {len(symbols)} symbols on {args.exchange}")

        # VPS không có historical data API → chỉ snapshot hiện tại
        # Chạy script này mỗi 1 phút trong giờ giao dịch để accumulate data
        logger.info("Fetching current market snapshots...")
        raw_data = vps_client.get_market_data(symbols)

        ts_now = int(time.time() * 1000)
        records = []
        for item in raw_data:
            try:
                record = VPSCandleRecord(
                    symbol=item.get("stockCode", ""),
                    exchange=args.exchange,
                    price=float(item.get("matchPrice", 0) or 0),
                    open=float(item.get("openPrice", 0) or 0),
                    high=float(item.get("highPrice", 0) or 0),
                    low=float(item.get("lowPrice", 0) or 0),
                    reference_price=float(item.get("referencePrice", 0) or 0),
                    ceiling_price=float(item.get("ceilPrice", 0) or 0),
                    floor_price=float(item.get("floorPrice", 0) or 0),
                    volume=float(item.get("totalTradingVolume", 0) or 0),
                    total_value=float(item.get("totalTradingValue", 0) or 0),
                    change_pct=float(item.get("changePercent", 0) or 0),
                    session_state=item.get("currentSession", "UNKNOWN"),
                    ts_ms=ts_now,
                )
                records.append(record)
            except Exception as e:
                logger.warning(f"VPS validation failed for {item.get('stockCode')}: {e}")

        writer.write_vps_snapshots(records)
        logger.info(f"Wrote {len(records)} VPS snapshots")

    elif args.command == "compact":
        writer = DeltaLakeWriter()
        writer.compact_table(args.table, args.size_mb)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
