"""
grpc_server.py — Python gRPC Server cho dataops_batch_api.

Expose HistoricalDataService (batch_data.proto) tại port 50051.
Bao gồm grpc.health.v1 healthcheck endpoint tiêu chuẩn.

Khởi động:
    python grpc_server.py            # production (port từ env GRPC_PORT, default 50051)
    GRPC_PORT=50052 python grpc_server.py

Env vars:
    GRPC_PORT          default: 50051
    GRPC_MAX_WORKERS   default: 10
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL — cho _delta_writer
    LOG_LEVEL          default: INFO

Architecture:
    backend_api (Go gRPC client)
        └─ GetBinanceOHLCV   → query Delta Lake s3://binance-datalake/delta/bronze/binance_candles_*
        └─ GetTCBSMarketEOD  → query Delta Lake s3://binance-datalake/delta/bronze/tcbs_market_eod
        └─ GetTCBSBSAHistory → query Delta Lake s3://binance-datalake/delta/bronze/tcbs_bsa_daily
        └─ GetDataRange      → metadata query (count, oldest, newest date)

Note: Generated protobuf files được import bằng tên có tiền tố _pb2:
    batch_data_pb2         — messages
    batch_data_pb2_grpc    — service stubs
Xem codegen command trong batch_data.proto.
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import time
from concurrent import futures
from datetime import datetime, timezone
from typing import Any

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

# ── Protobuf generated stubs ───────────────────────────────────────────────────
# Import có thể fail nếu chưa chạy codegen.
# Hướng dẫn tạo: cd dataops_protobuf && python -m grpc_tools.protoc \
#     -I. --python_out=../dataops_batch_api --grpc_python_out=../dataops_batch_api \
#     batch-data/batch_data.proto
try:
    import batch_data_pb2
    import batch_data_pb2_grpc
    _PROTO_AVAILABLE = True
except ImportError:
    logger.warning(
        "batch_data_pb2 not found — run codegen first: "
        "python -m grpc_tools.protoc -I dataops_protobuf --python_out=dataops_batch_api "
        "--grpc_python_out=dataops_batch_api dataops_protobuf/batch-data/batch_data.proto"
    )
    _PROTO_AVAILABLE = False

# ── Delta Lake query helpers ───────────────────────────────────────────────────
try:
    from deltalake import DeltaTable
    from cli.jobs._delta_writer import _storage_options, _table_path
    from cli.flows.loader.load_config import load_exchange_config
    _DELTA_AVAILABLE = True
except ImportError:
    logger.warning("deltalake or cli modules not available — Delta queries disabled.")
    _DELTA_AVAILABLE = False

_MAX_LIMIT = 10_000  # hard cap cho tất cả queries


# ── Servicer Implementation ────────────────────────────────────────────────────

class HistoricalDataServicer:
    """
    Python gRPC Servicer cho HistoricalDataService (batch_data.proto).

    Tất cả methods query DeltaTable trên MinIO S3 bằng deltalake (delta-rs).
    Không dùng Spark — delta-rs đủ nhanh cho batch queries < 10k records.
    """

    def __init__(self) -> None:
        if _DELTA_AVAILABLE:
            cfg = load_exchange_config()
            self._bucket: str = cfg["delta"]["bucket"]
        else:
            self._bucket = os.environ.get("DELTA_BUCKET", "binance-datalake")
        self._storage_opts = _storage_options() if _DELTA_AVAILABLE else {}

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _query_delta(self, table: str, layer: str, columns: list[str] | None = None) -> list[dict[str, Any]]:
        """Load toàn bộ DeltaTable thành list of dicts. Dùng cho metadata queries."""
        path = _table_path(self._bucket, layer, table)
        dt = DeltaTable(path, storage_options=self._storage_opts)
        pa_table = dt.to_pyarrow_table(columns=columns)
        return pa_table.to_pylist()

    def _query_delta_filtered(
        self,
        table: str,
        layer: str = "bronze",
        filters: list | None = None,
        columns: list[str] | None = None,
        limit: int = 0,
    ) -> list[dict[str, Any]]:
        """
        Query DeltaTable với optional partition filters và column projection.

        filters: PyArrow DNF filter format, e.g.
            [("symbol", "=", "BTCUSDT"), ("inst_type", "=", "SPOT")]
        """
        path = _table_path(self._bucket, layer, table)
        dt = DeltaTable(path, storage_options=self._storage_opts)
        pa_table = dt.to_pyarrow_table(filters=filters, columns=columns)
        rows = pa_table.to_pylist()
        if limit and limit > 0:
            rows = rows[:min(limit, _MAX_LIMIT)]
        return rows

    # ── RPC Methods ───────────────────────────────────────────────────────────

    def GetBinanceOHLCV(self, request, context):
        """
        Trả về Binance OHLCV candles từ Delta Lake.
        interval: "4h" → binance_candles_4h | "1d" → binance_candles_1d
        """
        if not _DELTA_AVAILABLE or not _PROTO_AVAILABLE:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("Delta Lake or proto stubs not available")
            return batch_data_pb2.BinanceOHLCVResponse()

        table = "binance_candles_4h" if request.interval == "4h" else "binance_candles_1d"
        filters = [
            ("symbol",    "=", request.symbol),
            ("inst_type", "=", request.inst_type),
        ]
        # Thêm time filters nếu có
        if request.HasField("from"):
            from_date = datetime.fromtimestamp(request.from_.seconds, tz=timezone.utc).strftime("%Y-%m-%d")
            filters.append(("year", ">=", from_date[:4]))
        if request.HasField("to"):
            to_date = datetime.fromtimestamp(request.to.seconds, tz=timezone.utc).strftime("%Y-%m-%d")
            filters.append(("year", "<=", to_date[:4]))

        limit = request.limit if request.limit > 0 else _MAX_LIMIT

        try:
            rows = self._query_delta_filtered(table, filters=filters, limit=limit)
        except Exception as exc:
            logger.error("GetBinanceOHLCV failed symbol=%s: %s", request.symbol, exc)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return batch_data_pb2.BinanceOHLCVResponse()

        bars = [
            batch_data_pb2.BinanceOHLCVBar(
                symbol=r["symbol"], inst_type=r["inst_type"], interval=r["interval"],
                ts_open_ms=r["ts_open_ms"], ts_close_ms=r["ts_close_ms"],
                open=r["open"], high=r["high"], low=r["low"], close=r["close"],
                volume=r["volume"], quote_volume=r["quote_volume"],
                num_trades=int(r["num_trades"]),
            )
            for r in rows
        ]
        return batch_data_pb2.BinanceOHLCVResponse(
            symbol=request.symbol, inst_type=request.inst_type,
            interval=request.interval, bars=bars, count=len(bars),
        )

    def GetTCBSMarketEOD(self, request, context):
        """Trả về TCBS daily EOD snapshot từ Delta Lake."""
        if not _DELTA_AVAILABLE or not _PROTO_AVAILABLE:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("Delta Lake or proto stubs not available")
            return batch_data_pb2.TCBSMarketEODResponse()

        filters = [("symbol", "=", request.symbol)]
        if request.exchange:
            filters.append(("exchange", "=", request.exchange))
        if request.from_date:
            filters.append(("trading_date", ">=", request.from_date))
        if request.to_date:
            filters.append(("trading_date", "<=", request.to_date))

        limit = request.limit if request.limit > 0 else _MAX_LIMIT

        try:
            rows = self._query_delta_filtered("tcbs_market_eod", filters=filters, limit=limit)
        except Exception as exc:
            logger.error("GetTCBSMarketEOD failed symbol=%s: %s", request.symbol, exc)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return batch_data_pb2.TCBSMarketEODResponse()

        bars = [
            batch_data_pb2.TCBSMarketEODBar(
                trading_date=r["trading_date"], symbol=r["symbol"], exchange=r["exchange"],
                open=r.get("open", 0.0), high=r.get("high", 0.0),
                low=r.get("low", 0.0), close=r.get("close", 0.0),
                total_vol=r.get("total_vol", 0.0), total_val=r.get("total_val", 0.0),
                buy_foreign_qty=r.get("buy_foreign_qty", 0.0),
                sell_foreign_qty=r.get("sell_foreign_qty", 0.0),
                net_foreign_qty=r.get("net_foreign_qty", 0.0),
                change_pct=r.get("change_pct", 0.0), ref_price=r.get("ref_price", 0.0),
                ceil_price=r.get("ceil_price", 0.0), floor_price=r.get("floor_price", 0.0),
            )
            for r in rows
        ]
        return batch_data_pb2.TCBSMarketEODResponse(
            symbol=request.symbol,
            exchange=request.exchange or "HOSE",
            bars=bars, count=len(bars),
        )

    def GetTCBSBSAHistory(self, request, context):
        """Trả về TCBS BSA daily summary từ Delta Lake."""
        if not _DELTA_AVAILABLE or not _PROTO_AVAILABLE:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("Delta Lake or proto stubs not available")
            return batch_data_pb2.TCBSBSAResponse()

        filters = [("ticker", "=", request.ticker)]
        if request.investor_type:
            filters.append(("investor_type", "=", request.investor_type))
        if request.from_date:
            filters.append(("trading_date", ">=", request.from_date))
        if request.to_date:
            filters.append(("trading_date", "<=", request.to_date))

        limit = request.limit if request.limit > 0 else 730  # 2 năm retention

        try:
            rows = self._query_delta_filtered("tcbs_bsa_daily", filters=filters, limit=limit)
        except Exception as exc:
            logger.error("GetTCBSBSAHistory failed ticker=%s: %s", request.ticker, exc)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return batch_data_pb2.TCBSBSAResponse()

        points = [
            batch_data_pb2.TCBSBSAPoint(
                trading_date=r["trading_date"], ticker=r["ticker"],
                investor_type=r["investor_type"],
                bup=r.get("bup", 0.0), sdp=r.get("sdp", 0.0), bsr=r.get("bsr", 0.0),
            )
            for r in rows
        ]
        return batch_data_pb2.TCBSBSAResponse(
            ticker=request.ticker, investor_type=request.investor_type,
            points=points, count=len(points),
        )

    def GetDataRange(self, request, context):
        """Metadata query: oldest/newest date và total count cho 1 data source."""
        if not _DELTA_AVAILABLE or not _PROTO_AVAILABLE:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("Delta Lake or proto stubs not available")
            return batch_data_pb2.DataRangeResponse()

        table_map = {
            "binance_4h":      ("binance_candles_4h", "ts_open_ms"),
            "binance_1d":      ("binance_candles_1d", "ts_open_ms"),
            "tcbs_market_eod": ("tcbs_market_eod",    "trading_date"),
            "tcbs_bsa_daily":  ("tcbs_bsa_daily",     "trading_date"),
        }
        if request.source not in table_map:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"Unknown source '{request.source}'. Valid: {list(table_map)}")
            return batch_data_pb2.DataRangeResponse()

        table, date_col = table_map[request.source]
        filters = []
        if request.symbol:
            col = "ticker" if "bsa" in table else "symbol"
            filters.append((col, "=", request.symbol))
        if request.inst_type:
            filters.append(("inst_type", "=", request.inst_type))
        if request.investor_type:
            filters.append(("investor_type", "=", request.investor_type))

        try:
            rows = self._query_delta_filtered(table, filters=filters or None, columns=[date_col])
        except Exception as exc:
            logger.error("GetDataRange failed source=%s: %s", request.source, exc)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return batch_data_pb2.DataRangeResponse()

        if not rows:
            return batch_data_pb2.DataRangeResponse(
                source=request.source, symbol=request.symbol, total_count=0,
            )

        dates = [str(r[date_col]) for r in rows if r.get(date_col) is not None]
        return batch_data_pb2.DataRangeResponse(
            source=request.source,
            symbol=request.symbol,
            oldest_date=min(dates),
            newest_date=max(dates),
            total_count=len(rows),
            last_updated_at=datetime.now(timezone.utc).isoformat(),
        )


# ── Server bootstrap ───────────────────────────────────────────────────────────

def serve() -> None:
    """Start gRPC server với HistoricalDataService + grpc.health.v1 healthcheck."""
    port         = int(os.environ.get("GRPC_PORT", "50051"))
    max_workers  = int(os.environ.get("GRPC_MAX_WORKERS", "10"))

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))

    # ── Register HistoricalDataService ────────────────────────────────────────
    if _PROTO_AVAILABLE:
        batch_data_pb2_grpc.add_HistoricalDataServiceServicer_to_server(
            HistoricalDataServicer(), server
        )
        logger.info("Registered: HistoricalDataService")
    else:
        logger.warning("HistoricalDataService NOT registered — proto stubs missing.")

    # ── Register grpc.health.v1 (tiêu chuẩn — dùng cho k8s readiness/liveness) ─
    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    # Set service status: SERVING khi sẵn sàng
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    if _PROTO_AVAILABLE:
        health_servicer.set(
            "batchdata.v1.HistoricalDataService",
            health_pb2.HealthCheckResponse.SERVING,
        )
    logger.info("Registered: grpc.health.v1.Health")

    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info("gRPC server started on port %d (workers=%d)", port, max_workers)

    # ── Graceful shutdown ─────────────────────────────────────────────────────
    def _shutdown(signum, frame):
        logger.info("Received signal %d — shutting down gRPC server gracefully...", signum)
        health_servicer.set("", health_pb2.HealthCheckResponse.NOT_SERVING)
        server.stop(grace=5)  # 5s grace để drain in-flight requests
        logger.info("gRPC server stopped.")
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    # Block cho đến khi shutdown signal
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
