# -*- coding: utf-8 -*-
# Generated from batch_data.proto (package batchdata.v1)
# Handwritten stub — grpc_tools.protoc unavailable on this build environment.
# Semantics identical to protoc grpc output.
#
# Proto source:  dataops_protobuf/batch-data/batch_data.proto
# Regenerate:    cd dataops_protobuf && python -m grpc_tools.protoc \
#                    -I. --python_out=../dataops_batch_api \
#                    --grpc_python_out=../dataops_batch_api \
#                    batch-data/batch_data.proto
from __future__ import annotations

import grpc

import batch_data_pb2 as _batch_data_pb2


# ---------------------------------------------------------------------------
# gRPC method descriptors
# Method names must match /batchdata.v1.HistoricalDataService/{MethodName}
# ---------------------------------------------------------------------------

_GRPC_SERVICE = "batchdata.v1.HistoricalDataService"


# ---------------------------------------------------------------------------
# Server-side: Servicer base class
# Subclass this and override each method in grpc_server.py.
# ---------------------------------------------------------------------------

class HistoricalDataServiceServicer:
    """
    Base class for HistoricalDataService server implementation.

    All methods raise NOT_IMPLEMENTED by default — override in subclass.
    This mirrors the generated servicer base from protoc --grpc_python_out.
    """

    def GetBinanceOHLCV(self, request, context):
        """Trả về Binance OHLCV candles từ Delta Lake (bronze/binance_candles_4h | 1d)."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def GetTCBSMarketEOD(self, request, context):
        """Trả về TCBS daily EOD snapshot từ Delta Lake (bronze/tcbs_market_eod)."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def GetTCBSBSAHistory(self, request, context):
        """Trả về TCBS BSA daily summary từ Delta Lake (bronze/tcbs_bsa_daily)."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def GetDataRange(self, request, context):
        """Metadata query: oldest/newest date và total_count cho 1 data source."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


# ---------------------------------------------------------------------------
# Server registration helper
# Called by grpc_server.py:
#   batch_data_pb2_grpc.add_HistoricalDataServiceServicer_to_server(servicer, server)
# ---------------------------------------------------------------------------

def add_HistoricalDataServiceServicer_to_server(servicer, server):
    """
    Register a HistoricalDataServiceServicer instance onto a grpc.Server.

    Mirrors the protoc-generated add_*Servicer_to_server() function.
    Uses grpc.method_service_handler with unary_unary for all 4 RPCs.
    """
    from grpc import unary_unary_rpc_method_handler as _unary_unary

    generic_handler = grpc.method_service_handler if hasattr(grpc, "method_service_handler") else None

    # Build per-method handlers using grpc.unary_unary_rpc_method_handler
    rpc_method_handlers = {
        "GetBinanceOHLCV": _unary_unary(
            servicer.GetBinanceOHLCV,
            request_deserializer=None,
            response_serializer=None,
        ),
        "GetTCBSMarketEOD": _unary_unary(
            servicer.GetTCBSMarketEOD,
            request_deserializer=None,
            response_serializer=None,
        ),
        "GetTCBSBSAHistory": _unary_unary(
            servicer.GetTCBSBSAHistory,
            request_deserializer=None,
            response_serializer=None,
        ),
        "GetDataRange": _unary_unary(
            servicer.GetDataRange,
            request_deserializer=None,
            response_serializer=None,
        ),
    }

    generic_handler = grpc.method_service_handler if hasattr(grpc, "method_service_handler") else None
    server.add_generic_rpc_handlers((
        grpc.method_service_handler(_GRPC_SERVICE, rpc_method_handlers)
        if generic_handler
        else _GenericMethodHandler(_GRPC_SERVICE, rpc_method_handlers),
    ))


class _GenericMethodHandler(grpc.ServiceRpcHandler):
    """
    Fallback generic handler khi grpc.method_service_handler không có.
    Dùng grpc.GenericRpcHandler pattern (grpcio < 1.60 compat).
    """

    def __init__(self, service_name: str, method_handlers: dict) -> None:
        self._name     = service_name
        self._handlers = method_handlers

    def service_name(self) -> str:
        return self._name

    def service(self, handler_call_details):
        method = handler_call_details.method.split("/")[-1]
        return self._handlers.get(method)


# ---------------------------------------------------------------------------
# Client-side stub
# Not used by grpc_server.py itself, but useful for integration tests.
# ---------------------------------------------------------------------------

class HistoricalDataServiceStub:
    """
    Client stub cho HistoricalDataService.
    Dùng trong integration tests hoặc CLI tools.
    """

    def __init__(self, channel) -> None:
        self.GetBinanceOHLCV = channel.unary_unary(
            f"/{_GRPC_SERVICE}/GetBinanceOHLCV",
        )
        self.GetTCBSMarketEOD = channel.unary_unary(
            f"/{_GRPC_SERVICE}/GetTCBSMarketEOD",
        )
        self.GetTCBSBSAHistory = channel.unary_unary(
            f"/{_GRPC_SERVICE}/GetTCBSBSAHistory",
        )
        self.GetDataRange = channel.unary_unary(
            f"/{_GRPC_SERVICE}/GetDataRange",
        )
