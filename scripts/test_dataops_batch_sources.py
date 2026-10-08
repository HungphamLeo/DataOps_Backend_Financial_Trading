#!/usr/bin/env python3
"""
test_dataops_batch_sources.py — Comprehensive test script for dataops_batch_api data sources.

Tests all REST client methods for:
  1. BinanceRestClient (Crypto: Klines/Candles, Exchange Instruments, Schema validation & Canonical Mapping)
  2. TCBSRestClient (VN Securities: Ticker Commons, Ticker Snaps, Derivatives, Put-through, Intraday BSA, Intraday History, Securities info, Pydantic & Canonical Mapping)

Usage:
  python scripts/test_dataops_batch_sources.py [--source all|binance|tcbs] [--verbose]
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

# Ensure dataops_batch_api is in Python module search path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
BATCH_API_DIR = PROJECT_ROOT / "dataops_batch_api"
if str(BATCH_API_DIR) not in sys.path:
    sys.path.insert(0, str(BATCH_API_DIR))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("test_batch_sources")


def print_section(title: str) -> None:
    sep = "=" * 80
    print(f"\n{sep}\n>>> {title.upper()}\n{sep}")


def print_json(data: Any, max_lines: int = 15) -> None:
    formatted = json.dumps(data, indent=2, ensure_ascii=False, default=str)
    lines = formatted.splitlines()
    if len(lines) > max_lines:
        print("\n".join(lines[:max_lines]))
        print(f"    ... [truncated {len(lines) - max_lines} lines]")
    else:
        print(formatted)


# ==============================================================================
# 1. BINANCE CLIENT TESTS
# ==============================================================================

def test_binance_sources(verbose: bool = False) -> None:
    print_section("Binance REST Client Data Ingestion Tests")
    
    from cli.ingestion.sources.binance_client import BinanceRestClient
    from cli.ingestion.schema.models import BinanceCandleRecord
    from cli.ingestion.schema.canonical import from_binance_kline_row

    client = BinanceRestClient.from_env()
    print(f"Initialized BinanceRestClient -> base_url: {client.base_url}")

    # Test 1.1: get_candles (Spot)
    print("\n--- 1.1: get_candles (BTCUSDT 1m, limit=5) ---")
    try:
        candles_spot = client.get_candles(inst_id="BTCUSDT", bar="1m", limit=5)
        print(f"✓ Success: fetched {len(candles_spot)} spot candles for BTCUSDT")
        if candles_spot:
            sample_candle = candles_spot[0]
            print(f"  Raw row[0] (openTime): {sample_candle[0]}")
            print(f"  Raw row[1..4] (OHLC): {sample_candle[1:5]}")
            print(f"  Raw row[5] (Volume): {sample_candle[5]}")
            print(f"  Raw row[7] (QuoteAssetVolume): {sample_candle[7]}")
            
            # Pydantic validation
            record = BinanceCandleRecord(
                inst_id="BTCUSDT",
                inst_type="SPOT",
                ts_ms=int(sample_candle[0]),
                open=float(sample_candle[1]),
                high=float(sample_candle[2]),
                low=float(sample_candle[3]),
                close=float(sample_candle[4]),
                vol=float(sample_candle[5]),
                vol_ccy=float(sample_candle[7]) if len(sample_candle) > 7 else 0.0,
                confirm=1,
            )
            print(f"  ✓ Pydantic validate OK: open={record.open}, high={record.high}, vol_ccy={record.vol_ccy}")
            
            # Canonical model mapping
            canonical = from_binance_kline_row(sample_candle, "BTCUSDT", "SPOT", "1m")
            print(f"  ✓ Canonical mapping OK: {canonical.symbol} @ {canonical.interval.value} -> O={canonical.open}, C={canonical.close}, QuoteVol={canonical.quote_volume}")
    except Exception as e:
        print(f"✗ Failed get_candles (Spot): {e}")

    # Test 1.2: get_candles (Different symbol & timeframe: ETHUSDT 1h)
    print("\n--- 1.2: get_candles (ETHUSDT 1h, limit=3) ---")
    try:
        candles_eth = client.get_candles(inst_id="ETHUSDT", bar="1h", limit=3)
        print(f"✓ Success: fetched {len(candles_eth)} candles for ETHUSDT 1h")
        if candles_eth:
            print(f"  Latest candle close: {candles_eth[-1][4]}")
    except Exception as e:
        print(f"✗ Failed get_candles (ETHUSDT): {e}")

    # Test 1.3: get_instruments (Exchange Info)
    print("\n--- 1.3: get_instruments (Exchange Info) ---")
    try:
        instruments = client.get_instruments(inst_type="SPOT")
        print(f"✓ Success: fetched {len(instruments)} trading symbols from Binance exchangeInfo")
        # Find some popular symbols
        usdt_pairs = [s["symbol"] for s in instruments if s.get("symbol", "").endswith("USDT")][:5]
        print(f"  Sample USDT trading pairs: {', '.join(usdt_pairs)}")
    except Exception as e:
        print(f"✗ Failed get_instruments: {e}")


# ==============================================================================
# 2. TCBS CLIENT TESTS
# ==============================================================================

def test_tcbs_sources(verbose: bool = False) -> None:
    print_section("TCBS iFlash REST Client Data Ingestion Tests")
    
    from cli.ingestion.sources.tcbs_client import TCBSRestClient, TCBSConstants
    from cli.ingestion.schema.models import (
        TCBSMarketRecord,
        TCBSDerivativeRecord,
        TCBSBSARecord,
        TCBSIntradayTickRecord,
    )
    from cli.ingestion.schema.canonical import from_tcbs_intraday_tick

    client = TCBSRestClient.from_env()
    
    print(f"Initialized TCBSRestClient -> base_url: {client.base_url}")
    print(f"API Key present: {bool(client.api_key)}")
    print(f"JWT Token present: {bool(client._jwt_token)}")
    if not client.api_key and not client._jwt_token:
        print("ℹ Note: TCBS Open API v1.0.0 requires authentication (API Key / JWT Token) for live requests.")
        print("  Set TCBS_API_KEY or TCBS_JWT_TOKEN in environment (.env) to authenticate live requests.")

    # Helper function to invoke underlying method without waiting on retry loop if not authenticated
    def _call(desc: str, fn, *fn_args, **fn_kwargs):
        print(f"\n--- {desc} ---")
        try:
            target = fn.__wrapped__ if hasattr(fn, "__wrapped__") else fn
            res = target(*fn_args, **fn_kwargs)
            print(f"✓ Success: returned data of type {type(res).__name__}")
            return res
        except Exception as e:
            print(f"✗ Failed / Auth required: {e}")
            return None

    # Test 2.1: get_ticker_commons (Specific tickers)
    res = _call("2.1: get_ticker_commons (tickers=['TCB', 'VCB', 'FPT', 'HPG'])",
                client.get_ticker_commons,
                client, tickers=["TCB", "VCB", "FPT", "HPG"])
    if res and isinstance(res, dict):
        data = res.get("data", [])
        trading_date = res.get("tradingDate", "N/A")
        print(f"  Fetched {len(data)} tickers (tradingDate: {trading_date})")

    # Test 2.2: get_ticker_commons (Basket index: VN30)
    _call("2.2: get_ticker_commons (index='VN30')",
          client.get_ticker_commons,
          client, index="VN30")

    # Test 2.3: get_ticker_snaps (Foreign room & market snapshot)
    _call("2.3: get_ticker_snaps (index='HOSE')",
          client.get_ticker_snaps,
          client, index="HOSE")

    # Test 2.4: get_derivatives (VN30F contracts)
    _call("2.4: get_derivatives (Derivatives Board)",
          client.get_derivatives,
          client)

    # Test 2.5: get_put_through_snaps (Giao dịch thỏa thuận)
    _call("2.5: get_put_through_snaps (Put-Through Deals)",
          client.get_put_through_snaps,
          client)

    # Test 2.6: get_intraday_bsa (Buy/Sell Analysis for TCB)
    _call("2.6: get_intraday_bsa (ticker='TCB', investor_type='all')",
          client.get_intraday_bsa,
          client, ticker="TCB", investor_type="all")

    # Test 2.7: get_intraday_bsa_ext & get_intraday_bsa_month
    _call("2.7a: get_intraday_bsa_ext (ticker='TCB')",
          client.get_intraday_bsa_ext,
          client, ticker="TCB")
    _call("2.7b: get_intraday_bsa_month (ticker='TCB')",
          client.get_intraday_bsa_month,
          client, ticker="TCB")

    # Test 2.8: get_intraday_history (Tick-by-tick paging for TCB)
    _call("2.8: get_intraday_history (ticker='TCB', page=0, size=5)",
          client.get_intraday_history,
          client, ticker="TCB", page=0, size=5)

    # Test 2.9: Batch helpers
    print("\n--- 2.9: Batch Helpers ---")
    try:
        batch_stocks = ["VCB", "TCB", "HPG"]
        batch_res = client.get_ticker_commons_batch(tickers=batch_stocks, batch_size=2)
        print(f"✓ get_ticker_commons_batch executed: {len(batch_res)} items fetched")
    except Exception as e:
        print(f"✗ get_ticker_commons_batch: {e}")

    # Test 2.10: get_securities
    _call("2.10: get_securities (filter='symbol=TCB')",
          client.get_securities,
          client, filter_expr="symbol=TCB", require_auth=False)


# ==============================================================================
# MAIN ENTRYPOINT
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="Test Binance & TCBS REST API batch ingestion sources.")
    parser.add_argument(
        "--source",
        choices=["all", "binance", "tcbs"],
        default="all",
        help="Specify which source to test (default: all)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable detailed debug logs and data dumps",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    print("\n" + "#" * 80)
    print("### STARTING DATAOPS BATCH INGESTION SOURCES TEST SUITE")
    print(f"### Target Sources: {args.source.upper()}")
    print("#" * 80)

    if args.source in ("all", "binance"):
        test_binance_sources(verbose=args.verbose)

    if args.source in ("all", "tcbs"):
        test_tcbs_sources(verbose=args.verbose)

    print("\n" + "#" * 80)
    print("### ALL TEST CALLS EXECUTED")
    print("#" * 80 + "\n")


if __name__ == "__main__":
    main()
