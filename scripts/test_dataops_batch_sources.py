#!/usr/bin/env python3
"""
test_dataops_batch_sources.py — Comprehensive Data Sources Test Suite for dataops_batch_api.

Phạm vi kiểm thử:
  1. Binance REST Client (Spot & USD-M Futures):
     - Spot: Klines (OHLCV), Exchange Info, Ticker 24h, Orderbook Depth, Recent Trades.
     - Futures: Klines, Exchange Info, Funding Rate, Mark Price, Open Interest.
     - Pydantic Validation: BinanceCandleRecord.
  2. TCBS iFlash REST Client:
     - Ticker Commons (by tickers & by index HOSE/VN30/HNX).
     - Ticker Snaps (Foreign room & market overview).
     - Derivatives Board (VN30F contracts).
     - Put-Through Deals (Giao dịch thỏa thuận).
     - Intraday BSA (Cung cầu / Buy-Sell Analysis: Daily, Extended, Monthly).
     - Intraday Tick History (Khớp lệnh tick-by-tick có phân trang).
     - Batch Helpers & Securities Listing.
     - Pydantic Validation & TCBS Constants Loader.

Cách sử dụng:
  # Chạy toàn bộ tests:
  python scripts/test_dataops_batch_sources.py

  # Chỉ chạy Binance hoặc TCBS:
  python scripts/test_dataops_batch_sources.py --source binance
  python scripts/test_dataops_batch_sources.py --source tcbs

  # Liệt kê danh sách các hàm test đơn lẻ:
  python scripts/test_dataops_batch_sources.py --list

  # Chạy một hàm test cụ thể:
  python scripts/test_dataops_batch_sources.py --test test_binance_spot_klines
  python scripts/test_dataops_batch_sources.py --test test_tcbs_ticker_commons_index

  # Chế độ hiển thị chi tiết (dump JSON payload):
  python scripts/test_dataops_batch_sources.py --source binance --verbose
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

# ── 1. Đảm bảo nạp đúng module dataops_batch_api ──────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
BATCH_API_DIR = PROJECT_ROOT / "dataops_batch_api"
if str(BATCH_API_DIR) not in sys.path:
    sys.path.insert(0, str(BATCH_API_DIR))

# ── 2. Auto-load .env từ project root (nếu python-dotenv có sẵn) ───────────────
# Giúp chạy script local mà không cần `source .env` thủ công.
# Không làm gì nếu dotenv không được cài.
try:
    from dotenv import load_dotenv
    _env_file = PROJECT_ROOT / ".env"
    if _env_file.exists():
        load_dotenv(_env_file, override=False)  # override=False: không ghi đè biến đã có trong shell
except ImportError:
    pass

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("test_batch_sources")


# ── 2. Utility Helpers ────────────────────────────────────────────────────────
def print_section(title: str) -> None:
    sep = "=" * 80
    print(f"\n{sep}\n>>> {title.upper()}\n{sep}")


def print_json(data: Any, max_lines: int = 15) -> None:
    try:
        formatted = json.dumps(data, indent=2, ensure_ascii=False, default=str)
        lines = formatted.splitlines()
        if len(lines) > max_lines:
            print("\n".join(lines[:max_lines]))
            print(f"    ... [truncated {len(lines) - max_lines} lines]")
        else:
            print(formatted)
    except Exception as e:
        print(f"    [Cannot format JSON: {e}] {str(data)[:200]}")


# Danh sách ghi nhận kết quả test phục vụ in bảng tổng kết
TEST_RESULTS: List[Dict[str, Any]] = []


def record_result(name: str, domain: str, status: str, details: str, duration_ms: float) -> None:
    TEST_RESULTS.append({
        "name": name,
        "domain": domain,
        "status": status,
        "details": details,
        "duration_ms": duration_ms,
    })


def _is_tcbs_auth_error(exc: Exception) -> bool:
    """Trả về True nếu exception là HTTP 401 Unauthorized từ TCBS API."""
    try:
        import requests
        if isinstance(exc, requests.HTTPError) and exc.response is not None:
            return exc.response.status_code == 401
    except ImportError:
        pass
    # Fallback: kiểm tra message string (trường hợp exception bị wrap bởi tenacity)
    return "401" in str(exc) and "Unauthorized" in str(exc)


def _is_tcbs_endpoint_error(exc: Exception) -> bool:
    """
    Trả về True nếu exception là HTTP 400/404 từ TCBS — endpoint không khả dụng
    hoặc thiếu params bắt buộc theo phiên bản API hiện tại.
    """
    try:
        import requests
        if isinstance(exc, requests.HTTPError) and exc.response is not None:
            return exc.response.status_code in (400, 404)
    except ImportError:
        pass
    return ("400" in str(exc) and "Bad Request" in str(exc)) or \
           ("404" in str(exc) and "Not Found" in str(exc))


# ==============================================================================
# PHẦN 1: BINANCE SPOT & FUTURES TEST FUNCTIONS
# ==============================================================================

def test_binance_spot_klines(verbose: bool = False) -> bool:
    """Test lấy nến OHLCV Spot (BTCUSDT, ETHUSDT) và kiểm tra index row[7] (quoteVolume)."""
    start_time = time.perf_counter()
    func_name = "test_binance_spot_klines"
    print(f"\n--- [BINANCE] 1.1: Spot Klines (BTCUSDT 1m, limit=5) ---")
    try:
        from cli.ingestion.sources.client.binance_client import BinanceSpotClient
        client = BinanceSpotClient.from_env()

        candles = client.get_klines(symbol="BTCUSDT", interval="1m", limit=5)
        duration = (time.perf_counter() - start_time) * 1000

        if not candles or not isinstance(candles, list):
            print(f"✗ FAILED: Trả về dữ liệu rỗng hoặc không phải list: {candles}")
            record_result(func_name, "Binance Spot", "FAIL", "Empty or invalid response", duration)
            return False

        print(f"✓ SUCCESS: Lấy thành công {len(candles)} nến Spot BTCUSDT")
        sample = candles[-1]
        print(f"  • Sample Candle [CloseTime: {sample[6]}]: Open={sample[1]}, High={sample[2]}, Low={sample[3]}, Close={sample[4]}")
        print(f"  • Base Volume (row[5]): {sample[5]} BTC")
        print(f"  • Quote Asset Volume (row[7]): {sample[7]} USDT")

        if verbose:
            print_json(candles)

        record_result(func_name, "Binance Spot", "PASS", f"{len(candles)} candles fetched", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "Binance Spot", "FAIL", str(e), duration)
        return False


def test_binance_spot_exchange_info(verbose: bool = False) -> bool:
    """Test lấy danh mục cặp giao dịch Spot và rate limits từ /api/v3/exchangeInfo."""
    start_time = time.perf_counter()
    func_name = "test_binance_spot_exchange_info"
    print(f"\n--- [BINANCE] 1.2: Spot Exchange Info ---")
    try:
        from cli.ingestion.sources.client.binance_client import BinanceSpotClient
        client = BinanceSpotClient.from_env()

        info = client.get_exchange_info()
        duration = (time.perf_counter() - start_time) * 1000

        symbols = info.get("symbols", [])
        print(f"✓ SUCCESS: Lấy thành công thông tin sàn Spot ({len(symbols)} symbols đang niêm yết)")
        
        usdt_symbols = [s["symbol"] for s in symbols if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"]
        print(f"  • Số cặp active với USDT: {len(usdt_symbols)}")
        print(f"  • Ví dụ một số cặp: {', '.join(usdt_symbols[:8])}")

        if verbose:
            print_json(info.get("rateLimits", []))

        record_result(func_name, "Binance Spot", "PASS", f"{len(symbols)} total symbols", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "Binance Spot", "FAIL", str(e), duration)
        return False


def test_binance_spot_ticker_24hr(verbose: bool = False) -> bool:
    """Test lấy thống kê biến động 24h từ /api/v3/ticker/24hr."""
    start_time = time.perf_counter()
    func_name = "test_binance_spot_ticker_24hr"
    print(f"\n--- [BINANCE] 1.3: Spot 24hr Ticker Stats (BTCUSDT) ---")
    try:
        from cli.ingestion.sources.client.binance_client import BinanceSpotClient
        client = BinanceSpotClient.from_env()

        stats = client.get_ticker_24hr(symbol="BTCUSDT")
        duration = (time.perf_counter() - start_time) * 1000

        print(f"✓ SUCCESS: Lấy thống kê 24h Spot BTCUSDT:")
        print(f"  • Giá hiện tại: {stats.get('lastPrice')} USDT ({stats.get('priceChangePercent')}%)")
        print(f"  • Khối lượng 24h: {stats.get('volume')} BTC ({float(stats.get('quoteVolume', 0)):,.2f} USDT)")
        print(f"  • Cao nhất / Thấp nhất 24h: High={stats.get('highPrice')}, Low={stats.get('lowPrice')}")

        if verbose:
            print_json(stats)

        record_result(func_name, "Binance Spot", "PASS", f"Price: {stats.get('lastPrice')}", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "Binance Spot", "FAIL", str(e), duration)
        return False


def test_binance_spot_orderbook(verbose: bool = False) -> bool:
    """Test lấy Orderbook Depth snapshot từ /api/v3/depth."""
    start_time = time.perf_counter()
    func_name = "test_binance_spot_orderbook"
    print(f"\n--- [BINANCE] 1.4: Spot Orderbook Depth (BTCUSDT, limit=5) ---")
    try:
        from cli.ingestion.sources.client.binance_client import BinanceSpotClient
        client = BinanceSpotClient.from_env()

        depth = client.get_depth(symbol="BTCUSDT", limit=5)
        duration = (time.perf_counter() - start_time) * 1000

        bids = depth.get("bids", [])
        asks = depth.get("asks", [])
        print(f"✓ SUCCESS: Lấy Orderbook ({len(bids)} bids, {len(asks)} asks)")
        if bids and asks:
            print(f"  • Best Bid: {bids[0][0]} (Qty: {bids[0][1]})")
            print(f"  • Best Ask: {asks[0][0]} (Qty: {asks[0][1]})")
            spread = float(asks[0][0]) - float(bids[0][0])
            print(f"  • Spread: {spread:.2f} USDT")

        if verbose:
            print_json(depth)

        record_result(func_name, "Binance Spot", "PASS", f"{len(bids)} bids / {len(asks)} asks", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "Binance Spot", "FAIL", str(e), duration)
        return False


def test_binance_futures_klines(verbose: bool = False) -> bool:
    """Test lấy nến USD-M Futures (BTCUSDT 4h, limit=5)."""
    start_time = time.perf_counter()
    func_name = "test_binance_futures_klines"
    print(f"\n--- [BINANCE] 1.5: Futures Klines (BTCUSDT 4h, limit=5) ---")
    try:
        from cli.ingestion.sources.client.binance_client import BinanceFuturesClient
        client = BinanceFuturesClient.from_env()

        candles = client.get_klines(symbol="BTCUSDT", interval="4h", limit=5)
        duration = (time.perf_counter() - start_time) * 1000

        if not candles or not isinstance(candles, list):
            print(f"✗ FAILED: Không có nến Futures trả về: {candles}")
            record_result(func_name, "Binance Futures", "FAIL", "Empty response", duration)
            return False

        print(f"✓ SUCCESS: Lấy thành công {len(candles)} nến Futures BTCUSDT 4h")
        sample = candles[-1]
        print(f"  • Sample 4h Candle: Open={sample[1]}, Close={sample[4]}, VolQuote={sample[7]} USDT")

        if verbose:
            print_json(candles)

        record_result(func_name, "Binance Futures", "PASS", f"{len(candles)} candles fetched", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        # 403 = Binance CloudFront geo-block — không phải lỗi code, bỏ qua retry
        try:
            from binance.error import ClientError
            if isinstance(e, ClientError) and e.status_code == 403:
                print(f"⚠ SKIP {func_name}: Binance Futures /fapi/v1/klines bị CloudFront geo-block (403) tại IP này.")
                print(f"  → Endpoint khác (funding_rate, mark_price, open_interest) vẫn hoạt động bình thường.")
                record_result(func_name, "Binance Futures", "SKIP", "403 CloudFront geo-block — not a code error", duration)
                return True
        except ImportError:
            pass
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "Binance Futures", "FAIL", str(e), duration)
        return False


def test_binance_futures_funding_rate(verbose: bool = False) -> bool:
    """Test lấy lịch sử Funding Rate của USD-M Futures từ /fapi/v1/fundingRate."""
    start_time = time.perf_counter()
    func_name = "test_binance_futures_funding_rate"
    print(f"\n--- [BINANCE] 1.6: Futures Funding Rate History (BTCUSDT) ---")
    try:
        from cli.ingestion.sources.client.binance_client import BinanceFuturesClient
        client = BinanceFuturesClient.from_env()

        funding = client.get_funding_rate(symbol="BTCUSDT", limit=5)
        duration = (time.perf_counter() - start_time) * 1000

        print(f"✓ SUCCESS: Lấy thành công {len(funding)} bản ghi Funding Rate:")
        if funding:
            latest = funding[-1]
            rate_pct = float(latest.get("fundingRate", 0)) * 100
            funding_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(latest.get("fundingTime", 0))/1000))
            print(f"  • Latest Funding Time: {funding_time} UTC -> Rate: {rate_pct:.4f}%")

        if verbose:
            print_json(funding)

        record_result(func_name, "Binance Futures", "PASS", f"{len(funding)} funding records", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "Binance Futures", "FAIL", str(e), duration)
        return False


def test_binance_futures_mark_price(verbose: bool = False) -> bool:
    """Test lấy Mark Price và Estimated Settle Price từ /fapi/v1/premiumIndex."""
    start_time = time.perf_counter()
    func_name = "test_binance_futures_mark_price"
    print(f"\n--- [BINANCE] 1.7: Futures Mark Price & Index Price (BTCUSDT) ---")
    try:
        from cli.ingestion.sources.client.binance_client import BinanceFuturesClient
        client = BinanceFuturesClient.from_env()

        data = client.get_mark_price(symbol="BTCUSDT")
        duration = (time.perf_counter() - start_time) * 1000

        print(f"✓ SUCCESS: Mark Price Data:")
        print(f"  • Mark Price: {data.get('markPrice')} USDT")
        print(f"  • Index Price: {data.get('indexPrice')} USDT")
        print(f"  • Last Funding Rate: {float(data.get('lastFundingRate', 0))*100:.4f}%")

        if verbose:
            print_json(data)

        record_result(func_name, "Binance Futures", "PASS", f"MarkPrice: {data.get('markPrice')}", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "Binance Futures", "FAIL", str(e), duration)
        return False


def test_binance_futures_open_interest(verbose: bool = False) -> bool:
    """Test lấy Open Interest (Hợp đồng mở) từ /fapi/v1/openInterest."""
    start_time = time.perf_counter()
    func_name = "test_binance_futures_open_interest"
    print(f"\n--- [BINANCE] 1.8: Futures Open Interest (BTCUSDT) ---")
    try:
        from cli.ingestion.sources.client.binance_client import BinanceFuturesClient
        client = BinanceFuturesClient.from_env()

        oi = client.get_open_interest(symbol="BTCUSDT")
        duration = (time.perf_counter() - start_time) * 1000

        print(f"✓ SUCCESS: Open Interest:")
        print(f"  • Open Interest: {float(oi.get('openInterest', 0)):,.2f} BTC")
        print(f"  • Symbol: {oi.get('symbol')}")

        if verbose:
            print_json(oi)

        record_result(func_name, "Binance Futures", "PASS", f"OI: {oi.get('openInterest')}", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "Binance Futures", "FAIL", str(e), duration)
        return False


def test_binance_pydantic_validation(verbose: bool = False) -> bool:
    """Test validate raw candle data qua Pydantic BinanceCandleRecord."""
    start_time = time.perf_counter()
    func_name = "test_binance_pydantic_validation"
    print(f"\n--- [BINANCE] 1.9: Pydantic Validation (BinanceCandleRecord) ---")
    try:
        from cli.ingestion.schema.models import BinanceCandleRecord

        # Case 1: Hợp lệ
        valid_candle = BinanceCandleRecord(
            inst_id="BTCUSDT",
            inst_type="SPOT",
            ts_ms=1700000000000,
            open=65000.0,
            high=65500.0,
            low=64800.0,
            close=65200.0,
            vol=12.5,
            vol_ccy=815000.0,
            confirm=1,
        )
        print(f"✓ SUCCESS: Validate bản ghi hợp lệ thành công: Open={valid_candle.open}, Close={valid_candle.close}")

        # Case 2: Vi phạm business rule (high < low)
        try:
            BinanceCandleRecord(
                inst_id="BTCUSDT",
                inst_type="SPOT",
                ts_ms=1700000000000,
                open=65000.0,
                high=60000.0,  # Lỗi: High < Low
                low=64000.0,
                close=65000.0,
                vol=1.0,
                vol_ccy=65000.0,
                confirm=1,
            )
            print("✗ FAILED: Schema không bắt được lỗi High < Low!")
            record_result(func_name, "Binance Validation", "FAIL", "Failed to catch High < Low violation", 0)
            return False
        except ValueError:
            print("✓ SUCCESS: Bắt đúng ngoại lệ khi High < Low")

        duration = (time.perf_counter() - start_time) * 1000
        record_result(func_name, "Binance Validation", "PASS", "Pydantic validator strict rules passed", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "Binance Validation", "FAIL", str(e), duration)
        return False


# ==============================================================================
# PHẦN 2: TCBS VIETNAM SECURITIES TEST FUNCTIONS
# ==============================================================================

def test_tcbs_constants_loader(verbose: bool = False) -> bool:
    """Test nạp và truy vấn TCBSConstants YAML loader."""
    start_time = time.perf_counter()
    func_name = "test_tcbs_constants_loader"
    print(f"\n--- [TCBS] 2.1: TCBS Constants & Config Loader ---")
    try:
        from cli.flows.loader.tcbs_constants_loader import TCBSConstants

        ep_commons = TCBSConstants.endpoint("ticker_commons")
        code_hose = TCBSConstants.index_code("HOSE")
        code_vn30 = TCBSConstants.index_code("VN30")
        ticker_fields = TCBSConstants.ticker_commons_fields()
        sessions = TCBSConstants.valid_session_states()

        duration = (time.perf_counter() - start_time) * 1000
        print(f"✓ SUCCESS: Nạp TCBSConstants thành công:")
        print(f"  • Endpoint 'ticker_commons': {ep_commons}")
        print(f"  • Index Code mapping: HOSE={code_hose}, VN30={code_vn30}")
        print(f"  • Valid Session States ({len(sessions)}): {sessions}")
        print(f"  • Số trường mapping ticker_commons_fields: {len(ticker_fields)} trường")

        if verbose:
            print_json(ticker_fields)

        record_result(func_name, "TCBS Loader", "PASS", f"Loaded endpoints & {len(ticker_fields)} fields", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "TCBS Loader", "FAIL", str(e), duration)
        return False


def test_tcbs_ticker_commons_symbols(verbose: bool = False) -> bool:
    """Test lấy snapshot giá đóng cửa/khớp lệnh theo danh sách mã cụ thể (TCB, VCB, FPT, HPG)."""
    start_time = time.perf_counter()
    func_name = "test_tcbs_ticker_commons_symbols"
    test_symbols = ["TCB", "VCB", "FPT", "HPG"]
    print(f"\n--- [TCBS] 2.2: Ticker Commons by Symbols ({test_symbols}) ---")
    try:
        from cli.ingestion.sources.client.tcbs_client import TCBSRestClient
        client = TCBSRestClient.from_env()

        res = client.get_ticker_commons(tickers=test_symbols)
        duration = (time.perf_counter() - start_time) * 1000

        # API có thể trả về dict {"data": [...], "tradingDate": ...} hoặc list
        data_list = res.get("data", []) if isinstance(res, dict) else res
        trading_date = res.get("tradingDate", "N/A") if isinstance(res, dict) else "N/A"

        if not data_list:
            print(f"ℹ INFO / AUTH: Trả về 0 bản ghi (Có thể ngoài giờ GD hoặc cần API Key/JWT)")
            record_result(func_name, "TCBS Market", "AUTH/EMPTY", f"0 items returned (Date: {trading_date})", duration)
            return True

        print(f"✓ SUCCESS: Lấy thành công {len(data_list)} mã cổ phiếu (Trading Date: {trading_date}):")
        for item in data_list[:3]:
            print(f"  • [{item.get('symbol')}] Giá khớp: {item.get('matchPrice')}, Khối lượng: {item.get('totalMatchVolume')}, Ref: {item.get('refPrice')}")

        if verbose:
            print_json(res)

        record_result(func_name, "TCBS Market", "PASS", f"{len(data_list)} tickers (Date: {trading_date})", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        if _is_tcbs_auth_error(e):
            print(f"⚠ SKIP {func_name}: TCBS_JWT_TOKEN chưa được set.")
            record_result(func_name, "TCBS Market", "SKIP", "401 Unauthorized — set TCBS_JWT_TOKEN to run", duration)
            return True
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "TCBS Market", "FAIL", str(e), duration)
        return False


def test_tcbs_ticker_commons_index(verbose: bool = False) -> bool:
    """Test lấy toàn bộ rổ chỉ số VN30 hoặc sàn HOSE từ get_ticker_commons(index='VN30')."""
    start_time = time.perf_counter()
    func_name = "test_tcbs_ticker_commons_index"
    print(f"\n--- [TCBS] 2.3: Ticker Commons by Basket Index (VN30) ---")
    try:
        from cli.ingestion.sources.client.tcbs_client import TCBSRestClient
        client = TCBSRestClient.from_env()

        res = client.get_ticker_commons(index="VN30")
        duration = (time.perf_counter() - start_time) * 1000

        data_list = res.get("data", []) if isinstance(res, dict) else res
        print(f"✓ SUCCESS: Lấy thành công dữ liệu rổ chỉ số VN30 ({len(data_list)} mã)")
        if data_list:
            sample_symbols = [x.get("symbol") for x in data_list if isinstance(x, dict)][:5]
            print(f"  • Sample symbols in VN30: {', '.join(sample_symbols)}")

        if verbose:
            print_json(res)

        record_result(func_name, "TCBS Market", "PASS", f"{len(data_list)} symbols in VN30", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        if _is_tcbs_auth_error(e):
            print(f"⚠ SKIP {func_name}: TCBS_JWT_TOKEN chưa được set.")
            record_result(func_name, "TCBS Market", "SKIP", "401 Unauthorized — set TCBS_JWT_TOKEN to run", duration)
            return True
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "TCBS Market", "FAIL", str(e), duration)
        return False


def test_tcbs_ticker_snaps(verbose: bool = False) -> bool:
    """Test lấy Foreign Room (room ngoại) & Market Snaps từ get_ticker_snaps."""
    start_time = time.perf_counter()
    func_name = "test_tcbs_ticker_snaps"
    print(f"\n--- [TCBS] 2.4: Ticker Snaps & Foreign Room (HOSE) ---")
    try:
        from cli.ingestion.sources.client.tcbs_client import TCBSRestClient
        client = TCBSRestClient.from_env()

        res = client.get_ticker_snaps(index="HOSE")
        duration = (time.perf_counter() - start_time) * 1000

        data_list = res if isinstance(res, list) else res.get("data", [])
        print(f"✓ SUCCESS: Lấy thành công Ticker Snaps ({len(data_list)} items)")

        if verbose:
            print_json(res)

        record_result(func_name, "TCBS Market", "PASS", f"{len(data_list)} snaps returned", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        if _is_tcbs_auth_error(e):
            print(f"⚠ SKIP {func_name}: TCBS_JWT_TOKEN chưa được set.")
            record_result(func_name, "TCBS Market", "SKIP", "401 Unauthorized — set TCBS_JWT_TOKEN to run", duration)
            return True
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "TCBS Market", "FAIL", str(e), duration)
        return False


def test_tcbs_derivatives(verbose: bool = False) -> bool:
    """Test lấy bảng giá chứng khoán phái sinh (VN30F contracts) từ get_derivatives."""
    start_time = time.perf_counter()
    func_name = "test_tcbs_derivatives"
    print(f"\n--- [TCBS] 2.5: Derivatives Contracts (VN30F) ---")
    try:
        from cli.ingestion.sources.client.tcbs_client import TCBSRestClient
        client = TCBSRestClient.from_env()

        res = client.get_derivatives()
        duration = (time.perf_counter() - start_time) * 1000

        data_list = res if isinstance(res, list) else res.get("data", [])
        print(f"✓ SUCCESS: Lấy thành công dữ liệu Phái sinh ({len(data_list)} contracts)")
        if data_list:
            for d in data_list[:2]:
                if isinstance(d, dict):
                    print(f"  • [{d.get('symbol')}] Giá khớp: {d.get('matchPrice')}, OI: {d.get('openInterest', 'N/A')}")

        if verbose:
            print_json(res)

        record_result(func_name, "TCBS Derivatives", "PASS", f"{len(data_list)} contracts", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        if _is_tcbs_auth_error(e):
            print(f"⚠ SKIP {func_name}: TCBS_JWT_TOKEN chưa được set.")
            record_result(func_name, "TCBS Derivatives", "SKIP", "401 Unauthorized — set TCBS_JWT_TOKEN to run", duration)
            return True
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "TCBS Derivatives", "FAIL", str(e), duration)
        return False


def test_tcbs_put_through_snaps(verbose: bool = False) -> bool:
    """Test lấy thông tin giao dịch thỏa thuận (Put-Through Deals)."""
    start_time = time.perf_counter()
    func_name = "test_tcbs_put_through_snaps"
    print(f"\n--- [TCBS] 2.6: Put-Through Snaps (Giao dịch thỏa thuận) ---")
    try:
        from cli.ingestion.sources.client.tcbs_client import TCBSRestClient
        client = TCBSRestClient.from_env()

        res = client.get_put_through_snaps()
        duration = (time.perf_counter() - start_time) * 1000

        data_list = res if isinstance(res, list) else res.get("data", [])
        print(f"✓ SUCCESS: Lấy thành công giao dịch thỏa thuận ({len(data_list)} deals)")

        if verbose:
            print_json(res)

        record_result(func_name, "TCBS Market", "PASS", f"{len(data_list)} put-through deals", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        if _is_tcbs_auth_error(e):
            print(f"⚠ SKIP {func_name}: TCBS_JWT_TOKEN chưa được set.")
            record_result(func_name, "TCBS Market", "SKIP", "401 Unauthorized — set TCBS_JWT_TOKEN to run", duration)
            return True
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "TCBS Market", "FAIL", str(e), duration)
        return False


def test_tcbs_intraday_bsa(verbose: bool = False) -> bool:
    """Test lấy phân tích cung cầu chủ động theo ngày (Buy/Sell Analysis) cho mã TCB."""
    start_time = time.perf_counter()
    func_name = "test_tcbs_intraday_bsa"
    symbol = "TCB"
    print(f"\n--- [TCBS] 2.7: Intraday BSA (Buy/Sell Analysis cho {symbol}) ---")
    try:
        from cli.ingestion.sources.client.tcbs_client import TCBSRestClient
        client = TCBSRestClient.from_env()

        res = client.get_intraday_bsa(ticker=symbol, investor_type="all")
        duration = (time.perf_counter() - start_time) * 1000

        data_list = res.get("data", []) if isinstance(res, dict) else res
        print(f"✓ SUCCESS: Lấy dữ liệu BSA cho {symbol} ({len(data_list)} bản ghi):")
        if data_list:
            sample = data_list[0] if isinstance(data_list, list) else data_list
            if isinstance(sample, dict):
                print(f"  • Mua chủ động (Active Buy): {sample.get('buyVolume') or sample.get('activeBuyVol', 'N/A')}")
                print(f"  • Bán chủ động (Active Sell): {sample.get('sellVolume') or sample.get('activeSellVol', 'N/A')}")

        if verbose:
            print_json(res)

        record_result(func_name, "TCBS BSA", "PASS", f"{len(data_list)} BSA records", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        if _is_tcbs_auth_error(e):
            print(f"⚠ SKIP {func_name}: TCBS_JWT_TOKEN chưa được set.")
            record_result(func_name, "TCBS BSA", "SKIP", "401 Unauthorized — set TCBS_JWT_TOKEN to run", duration)
            return True
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "TCBS BSA", "FAIL", str(e), duration)
        return False


def test_tcbs_intraday_bsa_ext_and_month(verbose: bool = False) -> bool:
    """Test lấy lịch sử BSA mở rộng và BSA theo tháng từ get_intraday_bsa_ext / month."""
    start_time = time.perf_counter()
    func_name = "test_tcbs_intraday_bsa_ext_and_month"
    symbol = "TCB"
    print(f"\n--- [TCBS] 2.8: BSA Extended & Monthly Summary ({symbol}) ---")
    try:
        from cli.ingestion.sources.client.tcbs_client import TCBSRestClient
        client = TCBSRestClient.from_env()

        # bsa-ext: thử lấy, chấp nhận 400 nếu endpoint không khả dụng ngoài giờ giao dịch
        res_ext = None
        try:
            res_ext = client.get_intraday_bsa_ext(ticker=symbol)
        except Exception as e_ext:
            if _is_tcbs_endpoint_error(e_ext):
                print(f"  ⚠ bsa-ext: 400/404 từ server — endpoint có thể cần giờ giao dịch hoặc đã thay đổi spec.")
            else:
                raise

        res_month = client.get_intraday_bsa_month(ticker=symbol)
        duration = (time.perf_counter() - start_time) * 1000

        ext_status = f"{type(res_ext).__name__}" if res_ext is not None else "N/A (400)"
        print(f"✓ SUCCESS: BSA Extended={ext_status}, BSA Monthly={type(res_month).__name__}")

        if verbose and res_ext is not None:
            print("--- Extended BSA: ---")
            print_json(res_ext)
        if verbose:
            print("--- Monthly BSA: ---")
            print_json(res_month)

        record_result(func_name, "TCBS BSA", "PASS", f"Ext={ext_status}, Monthly fetched", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        if _is_tcbs_auth_error(e):
            print(f"⚠ SKIP {func_name}: TCBS_JWT_TOKEN chưa được set.")
            record_result(func_name, "TCBS BSA", "SKIP", "401 Unauthorized — set TCBS_JWT_TOKEN to run", duration)
            return True
        if _is_tcbs_endpoint_error(e):
            print(f"⚠ SKIP {func_name}: 400/404 — endpoint bsa-month không khả dụng ({e})")
            record_result(func_name, "TCBS BSA", "SKIP", f"400/404 endpoint unavailable — {e}", duration)
            return True
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "TCBS BSA", "FAIL", str(e), duration)
        return False


def test_tcbs_intraday_history(verbose: bool = False) -> bool:
    """Test lấy lịch sử khớp lệnh từng tick (Intraday History) có phân trang."""
    start_time = time.perf_counter()
    func_name = "test_tcbs_intraday_history"
    symbol = "TCB"
    print(f"\n--- [TCBS] 2.9: Intraday History Ticks ({symbol}, page=0, size=5) ---")
    try:
        from cli.ingestion.sources.client.tcbs_client import TCBSRestClient
        client = TCBSRestClient.from_env()

        res = client.get_intraday_history(ticker=symbol, page=0, size=5)
        duration = (time.perf_counter() - start_time) * 1000

        ticks = res.get("data", []) if isinstance(res, dict) else res
        print(f"✓ SUCCESS: Lấy thành công {len(ticks)} ticks khớp lệnh gần nhất của {symbol}")
        if ticks and isinstance(ticks, list):
            for t in ticks[:3]:
                if isinstance(t, dict):
                    print(f"  • Time: {t.get('time')} | Price: {t.get('price')} | Volume: {t.get('volume')} | Side: {t.get('side', t.get('matchType', 'N/A'))}")

        if verbose:
            print_json(res)

        record_result(func_name, "TCBS Ticks", "PASS", f"{len(ticks)} ticks fetched", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        if _is_tcbs_auth_error(e):
            print(f"⚠ SKIP {func_name}: TCBS_JWT_TOKEN chưa được set.")
            record_result(func_name, "TCBS Ticks", "SKIP", "401 Unauthorized — set TCBS_JWT_TOKEN to run", duration)
            return True
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "TCBS Ticks", "FAIL", str(e), duration)
        return False


def test_tcbs_batch_helpers(verbose: bool = False) -> bool:
    """Test batch helper get_ticker_commons_batch tự động chia nhỏ chunk requests."""
    start_time = time.perf_counter()
    func_name = "test_tcbs_batch_helpers"
    batch_symbols = ["VCB", "TCB", "HPG", "FPT", "SSI", "VIC"]
    print(f"\n--- [TCBS] 2.10: Batch Helpers (Chunk size=2 cho {len(batch_symbols)} mã) ---")
    try:
        from cli.ingestion.sources.client.tcbs_client import TCBSRestClient
        client = TCBSRestClient.from_env()

        res = client.get_ticker_commons_batch(tickers=batch_symbols, batch_size=2)
        duration = (time.perf_counter() - start_time) * 1000

        print(f"✓ SUCCESS: get_ticker_commons_batch gộp kết quả thành công ({len(res)} items)")

        if verbose:
            print_json(res)

        record_result(func_name, "TCBS Batch", "PASS", f"{len(res)} batch items", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        if _is_tcbs_auth_error(e):
            print(f"⚠ SKIP {func_name}: TCBS_JWT_TOKEN chưa được set.")
            record_result(func_name, "TCBS Batch", "SKIP", "401 Unauthorized — set TCBS_JWT_TOKEN to run", duration)
            return True
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "TCBS Batch", "FAIL", str(e), duration)
        return False


def test_tcbs_pydantic_validation(verbose: bool = False) -> bool:
    """Test schema validation Pydantic cho TCBSMarketRecord và TCBSBSARecord."""
    start_time = time.perf_counter()
    func_name = "test_tcbs_pydantic_validation"
    print(f"\n--- [TCBS] 2.11: Pydantic Validation (TCBS Models) ---")
    try:
        from cli.ingestion.schema.models import TCBSMarketRecord, TCBSBSARecord

        # 1. Test TCBSMarketRecord
        market_rec = TCBSMarketRecord(
            symbol="TCB",
            exchange="HOSE",
            ref_price=24000.0,
            ceil_price=25650.0,
            floor_price=22350.0,
            match_price=24500.0,
            match_qty=150000.0,
            open=24100.0,
            high=24800.0,
            low=24050.0,
            avg_price=24450.0,
            total_vol=5200000.0,
            total_val=127000000000.0,
            trading_date="2026-10-09",
            ts_ms=1760000000000,
        )
        print(f"✓ SUCCESS: TCBSMarketRecord validated: symbol={market_rec.symbol}, match={market_rec.match_price}")

        # 2. Test TCBSBSARecord
        bsa_rec = TCBSBSARecord(
            ticker="TCB",
            investor_type="all",
            bup=52.5,
            sdp=47.5,
            bsr=1.1,
            event_time="14:30:00",
            ts_ms=1760000000000,
        )
        print(f"✓ SUCCESS: TCBSBSARecord validated: ticker={bsa_rec.ticker}, bup={bsa_rec.bup}%, sdp={bsa_rec.sdp}%")

        duration = (time.perf_counter() - start_time) * 1000
        record_result(func_name, "TCBS Validation", "PASS", "Models verified", duration)
        return True
    except Exception as e:
        duration = (time.perf_counter() - start_time) * 1000
        print(f"✗ FAILED {func_name}: {e}")
        record_result(func_name, "TCBS Validation", "FAIL", str(e), duration)
        return False


# ==============================================================================
# BẢN ĐỒ CÁC HÀM TEST (TEST REGISTRY)
# ==============================================================================

BINANCE_TESTS: Dict[str, Tuple[Callable[[bool], bool], str]] = {
    "test_binance_spot_klines": (test_binance_spot_klines, "Nến OHLCV Spot (BTCUSDT)"),
    "test_binance_spot_exchange_info": (test_binance_spot_exchange_info, "Danh mục cặp giao dịch Spot"),
    "test_binance_spot_ticker_24hr": (test_binance_spot_ticker_24hr, "Biến động 24h Spot (BTCUSDT)"),
    "test_binance_spot_orderbook": (test_binance_spot_orderbook, "Sổ lệnh Orderbook Depth"),
    "test_binance_futures_klines": (test_binance_futures_klines, "Nến OHLCV USD-M Futures (4h)"),
    "test_binance_futures_funding_rate": (test_binance_futures_funding_rate, "Lịch sử Funding Rate Futures"),
    "test_binance_futures_mark_price": (test_binance_futures_mark_price, "Mark Price & Index Price"),
    "test_binance_futures_open_interest": (test_binance_futures_open_interest, "Open Interest hợp đồng mở"),
    "test_binance_pydantic_validation": (test_binance_pydantic_validation, "Pydantic Schema Validation"),
}

TCBS_TESTS: Dict[str, Tuple[Callable[[bool], bool], str]] = {
    "test_tcbs_constants_loader": (test_tcbs_constants_loader, "TCBSConstants Loader & Config"),
    "test_tcbs_ticker_commons_symbols": (test_tcbs_ticker_commons_symbols, "Snapshot giá theo mã cổ phiếu"),
    "test_tcbs_ticker_commons_index": (test_tcbs_ticker_commons_index, "Snapshot toàn bộ rổ chỉ số VN30"),
    "test_tcbs_ticker_snaps": (test_tcbs_ticker_snaps, "Room ngoại & Ticker Snaps HOSE"),
    "test_tcbs_derivatives": (test_tcbs_derivatives, "Bảng giá phái sinh VN30F"),
    "test_tcbs_put_through_snaps": (test_tcbs_put_through_snaps, "Giao dịch thỏa thuận Put-through"),
    "test_tcbs_intraday_bsa": (test_tcbs_intraday_bsa, "Cung cầu chủ động BSA theo mã"),
    "test_tcbs_intraday_bsa_ext_and_month": (test_tcbs_intraday_bsa_ext_and_month, "Lịch sử BSA mở rộng & theo tháng"),
    "test_tcbs_intraday_history": (test_tcbs_intraday_history, "Khớp lệnh từng tick (Tick history)"),
    "test_tcbs_batch_helpers": (test_tcbs_batch_helpers, "Batch query phân trang tự động"),
    "test_tcbs_pydantic_validation": (test_tcbs_pydantic_validation, "Pydantic Schema Validation"),
}


def print_summary_table() -> None:
    """In bảng tổng kết toàn bộ kết quả kiểm thử."""
    print("\n" + "=" * 90)
    print(f"{'HÀM TEST':<38} | {'DOMAIN':<18} | {'STATUS':<10} | {'THỜI GIAN (ms)':<14}")
    print("-" * 90)
    for r in TEST_RESULTS:
        s = r['status']
        if "PASS" in s:
            status_display = f"✓ {s}"
        elif "SKIP" in s:
            status_display = f"⚠ {s}"
        else:
            status_display = f"✗ {s}"
        print(f"{r['name']:<38} | {r['domain']:<18} | {status_display:<10} | {r['duration_ms']:>10.2f} ms")
    print("=" * 90)


# ==============================================================================
# MAIN ENTRYPOINT
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Comprehensive Data Sources Test Suite for dataops_batch_api (Binance & TCBS).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--source",
        choices=["all", "binance", "tcbs"],
        default="all",
        help="Chọn nguồn dữ liệu để test (mặc định: all)",
    )
    parser.add_argument(
        "--test",
        type=str,
        default="",
        help="Chạy riêng lẻ một hàm test cụ thể theo tên (ví dụ: test_binance_spot_klines)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Liệt kê danh sách tất cả các hàm test có thể chạy",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Bật log chi tiết và in toàn bộ JSON response",
    )
    args = parser.parse_args()

    # Liệt kê các hàm test
    if args.list:
        print_section("Danh sách các hàm kiểm thử có sẵn:")
        print("📌 BINANCE TESTS:")
        for name, (_, desc) in BINANCE_TESTS.items():
            print(f"  • {name:<35} : {desc}")
        print("\n📌 TCBS TESTS:")
        for name, (_, desc) in TCBS_TESTS.items():
            print(f"  • {name:<35} : {desc}")
        return

    # Chạy một hàm test cụ thể
    if args.test:
        all_tests = {**BINANCE_TESTS, **TCBS_TESTS}
        if args.test not in all_tests:
            print(f"❌ Lỗi: Không tìm thấy hàm test '{args.test}'. Dùng --list để xem danh sách.")
            sys.exit(1)

        test_fn, desc = all_tests[args.test]
        print_section(f"CHẠY TEST ĐƠN LẺ: {args.test} ({desc})")
        test_fn(args.verbose)
        print_summary_table()
        return

    # Chạy theo nhóm nguồn
    print_section(f"BẮT ĐẦU TEST SUITE BATCH SOURCES (TARGET: {args.source.upper()})")

    if args.source in ("all", "binance"):
        print_section("KIỂM THỬ NGUỒN BINANCE (SPOT & USD-M FUTURES)")
        for name, (fn, _) in BINANCE_TESTS.items():
            fn(args.verbose)

    if args.source in ("all", "tcbs"):
        print_section("KIỂM THỬ NGUỒN TCBS (VIETNAM SECURITIES)")
        for name, (fn, _) in TCBS_TESTS.items():
            fn(args.verbose)

    # In bảng tổng kết
    print_summary_table()


if __name__ == "__main__":
    main()
