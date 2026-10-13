"""
tests/test_indicator_matrix_job.py — Unit tests cho Multi-Timeframe Indicator Matrix Job.

Kiểm tra:
    1. EMA Calculation: tính chính xác hệ số làm mượt, chuỗi output khớp độ dài.
    2. ATR Calculation: tính đúng True Range và Wilder's smoothing.
    3. RSI Calculation: giá trị luôn nằm trong khoảng [0, 100], monotonic uptrend/downtrend.
    4. Bollinger Bands Calculation: Upper >= Middle >= Lower, %B và Bandwidth.
    5. compute_indicator_matrix: tích hợp toàn bộ indicators vào records.
    6. Job Registry & Orchestrator Discovery: 'indicator-matrix' được load và khởi tạo đúng qua Facade.
"""
from __future__ import annotations

import math
import pytest

from dataops_processing.core.orchestrator import get_orchestrator
from dataops_processing.core.registry import JobRegistry
from dataops_processing.jobs.indicator_matrix_job import (
    calculate_atr,
    calculate_bollinger_bands,
    calculate_ema,
    calculate_rsi,
    compute_indicator_matrix,
    IndicatorMatrixBatchJob,
)


# ── Test 1: EMA Calculation ───────────────────────────────────────────────────

def test_calculate_ema_basic():
    prices = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0]
    ema_5 = calculate_ema(prices, period=5)

    assert len(ema_5) == len(prices)
    # EMA của chuỗi tăng đơn điệu phải tăng đơn điệu
    for i in range(1, len(ema_5)):
        assert ema_5[i] >= ema_5[i - 1]

    # Kiểm tra chuỗi rỗng
    assert calculate_ema([], 5) == []


def test_calculate_ema_constant_price():
    prices = [100.0] * 20
    ema_10 = calculate_ema(prices, period=10)
    assert len(ema_10) == 20
    for val in ema_10:
        assert math.isclose(val, 100.0, rel_tol=1e-5)


# ── Test 2: ATR Calculation ───────────────────────────────────────────────────

def test_calculate_atr_basic():
    highs = [12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0]
    lows =  [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0]
    closes =[11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0]

    atr_14 = calculate_atr(highs, lows, closes, period=14)
    assert len(atr_14) == len(closes)
    # Vì mỗi candle range đều là 2.0 (high-low=2, diff=1), ATR phải hội tụ về 2.0
    assert math.isclose(atr_14[-1], 2.0, rel_tol=1e-3)


# ── Test 3: RSI Calculation ───────────────────────────────────────────────────

def test_calculate_rsi_uptrend_and_downtrend():
    # Uptrend liên tục -> RSI phải tiến tới 100
    uptrend_closes = [float(i) for i in range(1, 30)]
    rsi_up = calculate_rsi(uptrend_closes, period=14)
    assert len(rsi_up) == len(uptrend_closes)
    assert rsi_up[-1] == 100.0

    # Downtrend liên tục -> RSI phải tiến tới 0
    downtrend_closes = [float(100 - i) for i in range(1, 30)]
    rsi_down = calculate_rsi(downtrend_closes, period=14)
    assert len(rsi_down) == len(downtrend_closes)
    assert rsi_down[-1] == 0.0


def test_calculate_rsi_bounds():
    # Giá dao động ngẫu nhiên -> RSI luôn nằm trong [0, 100]
    prices = [100.0, 102.0, 101.0, 105.0, 103.0, 108.0, 104.0, 102.0, 107.0, 110.0, 106.0, 108.0, 112.0, 109.0, 111.0, 115.0]
    rsi = calculate_rsi(prices, period=14)
    for val in rsi:
        assert 0.0 <= val <= 100.0


# ── Test 4: Bollinger Bands Calculation ────────────────────────────────────────

def test_calculate_bollinger_bands():
    prices = [100.0, 102.0, 101.0, 103.0, 105.0, 104.0, 106.0, 108.0, 107.0, 109.0,
              111.0, 110.0, 112.0, 114.0, 113.0, 115.0, 117.0, 116.0, 118.0, 120.0,
              122.0, 121.0, 123.0, 125.0]

    upper, middle, lower, bw, pct_b = calculate_bollinger_bands(prices, period=20, num_std=2.0)
    assert len(upper) == len(prices)
    assert len(middle) == len(prices)
    assert len(lower) == len(prices)

    for i in range(len(prices)):
        assert upper[i] >= middle[i]
        assert middle[i] >= lower[i]
        assert bw[i] >= 0.0


# ── Test 5: compute_indicator_matrix ───────────────────────────────────────────

def test_compute_indicator_matrix_integration():
    sample_rows = []
    base_ts = 1700000000000
    for i in range(30):
        sample_rows.append({
            "symbol": "BTCUSDT",
            "interval": "1d",
            "ts_open_ms": base_ts + i * 86400000,
            "open": 30000.0 + i * 100.0,
            "high": 30200.0 + i * 100.0,
            "low": 29900.0 + i * 100.0,
            "close": 30150.0 + i * 100.0,
            "volume": 1500.0 + i * 10.0,
        })

    records = compute_indicator_matrix(
        sample_rows,
        ema_periods=[20, 50, 200],
        rsi_period=14,
        atr_period=14,
        bb_period=20,
        bb_std=2.0,
    )

    assert len(records) == 30
    first = records[0]
    last = records[-1]

    # Kiểm tra đầy đủ các trường schema
    for col in ["symbol", "interval", "timestamp", "open", "high", "low", "close", "volume",
                "ema_20", "ema_50", "ema_200", "rsi_14", "atr_14",
                "bb_upper_20", "bb_middle_20", "bb_lower_20", "bb_bandwidth", "bb_pct_b"]:
        assert col in first
        assert col in last

    assert last["rsi_14"] > 50.0
    assert last["ema_20"] > 30000.0


# ── Test 6: Job Registry & Orchestrator Discovery ─────────────────────────────

def test_orchestrator_indicator_matrix_job_registration():
    orchestrator = get_orchestrator()
    registered_jobs = orchestrator.list_available_jobs()

    assert "indicator-matrix" in registered_jobs
    job = orchestrator.get_job("indicator-matrix")
    assert isinstance(job, IndicatorMatrixBatchJob)
    assert job.name == "indicator-matrix"
    assert job.job_type == "batch"
