"""
test_volume_bars_job.py — Unit tests for assign_volume_bars() in volume_bars_job.py.

Tests chỉ test pure Python algorithm (không cần PySpark/Delta).
"""
import pytest
from dataops_processing.jobs.volume_bars_job import assign_volume_bars


def _make_row(ts_open: int, ts_close: int, open_: float, high: float,
              low: float, close: float, volume: float) -> dict:
    return {
        "ts_open_ms":  ts_open,
        "ts_close_ms": ts_close,
        "open":        open_,
        "high":        high,
        "low":         low,
        "close":       close,
        "volume":      volume,
    }


# ---------------------------------------------------------------------------
# Test 1: empty input → empty output
# ---------------------------------------------------------------------------
def test_assign_volume_bars_empty():
    assert assign_volume_bars([], threshold=100.0) == []


# ---------------------------------------------------------------------------
# Test 2: threshold được tạo đúng khi cumsum vượt ngưỡng
# ---------------------------------------------------------------------------
def test_assign_volume_bars_threshold():
    """
    3 rows, mỗi row volume=200. Threshold=500.
    Cumsum: 200 → 400 → 600 (>= 500) → close bar 0.
    Partial bar còn lại: 0 (cumsum reset về 0 sau khi đóng bar).
    Kết quả: 1 full bar (total_volume=600, num_candles=3).
    """
    rows = [
        _make_row(1000, 1999, 100.0, 110.0, 95.0,  105.0, 200.0),
        _make_row(2000, 2999, 105.0, 115.0, 100.0, 110.0, 200.0),
        _make_row(3000, 3999, 110.0, 120.0, 108.0, 118.0, 200.0),
    ]
    bars = assign_volume_bars(rows, threshold=500.0)

    assert len(bars) == 1
    bar = bars[0]
    assert bar["bar_index"]    == 0
    assert bar["open"]         == 100.0
    assert bar["high"]         == 120.0
    assert bar["low"]          == 95.0
    assert bar["close"]        == 118.0
    assert bar["total_volume"] == 600.0
    assert bar["num_candles"]  == 3
    assert bar["bar_open_ts"]  == 1000
    assert bar["bar_close_ts"] == 3999


# ---------------------------------------------------------------------------
# Test 3: partial bar cuối được emit
# ---------------------------------------------------------------------------
def test_assign_volume_bars_partial_bar():
    """
    5 rows volume=300 mỗi row. Threshold=700.
    Row 0: cum=300
    Row 1: cum=600
    Row 2: cum=900 >= 700 → close bar 0 (total_volume=900)
    Row 3: cum=300 (reset)
    Row 4: cum=600 (< 700, chưa đóng bar)
    → partial bar 1 được emit với total_volume=600, num_candles=2.
    """
    rows = [
        _make_row(1000, 1999, 10.0, 12.0, 9.0,  11.0, 300.0),
        _make_row(2000, 2999, 11.0, 13.0, 10.0, 12.0, 300.0),
        _make_row(3000, 3999, 12.0, 14.0, 11.0, 13.0, 300.0),
        _make_row(4000, 4999, 13.0, 15.0, 12.0, 14.0, 300.0),
        _make_row(5000, 5999, 14.0, 16.0, 13.0, 15.0, 300.0),
    ]
    bars = assign_volume_bars(rows, threshold=700.0)

    assert len(bars) == 2

    # Bar 0 — full bar
    b0 = bars[0]
    assert b0["bar_index"]    == 0
    assert b0["total_volume"] == 900.0
    assert b0["num_candles"]  == 3
    assert b0["bar_open_ts"]  == 1000
    assert b0["bar_close_ts"] == 3999

    # Bar 1 — partial bar
    b1 = bars[1]
    assert b1["bar_index"]    == 1
    assert b1["total_volume"] == 600.0
    assert b1["num_candles"]  == 2
    assert b1["bar_open_ts"]  == 4000
    assert b1["bar_close_ts"] == 5999


# ---------------------------------------------------------------------------
# Test 4: nhiều full bars — verify bar_index tăng đúng
# ---------------------------------------------------------------------------
def test_assign_volume_bars_multiple_full_bars():
    """
    6 rows volume=100 mỗi row. Threshold=200.
    Mỗi 2 rows đóng 1 bar → 3 full bars.
    """
    rows = [
        _make_row(i * 1000, i * 1000 + 999, float(i), float(i + 1), float(i - 1), float(i), 100.0)
        for i in range(1, 7)
    ]
    bars = assign_volume_bars(rows, threshold=200.0)

    assert len(bars) == 3
    for idx, bar in enumerate(bars):
        assert bar["bar_index"]   == idx
        assert bar["num_candles"] == 2
        assert bar["total_volume"] == 200.0


# ---------------------------------------------------------------------------
# Test 5: single row volume >= threshold → 1 bar (no partial)
# ---------------------------------------------------------------------------
def test_assign_volume_bars_single_row_exact():
    rows = [_make_row(0, 999, 50.0, 55.0, 48.0, 52.0, 1000.0)]
    bars = assign_volume_bars(rows, threshold=500.0)

    assert len(bars) == 1
    assert bars[0]["total_volume"] == 1000.0
    assert bars[0]["num_candles"]  == 1


# ---------------------------------------------------------------------------
# Test 6: verify O(n) không crash với n lớn (regression test cho L27)
# ---------------------------------------------------------------------------
def test_assign_volume_bars_large_input_performance():
    """
    8760 rows (hourly data 1 năm). Nếu O(n²) thì ~76M ops → slow/timeout.
    Với O(n) (enumerate) thì ~8760 ops → fast.
    Threshold nhỏ để tạo nhiều bars.
    """
    n = 8760
    rows = [
        _make_row(i * 3600_000, i * 3600_000 + 3599_999,
                  float(i), float(i + 1), float(i - 1), float(i), 10.0)
        for i in range(1, n + 1)
    ]
    bars = assign_volume_bars(rows, threshold=50.0)

    # Mỗi 5 rows đóng 1 bar (vol=10 * 5 = 50 >= 50)
    assert len(bars) >= n // 5 - 1  # tolerance cho partial bars
    # Tất cả bar_index tăng dần
    for idx, bar in enumerate(bars):
        assert bar["bar_index"] == idx
