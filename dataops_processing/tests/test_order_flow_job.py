"""
tests/test_order_flow_job.py — Unit tests cho Order Flow / CVD Streaming Job (TASK-P2).

Kiểm tra phần pure-Python (không cần Spark):
    1. classify_side: chuẩn hóa buy/sell, bỏ side không xác định.
    2. compute_order_flow_metrics: delta, imbalance, zero-volume.
    3. aggregate_trades: tumbling window + CVD tích lũy theo symbol.
    4. to_line_protocol: schema InfluxDB + escape tag.
    5. Job Registry: 'order-flow' được Orchestrator load.
"""
from __future__ import annotations

import pytest

from dataops_processing.core.orchestrator import get_orchestrator
from dataops_processing.jobs.order_flow_job import (
    aggregate_trades,
    classify_side,
    compute_order_flow_metrics,
    to_line_protocol,
)


def test_classify_side():
    assert classify_side("buy") == "buy"
    assert classify_side("BUY") == "buy"
    assert classify_side("S") == "sell"
    assert classify_side("sell") == "sell"
    assert classify_side("unknown") is None
    assert classify_side(None) is None


def test_compute_order_flow_metrics():
    m = compute_order_flow_metrics(buy_volume=3.0, sell_volume=1.0)
    assert m["delta"] == 2.0
    assert m["imbalance"] == pytest.approx(0.5)

    z = compute_order_flow_metrics(0.0, 0.0)
    assert z["delta"] == 0.0 and z["imbalance"] == 0.0

    assert compute_order_flow_metrics(0.0, 5.0)["imbalance"] == -1.0


def test_aggregate_trades_windows_and_cvd():
    w = 60_000
    trades = [
        ("BTCUSDT", 2.0, "buy",  1_000),
        ("BTCUSDT", 1.0, "sell", 30_000),
        ("BTCUSDT", 4.0, "buy",  61_000),   # window #2
        ("BTCUSDT", 9.0, "weird", 62_000),  # bị bỏ qua
        ("ETHUSDT", 5.0, "sell", 1_000),
    ]
    rows = aggregate_trades(trades, w)
    btc = [r for r in rows if r["symbol"] == "BTCUSDT"]
    eth = [r for r in rows if r["symbol"] == "ETHUSDT"]

    assert len(btc) == 2
    assert btc[0]["buy_volume"] == 2.0 and btc[0]["sell_volume"] == 1.0
    assert btc[0]["delta"] == 1.0 and btc[0]["cvd"] == 1.0 and btc[0]["num_trades"] == 2
    assert btc[1]["delta"] == 4.0 and btc[1]["cvd"] == 5.0   # CVD tích lũy
    assert btc[1]["window_start_ms"] == 60_000 and btc[1]["window_end_ms"] == 120_000
    assert eth[0]["cvd"] == -5.0                              # cvd độc lập theo symbol


def test_to_line_protocol():
    line = to_line_protocol(
        measurement="order_flow", symbol="BTCUSDT", interval="1m",
        buy_volume=3.0, sell_volume=1.0, delta=2.0, imbalance=0.5,
        num_trades=4, ts_ns=1700000000000000000,
    )
    assert line == (
        "order_flow,symbol=BTCUSDT,interval=1m "
        "buy_volume=3.0,sell_volume=1.0,delta=2.0,imbalance=0.5,num_trades=4i "
        "1700000000000000000"
    )
    escaped = to_line_protocol("order_flow", "A B,C", "1m", 1, 1, 0, 0, 1, 1)
    assert "symbol=A\\ B\\,C" in escaped


def test_orchestrator_order_flow_job_registration():
    orchestrator = get_orchestrator()
    assert "order-flow" in orchestrator.list_available_jobs()
    job = orchestrator.get_job("order-flow")
    assert job.name == "order-flow"
    assert job.job_type == "streaming"
    assert job.settings.order_flow_job.influx_measurement == "order_flow"
