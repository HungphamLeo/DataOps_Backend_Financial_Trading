"""
tests/test_market_breadth_job.py — Unit tests cho Market Breadth & Foreign Flow Job (TASK-P3).
"""
from __future__ import annotations

import pytest

from dataops_processing.core.orchestrator import get_orchestrator
from dataops_processing.jobs.market_breadth_job import classify_move, compute_market_breadth

D = "2026-10-13"


def _row(symbol, exchange, close, ref, **kw):
    base = {
        "symbol": symbol, "exchange": exchange, "close": close, "ref_price": ref,
        "ceil_price": ref * 1.07, "floor_price": ref * 0.93,
        "total_vol": 100.0, "total_val": 1000.0,
        "buy_foreign_qty": 10.0, "sell_foreign_qty": 4.0, "net_foreign_qty": 6.0,
        "change_pct": 0.0,
    }
    base.update(kw)
    return base


def test_classify_move():
    assert classify_move({"close": 11, "ref_price": 10}) == "up"
    assert classify_move({"close": 9, "ref_price": 10}) == "down"
    assert classify_move({"close": 10, "ref_price": 10}) == "flat"
    assert classify_move({"close": 0, "ref_price": 0, "change_pct": -1.2}) == "down"  # fallback


def test_compute_market_breadth_groups_and_metrics():
    rows = [
        _row("VCB", "HOSE", 10.7, 10.0),                 # up + ceiling
        _row("FPT", "HOSE", 9.3, 10.0),                  # down + floor
        _row("XYZ", "HNX", 10.0, 10.0, net_foreign_qty=None),  # flat, net suy ra 10-4=6
    ]
    out = {r["group"]: r for r in compute_market_breadth(rows, D, {"VN30": ["VCB", "FPT"], "EMPTY": ["ZZZ"]})}

    assert set(out) == {"ALL", "EXCHANGE:HNX", "EXCHANGE:HOSE", "BASKET:VN30"}   # EMPTY bị bỏ
    allr = out["ALL"]
    assert (allr["advancers"], allr["decliners"], allr["unchanged"]) == (1, 1, 1)
    assert allr["ceiling_count"] == 1 and allr["floor_count"] == 1
    assert allr["advance_decline_ratio"] == 1.0
    assert allr["pct_advancing"] == pytest.approx(33.33)
    assert allr["foreign_net_qty"] == 18.0
    assert allr["foreign_net_value_est"] == pytest.approx(6 * 10.7 + 6 * 9.3 + 6 * 10.0)
    assert out["BASKET:VN30"]["symbols"] == 2
    assert out["EXCHANGE:HNX"]["unchanged"] == 1


def test_compute_market_breadth_empty_and_no_decliners():
    assert compute_market_breadth([], D) == []
    only_up = compute_market_breadth([_row("A", "HOSE", 11, 10)], D)[0]
    assert only_up["advance_decline_ratio"] == 1.0   # down == 0 → trả số advancers


def test_orchestrator_market_breadth_registration():
    orch = get_orchestrator()
    assert "market-breadth" in orch.list_available_jobs()
    job = orch.get_job("market-breadth")
    assert job.job_type == "batch"
    assert "VCB" in job.settings.market_breadth_job.baskets["VN30"]
