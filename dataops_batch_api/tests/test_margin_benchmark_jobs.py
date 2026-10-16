"""Unit tests cho tcbs_margin_job & benchmark_index_job (pure functions, không I/O)."""
from cli.jobs.benchmark_index_job import btc_dominance_proxy, summarize_basket
from cli.jobs.tcbs_margin_job import parse_risk_response

NOW = "2026-10-14T08:45:00+00:00"
DAY = "2026-10-14"


# ── tcbs_margin_job ──────────────────────────────────────────────────────────

def test_parse_risk_list_response():
    resp = [{
        "accountNo": "A1", "rtt": 45.5, "outstanding": 1000.0, "accruedInterest": 5,
        "dueAmount": 0, "overdueAmount": 0, "totalFeeDebt": 1.5,
        "riskPolicy": {"initialMargin": 50, "maintenanceMargin": 40, "liquidationMargin": 30},
        "riskStatus": {"code": "SAFE", "description": "ok"},
    }]
    rows = parse_risk_response(resp, "A1", DAY, NOW)
    assert len(rows) == 1
    r = rows[0]
    assert r["account_no"] == "A1"
    assert r["rtt"] == 45.5
    assert r["maintenance_margin"] == 40.0
    assert r["risk_status_code"] == "SAFE"


def test_parse_risk_dict_data_and_missing_fields():
    rows = parse_risk_response({"data": [{"rtt": "bad"}]}, "A2", DAY, NOW)
    assert rows[0]["account_no"] == "A2"      # fallback
    assert rows[0]["rtt"] is None             # invalid -> None
    assert rows[0]["initial_margin"] is None  # missing riskPolicy


def test_parse_risk_empty():
    assert parse_risk_response(None, "A", DAY, NOW) == []
    assert parse_risk_response([], "A", DAY, NOW) == []


# ── benchmark_index_job ──────────────────────────────────────────────────────

def test_summarize_basket():
    items = [
        {"totalVol": 100, "totalVal": 1000, "changePercent": 2.0, "buyForeignQty": 10, "sellForeignQty": 4},
        {"totalVol": 50,  "totalVal": 3000, "changePercent": -1.0, "buyForeignQty": 0, "sellForeignQty": 6},
        {"totalVol": 10,  "totalVal": 0,    "changePercent": 0.0},
    ]
    row = summarize_basket(items, "VN30", DAY, NOW)
    assert row["benchmark"] == "VN30"
    assert row["advancers"] == 1 and row["decliners"] == 1 and row["unchanged"] == 1
    assert row["total_vol"] == 160 and row["total_val"] == 4000
    assert row["net_foreign_qty"] == 0.0           # (10-4) + (0-6)
    assert row["change_pct"] == -0.25              # (2*1000 - 1*3000)/4000


def test_summarize_basket_empty():
    assert summarize_basket([], "VNINDEX", DAY, NOW) is None


def test_btc_dominance_proxy():
    row = btc_dominance_proxy({"BTCUSDT": 600.0, "ETHUSDT": 300.0, "SOLUSDT": 100.0}, DAY, NOW)
    assert row["benchmark"] == "BTC.D_PROXY"
    assert row["close"] == 60.0


def test_btc_dominance_proxy_no_btc():
    assert btc_dominance_proxy({"ETHUSDT": 10.0}, DAY, NOW) is None
