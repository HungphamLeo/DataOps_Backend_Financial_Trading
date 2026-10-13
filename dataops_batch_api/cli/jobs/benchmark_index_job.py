"""
benchmark_index_job.py — [Daily 15:50 ICT] Benchmark ingestion (VNINDEX, VN30, BTC.D) → Delta Bronze.

Schedule: cron "50 8 * * 1-5" (UTC = 15:50 ICT, T2-T6) — sau tcbs_eod_job.

GIỚI HẠN NGUỒN DỮ LIỆU (xác minh với openapi-v1.0.0.json & Binance REST):
    - TCBS OpenAPI KHÔNG có endpoint giá trị chỉ số (index level). Chỉ có
      /tartarus/v1/tickerCommons?index=<basket>. Vì vậy VNINDEX / VN30 được
      lưu dưới dạng *basket aggregate* (breadth, thanh khoản, NN ròng,
      %thay đổi bình quân gia quyền theo giá trị GD) — KHÔNG phải điểm chỉ số.
        VNINDEX → basket HOSE ; VN30 → basket VN30
    - Binance KHÔNG có BTC dominance. BTC.D được xấp xỉ bằng tỷ trọng
      quote-volume 1d của BTCUSDT trên tổng các spot symbols cấu hình
      (benchmark = "BTC.D_PROXY", cột close = % tỷ trọng).

Delta table: s3://binance-datalake/delta/bronze/benchmark_index_daily
Partition:   trading_date
Retention:   append-only

Schema:
    trading_date     string   "YYYY-MM-DD"
    benchmark        string   "VNINDEX" | "VN30" | "BTC.D_PROXY"
    source           string   "tcbs_basket" | "binance_vol_share"
    close            double   BTC.D_PROXY: % dominance; TCBS: null (không có index level)
    change_pct       double   TCBS: value-weighted % change ; BTC.D_PROXY: null
    total_vol        double
    total_val        double
    advancers        int64
    decliners        int64
    unchanged        int64
    net_foreign_qty  double
    ingested_at      string
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pyarrow as pa

from cli.ingestion.sources.client.binance_client import BinanceSpotClient
from cli.ingestion.sources.client.tcbs_client import TCBSRestClient
from cli.jobs._delta_writer import _table_path, write_records
from cli.jobs.metrics import push_metrics, record_ingest
from cli.flows.loader.load_config import load_exchange_config
from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

_SCHEMA = pa.schema([
    pa.field("trading_date",    pa.string(),  nullable=False),
    pa.field("benchmark",       pa.string(),  nullable=False),
    pa.field("source",          pa.string(),  nullable=False),
    pa.field("close",           pa.float64(), nullable=True),
    pa.field("change_pct",      pa.float64(), nullable=True),
    pa.field("total_vol",       pa.float64(), nullable=True),
    pa.field("total_val",       pa.float64(), nullable=True),
    pa.field("advancers",       pa.int64(),   nullable=True),
    pa.field("decliners",       pa.int64(),   nullable=True),
    pa.field("unchanged",       pa.int64(),   nullable=True),
    pa.field("net_foreign_qty", pa.float64(), nullable=True),
    pa.field("ingested_at",     pa.string(),  nullable=False),
])

# benchmark name → TCBS basket index
_DEFAULT_TCBS_BENCHMARKS: Dict[str, str] = {"VNINDEX": "HOSE", "VN30": "VN30"}
_MS_PER_DAY = 86_400_000


def _num(value: Any) -> float:
    try:
        return float(value) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def summarize_basket(
    items: List[dict], benchmark: str, trading_date: str, now_iso: str
) -> Optional[Dict[str, Any]]:
    """
    Tổng hợp tickerCommons items của 1 basket → 1 row benchmark.
    Pure function (không I/O).
    """
    if not items:
        return None

    total_vol = total_val = net_foreign = 0.0
    weighted_chg = 0.0
    adv = dec = unch = 0
    for it in items:
        val = _num(it.get("totalVal"))
        chg = _num(it.get("changePercent"))
        total_vol += _num(it.get("totalVol"))
        total_val += val
        weighted_chg += chg * val
        net_foreign += _num(it.get("buyForeignQty")) - _num(it.get("sellForeignQty"))
        if chg > 0:
            adv += 1
        elif chg < 0:
            dec += 1
        else:
            unch += 1

    return {
        "trading_date":    trading_date,
        "benchmark":       benchmark,
        "source":          "tcbs_basket",
        "close":           None,
        "change_pct":      round(weighted_chg / total_val, 4) if total_val > 0 else None,
        "total_vol":       total_vol,
        "total_val":       total_val,
        "advancers":       adv,
        "decliners":       dec,
        "unchanged":       unch,
        "net_foreign_qty": round(net_foreign, 0),
        "ingested_at":     now_iso,
    }


def btc_dominance_proxy(
    quote_volumes: Dict[str, float], trading_date: str, now_iso: str, btc_symbol: str = "BTCUSDT"
) -> Optional[Dict[str, Any]]:
    """
    BTC.D_PROXY = quote-volume BTC / tổng quote-volume các symbols * 100.
    Pure function (không I/O).
    """
    total = sum(v for v in quote_volumes.values() if v > 0)
    btc = quote_volumes.get(btc_symbol, 0.0)
    if total <= 0 or btc <= 0:
        return None
    return {
        "trading_date":    trading_date,
        "benchmark":       "BTC.D_PROXY",
        "source":          "binance_vol_share",
        "close":           round(btc / total * 100.0, 4),
        "change_pct":      None,
        "total_vol":       None,
        "total_val":       total,
        "advancers":       None,
        "decliners":       None,
        "unchanged":       None,
        "net_foreign_qty": None,
        "ingested_at":     now_iso,
    }


def _extract_items(resp: Any) -> List[dict]:
    if isinstance(resp, dict):
        data = resp.get("data", [])
    elif isinstance(resp, list):
        data = resp
    else:
        data = []
    return [d for d in data if isinstance(d, dict)]


def run() -> dict:
    """Chạy job benchmark ingestion. Returns {"written": int, "errors": int}."""
    cfg     = load_exchange_config()
    bucket  = cfg["delta"]["bucket"]
    path    = _table_path(bucket, "bronze", "benchmark_index_daily")
    now     = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    today   = now.strftime("%Y-%m-%d")

    benchmarks = cfg["tcbs"].get("benchmark_baskets") or _DEFAULT_TCBS_BENCHMARKS
    records: List[Dict[str, Any]] = []
    errors = 0

    # ── TCBS baskets (VNINDEX, VN30) ─────────────────────────────────────────
    try:
        client = TCBSRestClient.from_env()
        client.authenticate()
        for name, index in benchmarks.items():
            try:
                items = _extract_items(client.get_ticker_commons(index=index))
                row = summarize_basket(items, name, today, now_iso)
                if row:
                    records.append(row)
                else:
                    logger.warning("benchmark_index_job: empty basket %s (%s)", name, index)
            except Exception as exc:
                logger.error("benchmark_index_job: %s failed: %s", name, exc)
                errors += 1
    except Exception as exc:
        logger.error("benchmark_index_job: TCBS auth failed: %s", exc)
        errors += 1

    # ── Binance BTC dominance proxy (nến 1d hôm qua đã đóng) ─────────────────
    try:
        spot = BinanceSpotClient.from_env()
        end_ms   = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        start_ms = end_ms - _MS_PER_DAY
        vols: Dict[str, float] = {}
        for symbol in cfg["binance"]["instruments"]["spot"]:
            rows = spot.get_klines(symbol, interval="1d", start_time=start_ms, end_time=end_ms, limit=2)
            if rows:
                vols[symbol] = float(rows[-1][7])  # row[7] = quoteAssetVolume
        row = btc_dominance_proxy(
            vols, (now - timedelta(days=1)).strftime("%Y-%m-%d"), now_iso
        )
        if row:
            records.append(row)
        else:
            logger.warning("benchmark_index_job: BTC.D proxy unavailable (vols=%s)", vols)
    except Exception as exc:
        logger.error("benchmark_index_job: Binance BTC.D failed: %s", exc)
        errors += 1

    written = 0
    if records:
        written = write_records(path, records, _SCHEMA, mode="append", partition_by=["trading_date"])

    record_ingest("benchmark_index_job", count=written, status="success")
    if errors:
        record_ingest("benchmark_index_job", count=errors, status="error")
    push_metrics("benchmark_index_job")

    logger.info("benchmark_index_job: DONE written=%d errors=%d", written, errors)
    return {"written": written, "errors": errors}


if __name__ == "__main__":
    from pathlib import Path
    try:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).resolve().parents[4] / ".env", override=False)
    except ImportError:
        pass
    print(run())
