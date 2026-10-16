"""
market_breadth_job.py — EOD Market Breadth & Foreign Flow Batch Job (TASK-P3).

Pipeline:
    Delta Bronze  tcbs_market_eod  (1 row / symbol / trading_date — từ dataops_batch_api)
        → collect rows của 1 trading_date
        → compute_market_breadth() theo group: ALL, từng exchange, từng basket (VN30 ... từ config)
        → Delta Gold  market_breadth  (idempotent: replaceWhere trading_date)

Chỉ số mỗi (trading_date, group):
    advancers / decliners / unchanged, advance_decline_ratio, pct_advancing,
    ceiling_count / floor_count, total_volume, total_value,
    foreign_buy_qty / foreign_sell_qty / foreign_net_qty,
    foreign_net_value_est  (= Σ net_foreign_qty × close — ƯỚC TÍNH vì Bronze chỉ có KL, không có GT NN)

Giới hạn đã biết:
    - Bronze không có trường ngành ICB → group theo exchange + baskets khai báo trong config.
    - Funding-rate heatmap Crypto cần Bronze funding (chưa có job ingest ở batch_api) → chưa thuộc job này.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

from dataops_processing.core.base_job import BaseProcessingJob
from dataops_processing.core.registry import JobRegistry
from dataops_processing.log.logger_setup import logger_manager

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logger_manager.get_logger(__name__)


# =============================================================================
# Pure-Python core (testable độc lập, không cần Spark)
# =============================================================================

def _f(row: Dict[str, Any], key: str) -> float:
    v = row.get(key)
    return float(v) if v is not None else 0.0


def classify_move(row: Dict[str, Any]) -> str:
    """'up' | 'down' | 'flat' — ưu tiên close vs ref_price; fallback change_pct."""
    close, ref = _f(row, "close"), _f(row, "ref_price")
    if close > 0 and ref > 0:
        diff = close - ref
    else:
        diff = _f(row, "change_pct")
    if diff > 0:
        return "up"
    if diff < 0:
        return "down"
    return "flat"


def _aggregate(group: str, trading_date: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    up = down = flat = ceiling = floor = 0
    vol = val = buy_f = sell_f = net_f = net_val = 0.0
    for r in rows:
        move = classify_move(r)
        up += move == "up"
        down += move == "down"
        flat += move == "flat"

        close = _f(r, "close")
        if close > 0 and _f(r, "ceil_price") > 0 and close >= _f(r, "ceil_price") * 0.9999:
            ceiling += 1
        if close > 0 and _f(r, "floor_price") > 0 and close <= _f(r, "floor_price") * 1.0001:
            floor += 1

        vol += _f(r, "total_vol")
        val += _f(r, "total_val")
        buy_f += _f(r, "buy_foreign_qty")
        sell_f += _f(r, "sell_foreign_qty")
        net_qty = r.get("net_foreign_qty")
        net_qty = float(net_qty) if net_qty is not None else _f(r, "buy_foreign_qty") - _f(r, "sell_foreign_qty")
        net_f += net_qty
        net_val += net_qty * close

    n = len(rows)
    return {
        "trading_date": trading_date,
        "group": group,
        "symbols": n,
        "advancers": int(up),
        "decliners": int(down),
        "unchanged": int(flat),
        "advance_decline_ratio": round(up / down, 4) if down > 0 else float(up),
        "pct_advancing": round(up / n * 100.0, 2) if n else 0.0,
        "ceiling_count": int(ceiling),
        "floor_count": int(floor),
        "total_volume": vol,
        "total_value": val,
        "foreign_buy_qty": buy_f,
        "foreign_sell_qty": sell_f,
        "foreign_net_qty": net_f,
        "foreign_net_value_est": round(net_val, 2),
    }


def compute_market_breadth(
    rows: Iterable[Dict[str, Any]],
    trading_date: str,
    baskets: Optional[Dict[str, List[str]]] = None,
) -> List[Dict[str, Any]]:
    """
    Tính breadth cho 1 trading_date.

    Groups: "ALL", "EXCHANGE:<ex>" cho từng exchange có dữ liệu, "BASKET:<name>" cho từng basket.
    Basket không có symbol nào khớp dữ liệu thì bị bỏ qua.
    """
    rows = [r for r in rows if r.get("symbol")]
    if not rows:
        return []

    out = [_aggregate("ALL", trading_date, rows)]

    by_exchange: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        ex = str(r.get("exchange") or "").upper()
        if ex:
            by_exchange.setdefault(ex, []).append(r)
    for ex in sorted(by_exchange):
        out.append(_aggregate(f"EXCHANGE:{ex}", trading_date, by_exchange[ex]))

    for name, members in (baskets or {}).items():
        member_set = {m.upper() for m in members}
        subset = [r for r in rows if str(r["symbol"]).upper() in member_set]
        if subset:
            out.append(_aggregate(f"BASKET:{name}", trading_date, subset))

    return out


# =============================================================================
# Concrete Strategy: Market Breadth Batch Job
# =============================================================================

@JobRegistry.register("market-breadth")
class MarketBreadthBatchJob(BaseProcessingJob):
    """Job batch EOD tổng hợp market breadth & dòng tiền khối ngoại vào Delta Gold."""

    @property
    def name(self) -> str:
        return "market-breadth"

    @property
    def job_type(self) -> str:
        return "batch"

    def execute(
        self,
        spark: Optional["SparkSession"] = None,
        trading_date: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Args:
            spark: SparkSession tùy chọn.
            trading_date: "YYYY-MM-DD". None = ngày mới nhất có trong Bronze.
        """
        try:
            from pyspark.sql import functions as F
            from pyspark.sql.types import (
                DoubleType, LongType, StringType, StructField, StructType,
            )
        except ImportError as exc:
            logger.error("PySpark not installed: %s", exc, exc_info=True)
            return {"written": 0, "trading_date": trading_date, "errors": 1}

        cfg = self.settings.market_breadth_job
        created_spark_internally = False
        if spark is None:
            from dataops_processing.config.spark_config import build_spark_session
            spark = build_spark_session("dataops-market-breadth-job")
            created_spark_internally = True

        try:
            bronze = spark.read.format("delta").load(self.paths.delta_bronze_path(cfg.source_table))
            if trading_date is None:
                trading_date = bronze.agg(F.max("trading_date")).collect()[0][0]
            if not trading_date:
                logger.warning("market_breadth_job: bronze %s is empty", cfg.source_table)
                return {"written": 0, "trading_date": None, "errors": 0}

            rows = [r.asDict() for r in bronze.filter(F.col("trading_date") == trading_date).collect()]
            records = compute_market_breadth(rows, trading_date, cfg.baskets)
            if not records:
                logger.warning("market_breadth_job: no rows for trading_date=%s", trading_date)
                return {"written": 0, "trading_date": trading_date, "errors": 0}

            now_iso = datetime.now(timezone.utc).isoformat()
            for rec in records:
                rec["computed_at"] = now_iso

            schema = StructType([
                StructField("trading_date",           StringType(), False),
                StructField("group",                  StringType(), False),
                StructField("symbols",                LongType(),   False),
                StructField("advancers",              LongType(),   False),
                StructField("decliners",              LongType(),   False),
                StructField("unchanged",              LongType(),   False),
                StructField("advance_decline_ratio",  DoubleType(), False),
                StructField("pct_advancing",          DoubleType(), False),
                StructField("ceiling_count",          LongType(),   False),
                StructField("floor_count",            LongType(),   False),
                StructField("total_volume",           DoubleType(), False),
                StructField("total_value",            DoubleType(), False),
                StructField("foreign_buy_qty",        DoubleType(), False),
                StructField("foreign_sell_qty",       DoubleType(), False),
                StructField("foreign_net_qty",        DoubleType(), False),
                StructField("foreign_net_value_est",  DoubleType(), False),
                StructField("computed_at",            StringType(), False),
            ])
            gold_path = self.paths.delta_gold_path(cfg.gold_table)
            (
                spark.createDataFrame(records, schema=schema)
                .write.format("delta")
                .mode("overwrite")
                .option("replaceWhere", f"trading_date = '{trading_date}'")   # idempotent theo ngày
                .partitionBy("trading_date")
                .save(gold_path)
            )
            logger.info("market_breadth_job: %s → %d group rows written to %s", trading_date, len(records), gold_path)
            return {"written": len(records), "trading_date": trading_date, "errors": 0}
        except Exception as exc:
            logger.error("market_breadth_job failed — %s", exc, exc_info=True)
            return {"written": 0, "trading_date": trading_date, "errors": 1}
        finally:
            if created_spark_internally:
                spark.stop()
