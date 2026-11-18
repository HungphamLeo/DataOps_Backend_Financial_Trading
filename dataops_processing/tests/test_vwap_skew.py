"""ST12: kiểm tra gộp salt partials bảo toàn phép tính VWAP."""

import pytest


@pytest.fixture(scope="module")
def spark():
    """Tạo Spark local session cho test phép gộp ST12."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-st12-vwap-skew")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_st12_merges_partial_sums_not_partial_vwap(spark):
    """VWAP cuối phải là tổng notional / tổng volume qua mọi salt."""
    from dataops_processing.jobs.vwap_job import _merge_vwap_salt_partials

    partials = spark.createDataFrame(
        [
            ("BTCUSDT", "2026-01-01 00:00:00", "2026-01-01 00:05:00", 100.0, 1.0, 1),
            ("BTCUSDT", "2026-01-01 00:00:00", "2026-01-01 00:05:00", 900.0, 3.0, 1),
        ],
        schema=(
            "symbol string, window_start timestamp, window_end timestamp, "
            "partial_notional double, partial_volume double, partial_trades long"
        ),
    )

    result = _merge_vwap_salt_partials(partials, interval="5m").collect()

    assert len(result) == 1
    assert result[0]["vwap"] == pytest.approx(250.0)
    assert result[0]["total_volume"] == pytest.approx(4.0)
    assert result[0]["num_trades"] == 2