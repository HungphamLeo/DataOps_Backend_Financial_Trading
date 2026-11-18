from types import SimpleNamespace

import pytest

from dataops_processing.jobs.maintenance.lakehouse_compaction import (
    validate_maintenance_config,
)


def _settings(retention=168, zorder_by=None):
    return SimpleNamespace(
        maintenance_vacuum_retention_hours=retention,
        tables={"vwap_1m": "vwap_1m"},
        maintenance_tables={
            "vwap_1m": {
                "partition_by": ["symbol"],
                "zorder_by": zorder_by or ["window_end"],
            }
        },
    )


def test_retention_must_be_at_least_seven_days():
    with pytest.raises(ValueError, match=">= 168"):
        validate_maintenance_config(_settings(retention=167))


def test_table_must_exist_in_delta_tables():
    settings = _settings()
    settings.maintenance_tables = {"unknown": {"partition_by": [], "zorder_by": []}}

    with pytest.raises(ValueError, match="chưa có trong delta.tables"):
        validate_maintenance_config(settings)


def test_zorder_must_not_repeat_partition_column():
    with pytest.raises(ValueError, match="partition columns"):
        validate_maintenance_config(_settings(zorder_by=["symbol"]))