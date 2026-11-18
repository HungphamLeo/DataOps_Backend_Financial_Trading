"""
Lakehouse maintenance cho Delta Silver.

OPTIMIZE gom file Parquet nhỏ và tùy chọn sắp xếp dữ liệu theo Z-ORDER để
giảm I/O khi query. VACUUM xóa file vật lý cũ đã được Delta đánh dấu là
không còn thuộc snapshot hiện hành.

VACUUM được bật bằng flag riêng, không chạy mặc định. Giữ retention tối thiểu
168 giờ để các reader hoặc job chạy lâu không tham chiếu tới file đã bị xóa.
Job này chỉ tác động tới các bảng được khai báo rõ trong processing_config.yaml.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from dataops_processing.config.paths import get_path_resolver
from dataops_processing.config.settings import get_settings
from dataops_processing.log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)
MIN_VACUUM_RETENTION_HOURS = 168


def validate_maintenance_config(delta_settings: Any) -> None:
    """
    Fail fast nếu cấu hình có thể làm VACUUM quá sớm hoặc Z-ORDER vô ích.

    Delta không nên xóa file trong khi một query/job còn có thể cần snapshot
    cũ. Đồng thời, Z-ORDER theo partition column thường không đem lại lợi ích:
    partition pruning đã loại dữ liệu theo cột đó trước khi đọc file.
    """
    if delta_settings.maintenance_vacuum_retention_hours < MIN_VACUUM_RETENTION_HOURS:
        raise ValueError(
            "VACUUM retention phải >= 168 giờ; không tắt Delta retention safety check."
        )

    for table_key, policy in delta_settings.maintenance_tables.items():
        if table_key not in delta_settings.tables:
            raise ValueError(f"Bảng maintenance '{table_key}' chưa có trong delta.tables.")

        partition_columns = set(policy.get("partition_by", []))
        zorder_columns = set(policy.get("zorder_by", []))
        overlap = partition_columns & zorder_columns
        if overlap:
            raise ValueError(
                f"{table_key}: không Z-ORDER theo partition columns: {sorted(overlap)}"
            )


def compact_tables(
    spark: Any,
    table_keys: list[str],
    *,
    run_vacuum: bool = False,
) -> dict[str, Any]:
    """
    Chạy OPTIMIZE cho các bảng được chọn và VACUUM khi được yêu cầu.

    Đường dẫn được lấy từ StoragePathResolver để tuân theo bucket/base path
    cấu hình. Bảng không phải Delta hoặc cấu hình sai được ghi nhận thành lỗi
    thay vì bị bỏ qua âm thầm. Mỗi bảng được xử lý độc lập để một lỗi không
    ngăn các bảng còn lại được bảo trì.
    """
    from delta.tables import DeltaTable

    settings = get_settings()
    delta_cfg = settings.delta
    paths = get_path_resolver()
    validate_maintenance_config(delta_cfg)

    spark.conf.set(
        "spark.databricks.delta.optimize.maxFileSize",
        str(delta_cfg.maintenance_target_file_size_bytes),
    )

    result: dict[str, Any] = {"optimized": [], "vacuumed": [], "skipped": [], "errors": []}

    for table_key in table_keys:
        policy = delta_cfg.maintenance_tables.get(table_key)
        if policy is None:
            result["errors"].append(f"{table_key}: không có maintenance policy")
            continue

        table_path = paths.delta_silver_path(table_key)
        try:
            if not DeltaTable.isDeltaTable(spark, table_path):
                result["skipped"].append(f"{table_key}: chưa tồn tại hoặc không phải Delta")
                continue

            actual_columns = set(spark.read.format("delta").load(table_path).columns)
            zorder_columns = policy.get("zorder_by", [])
            missing_columns = set(zorder_columns) - actual_columns
            if missing_columns:
                raise ValueError(f"Z-ORDER columns không có trong bảng: {sorted(missing_columns)}")

            delta_table = DeltaTable.forPath(spark, table_path)
            optimize = delta_table.optimize()
            if zorder_columns:
                optimize.executeZOrderBy(*zorder_columns)
            else:
                optimize.executeCompaction()
            result["optimized"].append(table_key)

            if run_vacuum:
                delta_table.vacuum(
                    retentionHours=delta_cfg.maintenance_vacuum_retention_hours
                )
                result["vacuumed"].append(table_key)

        except Exception as exc:
            logger.exception("Lakehouse maintenance thất bại cho %s", table_key)
            result["errors"].append(f"{table_key}: {exc}")

    return result


def main(argv: list[str] | None = None) -> int:
    """
    CLI entry point để chạy thủ công hoặc từ scheduler.

    `--vacuum` là explicit opt-in vì đây là bước xóa file vật lý, khác với
    OPTIMIZE. `--dry-run` chỉ in kế hoạch, không mở Spark và không ghi dữ liệu.
    """
    settings = get_settings()
    configured_tables = settings.delta.maintenance_tables

    parser = argparse.ArgumentParser(description="Delta Lake OPTIMIZE / VACUUM")
    parser.add_argument("--table", action="append", help="Table key; có thể truyền nhiều lần")
    parser.add_argument("--all", action="store_true", help="Chạy tất cả bảng đã cấu hình")
    parser.add_argument("--vacuum", action="store_true", help="Bật VACUUM sau OPTIMIZE")
    parser.add_argument("--dry-run", action="store_true", help="Chỉ in kế hoạch thực thi")
    args = parser.parse_args(argv)

    if args.all == bool(args.table):
        parser.error("Chọn đúng một trong --all hoặc --table.")

    table_keys = list(configured_tables) if args.all else args.table
    unknown = sorted(set(table_keys) - set(configured_tables))
    if unknown:
        parser.error(f"Bảng chưa được cấu hình maintenance: {unknown}")

    try:
        validate_maintenance_config(settings.delta)
    except ValueError as exc:
        parser.error(str(exc))

    if args.dry_run:
        print(json.dumps({
            "tables": table_keys,
            "vacuum": args.vacuum,
            "retention_hours": settings.delta.maintenance_vacuum_retention_hours,
            "target_file_size_bytes": settings.delta.maintenance_target_file_size_bytes,
        }, indent=2))
        return 0

    from dataops_processing.config.spark_config import build_spark_session

    spark = build_spark_session("dataops-lakehouse-maintenance")
    try:
        result = compact_tables(spark, table_keys, run_vacuum=args.vacuum)
        print(json.dumps(result, indent=2, default=str))
        return 1 if result["errors"] else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())