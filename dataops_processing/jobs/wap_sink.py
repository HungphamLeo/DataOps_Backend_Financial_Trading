"""
ST13: Write-Audit-Publish helpers for Delta Lake sinks.

A batch is written to a staging path, validated by Spark, and merged into
the final table only when every audit check passes.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WAPAuditResult:
    """ST13 audit outcome returned before publishing staging data."""

    valid: bool
    row_count: int
    reason: str = ""


def audit_vwap_batch(batch_df) -> WAPAuditResult:
    """
    ST13: Validate VWAP staging rows before final Delta publish.

    Args:
        batch_df: Spark DataFrame containing VWAP staging records.

    Returns:
        WAPAuditResult with valid=True only when all required invariants pass.

    Raises:
        No exceptions; audit failures are returned and logged by the caller.
    """
    from pyspark.sql import functions as F

    row_count = batch_df.count()

    invalid_count = (
        batch_df
        .filter(
            F.col("symbol").isNull()
            | (F.length(F.col("symbol")) == 0)
            | F.col("window_start").isNull()
            | F.col("window_end").isNull()
            | F.col("vwap").isNull()
            | (F.col("vwap") <= 0)
            | F.col("total_volume").isNull()
            | (F.col("total_volume") <= 0)
            | (F.col("window_end") <= F.col("window_start"))
        )
        .count()
    )

    if invalid_count > 0:
        return WAPAuditResult(
            valid=False,
            row_count=row_count,
            reason=f"{invalid_count} VWAP rows failed ST13 audit",
        )

    return WAPAuditResult(valid=True, row_count=row_count)


def write_audit_publish_vwap(
    spark,
    final_path: str,
    staging_path: str,
    batch_df,
    batch_id: int,
) -> WAPAuditResult:
    """
    ST13: Execute Write-Audit-Publish for one VWAP micro-batch.

    Steps:
        1. Write batch to a staging Delta path.
        2. Audit symbol, timestamps, VWAP and volume.
        3. MERGE staging rows into the final Delta table.
        4. Clear staging only after publish succeeds.

    Args:
        spark: Active SparkSession.
        final_path: Delta path for the published Silver table.
        staging_path: Temporary Delta path for one micro-batch.
        batch_df: Spark DataFrame for the closed VWAP window.
        batch_id: Micro-batch ID for audit tracing.

    Returns:
        WAPAuditResult describing the audit result.

    Raises:
        Exception: Any staging, audit or merge failure is raised so Spark
            marks the micro-batch as failed and can retry from checkpoint.
    """
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable

    batch_df.write.format("delta").mode("overwrite").save(staging_path)
    staging_df = spark.read.format("delta").load(staging_path)

    audit = audit_vwap_batch(staging_df)
    if not audit.valid:
        raise ValueError(
            f"ST13 audit rejected batch_id={batch_id}: {audit.reason}"
        )

    if DeltaTable.isDeltaTable(spark, final_path):
        final_table = DeltaTable.forPath(spark, final_path)

        (
            final_table.alias("target")
            .merge(
                staging_df.alias("source"),
                "target.symbol = source.symbol "
                "AND target.window_start = source.window_start "
                "AND target.window_end = source.window_end "
                "AND target.interval = source.interval",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        staging_df.write.format("delta").mode("overwrite").save(final_path)

    return audit