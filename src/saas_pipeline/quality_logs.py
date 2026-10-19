"""Registro centralizado de controles de calidad."""

from datetime import datetime, timezone

from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


QUALITY_LOG_SCHEMA = StructType([
    StructField("_run_id", StringType(), False),
    StructField("_batch_id", StringType(), False),
    StructField("tenant_id", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("check_name", StringType(), False),
    StructField("check_severity", StringType(), False),
    StructField("records_checked", LongType(), False),
    StructField("records_failed", LongType(), False),
    StructField("check_passed", BooleanType(), False),
    StructField("executed_at", TimestampType(), False),
])


def create_quality_log(
    run_id,
    batch_id,
    tenant_id,
    layer,
    table_name,
    check_name,
    severity,
    records_checked,
    records_failed,
):
    """Construye un registro de calidad con el esquema requerido."""

    if severity not in {"critical", "warning", "info"}:
        raise ValueError(f"Invalid severity: {severity}")

    if layer not in {"bronze", "silver", "gold"}:
        raise ValueError(f"Invalid layer: {layer}")

    if not 0 <= records_failed <= records_checked:
        raise ValueError("Invalid quality check counts")

    return {
        "_run_id": run_id,
        "_batch_id": batch_id,
        "tenant_id": tenant_id,
        "layer": layer,
        "table_name": table_name,
        "check_name": check_name,
        "check_severity": severity,
        "records_checked": records_checked,
        "records_failed": records_failed,
        "check_passed": records_failed == 0,
        "executed_at": datetime.now(timezone.utc),
    }


def save_quality_logs(spark, logs, path):
    """Persiste los resultados de calidad en una tabla Delta."""

    if not logs:
        return

    logs_df = spark.createDataFrame(
        logs,
        schema=QUALITY_LOG_SCHEMA,
    )

    (
        logs_df.write
        .format("delta")
        .mode("append")
        .save(path)
    )