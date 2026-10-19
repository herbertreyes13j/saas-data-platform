"""Controles de calidad para la capa Silver."""

from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

from saas_pipeline.quality_logs import create_quality_log
from saas_pipeline.silver import (
    MERGE_KEY,
    deduplicate_deliveries,
    filter_delivery_types,
)


REQUIRED_FIELDS = [
    "_tenant_id",
    "fecha_proceso",
    "transporte",
    "ruta",
    "tipo_entrega",
    "material",
    "precio",
    "cantidad",
    "unidad",
    "cantidad_st",
]


def evaluate_silver_quality(fact_df, run_id, batch_id, tenant):
    """Evalúa controles críticos de integridad en Silver."""

    # 1. Detectar claves de negocio duplicadas.
    key_window = Window.partitionBy(*MERGE_KEY)

    checked_df = fact_df.withColumn(
        "_key_count",
        F.count("*").over(key_window),
    )

    # 2. Validar campos obligatorios.
    null_required = reduce(
        lambda left, right: left | right,
        [F.col(field).isNull() for field in REQUIRED_FIELDS],
    )

    # 3. Validar conversión de unidades.
    valid_conversion = (
        F.when(
            F.col("unidad") == "CS",
            F.col("cantidad_st") == F.col("cantidad") * 20,
        )
        .when(
            F.col("unidad") == "ST",
            F.col("cantidad_st") == F.col("cantidad"),
        )
        .otherwise(F.lit(False))
    )

    # 4. Validar vigencia histórica del material.
    delivery_date = F.to_date(
        F.col("fecha_proceso"),
        "yyyyMMdd",
    )

    valid_material_version = (
        delivery_date.between(
            F.col("material_valid_from"),
            F.col("material_valid_to"),
        )
    )

    # 5. Definir condiciones de incumplimiento.
    checks = {
        "required_fields_not_null": null_required,

        "unique_business_key": (
            F.col("_key_count") > 1
        ),

        "positive_normalized_quantity": (
            F.col("cantidad_st").isNull()
            | (F.col("cantidad_st") <= 0)
        ),

        "transaction_price_present": (
            F.col("precio").isNull()
        ),

        "historical_material_version": (
            ~F.coalesce(
                valid_material_version,
                F.lit(False),
            )
        ),

        "valid_unit_conversion": (
            ~F.coalesce(
                valid_conversion,
                F.lit(False),
            )
        ),
    }

    # 6. Calcular los resultados en una agregación.
    results = checked_df.agg(
        F.count("*").alias("records_checked"),

        *[
            F.sum(
                F.when(
                    F.coalesce(condition, F.lit(True)),
                    1,
                ).otherwise(0)
            ).alias(name)

            for name, condition in checks.items()
        ],
    ).first()

    records_checked = results["records_checked"]

    # 7. Construir los registros de auditoría.
    logs = []

    for check_name in checks:
        log = create_quality_log(
            run_id=run_id,
            batch_id=batch_id,
            tenant_id=tenant,
            layer="silver",
            table_name="fact_deliveries",
            check_name=check_name,
            severity="critical",
            records_checked=records_checked,
            records_failed=results[check_name] or 0,
        )

        logs.append(log)

    return logs

def evaluate_preparation_quality(
    bronze_df,
    quarantine_df,
    run_id,
    batch_id,
    tenant,
):
    """Audita anomalías y descartes de la preparación Silver."""

    # 1. Contar los registros recibidos desde Bronze.
    bronze_count = bronze_df.count()

    # 2. Identificar duplicados exactos eliminados.
    deduplicated_df = deduplicate_deliveries(bronze_df)
    deduplicated_count = deduplicated_df.count()

    duplicates_removed = bronze_count - deduplicated_count

    # 3. Contabilizar tipos de entrega descartados.
    filtered_df = filter_delivery_types(deduplicated_df)
    filtered_count = filtered_df.count()

    invalid_types = deduplicated_count - filtered_count

    # 4. Contabilizar registros enviados a cuarentena.
    quarantine_count = quarantine_df.count()

    # 5. Definir métricas y severidades.
    checks = [
        (
            "invalid_delivery_type",
            "warning",
            deduplicated_count,
            invalid_types,
        ),
        (
            "exact_duplicates_removed",
            "info",
            bronze_count,
            duplicates_removed,
        ),
        (
            "quarantined_records",
            "info",
            filtered_count,
            quarantine_count,
        ),
    ]

    # 6. Convertir los resultados en quality logs.
    logs = []

    for name, severity, checked, failed in checks:
        log = create_quality_log(
            run_id=run_id,
            batch_id=batch_id,
            tenant_id=tenant,
            layer="silver",
            table_name="fact_deliveries",
            check_name=name,
            severity=severity,
            records_checked=checked,
            records_failed=failed,
        )

        logs.append(log)

    return logs

def enforce_critical_quality(logs, fail_on_critical):
    """Detiene el procesamiento si fallan controles críticos."""

    if not fail_on_critical:
        return

    failed_checks = [
        f"{log['check_name']}={log['records_failed']}"
        for log in logs
        if (
            log["check_severity"] == "critical"
            and log["records_failed"] > 0
        )
    ]

    if failed_checks:
        raise ValueError(
            "Critical quality checks failed: "
            + ", ".join(failed_checks)
        )