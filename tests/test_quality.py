"""Pruebas automatizadas de calidad de datos."""

from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession, functions as F

from saas_pipeline.quality import (
    enforce_critical_quality,
    evaluate_silver_quality,
)
from saas_pipeline.quality_logs import create_quality_log


def test_quality_log_creation():
    """Verifica los campos principales de un quality log."""

    log = create_quality_log(
        run_id="run-001",
        batch_id="batch-001",
        tenant_id="gt",
        layer="silver",
        table_name="fact_deliveries",
        check_name="required_fields_not_null",
        severity="critical",
        records_checked=10,
        records_failed=0,
    )

    assert log["_run_id"] == "run-001"
    assert log["_batch_id"] == "batch-001"
    assert log["tenant_id"] == "gt"
    assert log["check_severity"] == "critical"
    assert log["records_checked"] == 10
    assert log["records_failed"] == 0
    assert log["check_passed"] is True


@pytest.mark.parametrize(
    "severity,failed",
    [
        ("critical", 2),
        ("warning", 5),
        ("info", 1),
    ],
)
def test_quality_log_detects_failures(severity, failed):
    """Registra anomalías independientemente de su severidad."""

    log = create_quality_log(
        run_id="run-001",
        batch_id="batch-001",
        tenant_id="gt",
        layer="silver",
        table_name="fact_deliveries",
        check_name="test_check",
        severity=severity,
        records_checked=10,
        records_failed=failed,
    )

    assert log["records_failed"] == failed
    assert log["check_passed"] is False


def test_warning_does_not_stop_pipeline():
    """Un warning no debe detener el procesamiento."""

    logs = [{
        "check_name": "invalid_delivery_type",
        "check_severity": "warning",
        "records_failed": 19,
    }]

    enforce_critical_quality(logs, True)


def test_disabled_critical_policy():
    """Un fallo crítico se tolera cuando la política está desactivada."""

    logs = [{
        "check_name": "invalid_quantity",
        "check_severity": "critical",
        "records_failed": 2,
    }]

    enforce_critical_quality(logs, False)


def test_enabled_critical_policy():
    """Un fallo crítico detiene el procesamiento cuando está activado."""

    logs = [{
        "check_name": "invalid_quantity",
        "check_severity": "critical",
        "records_failed": 2,
    }]

    with pytest.raises(
        ValueError,
        match="invalid_quantity=2",
    ):
        enforce_critical_quality(logs, True)


def test_silver_quality_with_invalid_data():
    """Comprueba que los checks detecten anomalías sintéticas."""

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("Test-Silver-Quality")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        rows = [(
            "gt",
            "20250312",
            1,
            101,
            "ZPRE",
            "MAT001",
            Decimal("5.00"),
            Decimal("2.00"),
            "CS",
            Decimal("40.00"),
            date(2025, 1, 1),
            date(2025, 12, 31),
        )]

        columns = [
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
            "material_valid_from",
            "material_valid_to",
        ]

        valid_df = spark.createDataFrame(rows, columns)

        # Primero: el registro válido debe pasar.
        valid_logs = evaluate_silver_quality(
            valid_df,
            "run-test",
            "batch-test",
            "gt",
        )

        assert len(valid_logs) == 6
        assert all(
            log["records_failed"] == 0
            for log in valid_logs
        )

        # Segundo: introducir dos anomalías deliberadamente.
        invalid_df = (
            valid_df
            .withColumn(
                "precio",
                F.lit(None).cast("decimal(38,18)"),
            )
            .withColumn(
                "cantidad_st",
                F.lit(0).cast("decimal(38,18)"),
            )
        )

        invalid_logs = evaluate_silver_quality(
            invalid_df,
            "run-test",
            "batch-test",
            "gt",
        )

        failures = {
            log["check_name"]: log["records_failed"]
            for log in invalid_logs
        }

        assert failures["required_fields_not_null"] == 1
        assert failures["transaction_price_present"] == 1
        assert failures["positive_normalized_quantity"] == 1
        assert failures["valid_unit_conversion"] == 1

        # Tercero: duplicar la clave de negocio.
        duplicated_df = valid_df.unionByName(valid_df)

        duplicate_logs = evaluate_silver_quality(
            duplicated_df,
            "run-test",
            "batch-test",
            "gt",
        )

        duplicate_failures = {
            log["check_name"]: log["records_failed"]
            for log in duplicate_logs
        }

        assert duplicate_failures["unique_business_key"] == 2

    finally:
        spark.stop()
