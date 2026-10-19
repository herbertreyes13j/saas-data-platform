"""Pruebas de métricas Gold e idempotencia Delta."""

from decimal import Decimal

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

from saas_pipeline.gold import (
    build_daily_metrics,
    save_gold_metrics,
)


@pytest.fixture(scope="module")
def spark():
    """Crea una sesión Spark con soporte Delta Lake."""

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("Test-Gold")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
    )

    session = configure_spark_with_delta_pip(
        builder
    ).getOrCreate()

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


@pytest.fixture
def sample_gold(spark):
    """Genera métricas Gold a partir de entregas sintéticas."""

    rows = [
        # País, fecha, tipo, cantidad, precio, ruta, transporte.
        (
            "gt", "20250312", "ZPRE",
            Decimal("40.00"), Decimal("5.00"), 101, 1,
        ),
        (
            "gt", "20250312", "ZPRE",
            Decimal("3.00"), Decimal("10.00"), 101, 2,
        ),
        (
            "gt", "20250312", "ZPRE",
            Decimal("2.00"), Decimal("7.00"), 102, 2,
        ),
        (
            "gt", "20250312", "Z04",
            Decimal("1.00"), Decimal("8.00"), 103, 3,
        ),
        (
            "gt", "20250415", "ZPRE",
            Decimal("10.00"), Decimal("2.00"), 105, 5,
        ),
    ]

    columns = [
        "_tenant_id",
        "fecha_proceso",
        "tipo_entrega",
        "cantidad_st",
        "precio",
        "ruta",
        "transporte",
    ]

    fact_df = spark.createDataFrame(rows, columns)

    return build_daily_metrics(fact_df)


def test_gold_business_metrics(sample_gold):
    """Comprueba agrupación y métricas solicitadas."""

    rows = sample_gold.collect()

    # Tres combinaciones de fecha y tipo de entrega.
    assert len(rows) == 3

    results = {
        (
            row["tenant_id"],
            row["fecha_proceso"],
            row["tipo_entrega"],
        ): row
        for row in rows
    }

    assert len(results) == 3

    # Tres entregas ZPRE de marzo.
    march = results["gt", "20250312", "ZPRE"]

    # 40 + 3 + 2 = 45 unidades.
    assert march["total_units"] == Decimal("45")

    # (40 * 5) + (3 * 10) + (2 * 7) = 244.
    assert march["total_revenue"] == Decimal("244")

    # Rutas: 101 y 102.
    assert march["active_routes"] == 2

    # Transportes: 1 y 2.
    assert march["active_transports"] == 2

    # El tipo Z04 debe estar en otro grupo.
    other_type = results["gt", "20250312", "Z04"]

    assert other_type["total_units"] == Decimal("1")
    assert other_type["total_revenue"] == Decimal("8")

    # Abril debe estar separado de marzo.
    april = results["gt", "20250415", "ZPRE"]

    assert april["total_units"] == Decimal("10")
    assert april["total_revenue"] == Decimal("20")


def test_gold_idempotency_and_partial_reprocessing(
    spark,
    sample_gold,
    tmp_path,
):
    """Verifica overwrite, replaceWhere y restauración."""

    gold_path = str(tmp_path / "gold_metrics")

    def read_snapshot():
        """Recupera una representación estable de Gold."""

        return (
            spark.read
            .format("delta")
            .load(gold_path)
            .orderBy(
                "tenant_id",
                "fecha_proceso",
                "tipo_entrega",
            )
            .collect()
        )

    # 1. Carga inicial de marzo y abril.
    save_gold_metrics(
        sample_gold,
        gold_path,
        "2025-03-01",
        "2025-04-30",
    )

    initial_rows = read_snapshot()

    assert len(initial_rows) == 3

    # 2. Ejecutar nuevamente la misma carga.
    save_gold_metrics(
        sample_gold,
        gold_path,
        "2025-03-01",
        "2025-04-30",
    )

    repeated_rows = read_snapshot()

    # Idempotencia: mismos registros y valores.
    assert repeated_rows == initial_rows

    # 3. Modificar artificialmente solo marzo.
    modified_march = (
        sample_gold
        .filter(
            F.col("fecha_proceso").between(
                "20250301",
                "20250331",
            )
        )
        .withColumn(
            "active_routes",
            F.col("active_routes") + 1,
        )
    )

    save_gold_metrics(
        modified_march,
        gold_path,
        "2025-03-01",
        "2025-03-31",
    )

    updated_rows = read_snapshot()

    def outside_march(row):
        return not (
            "20250301"
            <= row["fecha_proceso"]
            <= "20250331"
        )

    # Abril debe permanecer exactamente igual.
    original_april = [
        row for row in initial_rows
        if outside_march(row)
    ]

    updated_april = [
        row for row in updated_rows
        if outside_march(row)
    ]

    assert updated_april == original_april

    # Las dos filas de marzo deben haber cambiado.
    initial_march = {
        (row["fecha_proceso"], row["tipo_entrega"]): row
        for row in initial_rows
        if not outside_march(row)
    }

    updated_march = {
        (row["fecha_proceso"], row["tipo_entrega"]): row
        for row in updated_rows
        if not outside_march(row)
    }

    assert len(initial_march) == 2
    assert initial_march.keys() == updated_march.keys()

    for key, original in initial_march.items():
        updated = updated_march[key]

        assert (
            updated["active_routes"]
            == original["active_routes"] + 1
        )

        assert updated["total_units"] == original["total_units"]
        assert updated["total_revenue"] == original["total_revenue"]

    assert len(updated_rows) == len(initial_rows)

    # 4. Restaurar marzo con sus métricas originales.
    save_gold_metrics(
        sample_gold,
        gold_path,
        "2025-03-01",
        "2025-03-31",
    )

    restored_rows = read_snapshot()

    assert restored_rows == initial_rows
