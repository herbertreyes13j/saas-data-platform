from datetime import date
from decimal import Decimal

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from saas_pipeline.silver_materials import (
    enrich_deliveries_with_materials,
    merge_materials,
)


def test_scd2_merge_and_temporal_join(tmp_path):
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("Test-SCD2")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    try:
        materials_df = spark.createDataFrame(
            [
                (
                    "MAT001", "Producto A", "BEBIDAS",
                    Decimal("31.95"),
                    date(2024, 1, 1),
                    date(2025, 3, 31),
                    False,
                ),
                (
                    "MAT001", "Producto A", "BEBIDAS",
                    Decimal("33.80"),
                    date(2025, 4, 1),
                    date(9999, 12, 31),
                    True,
                ),
            ],
            [
                "material", "descripcion", "categoria",
                "precio_base", "valid_from", "valid_to",
                "is_current",
            ],
        )

        path = str(tmp_path / "dim_materials")

        # Primera ejecución: crear la dimensión.
        merge_materials(spark, materials_df, path)

        # Segunda ejecución: no duplicar versiones.
        merge_materials(spark, materials_df, path)

        dimension_df = spark.read.format("delta").load(path)

        assert dimension_df.count() == 2

        # Entregas antes y después del cambio de precio.
        deliveries_df = spark.createDataFrame(
            [
                ("MAT001", "20250315"),
                ("MAT001", "20250415"),
                ("MAT999", "20250415"),
            ],
            ["material", "fecha_proceso"],
        )

        result_df = enrich_deliveries_with_materials(
            deliveries_df,
            dimension_df,
        )

        assert result_df.count() == 3

        results = {
            (row["material"], row["fecha_proceso"]): row
            for row in result_df.collect()
        }

        assert float(
            results["MAT001", "20250315"]["material_precio_base"]
        ) == 31.95

        assert float(
            results["MAT001", "20250415"]["material_precio_base"]
        ) == 33.80

        assert (
            results["MAT999", "20250415"]["material_precio_base"]
            is None
        )

    finally:
        spark.stop()