from pyspark.sql import SparkSession

from saas_pipeline.silver import transform_deliveries


def test_silver_business_transformations():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("Test-Silver")
        .getOrCreate()
    )

    try:
        rows = [
            ("gt", "20261001", 1, 10, "ZPRE", "MAT001", 5.0, 2.0, "CS"),
            ("gt", "20261001", 1, 10, "ZPRE", "MAT001", 5.0, 2.0, "CS"),
            ("gt", "20261001", 2, 20, "Z04", "MAT002", 10.0, 3.0, "ST"),
            ("gt", "20261001", 3, 30, "ZVE1", "MAT999", None, -1.0, "ST"),
            ("gt", "20261001", 4, 40, "COBR", "MAT001", 15.0, 2.0, "ST"),
        ]

        columns = [
            "pais", "fecha_proceso", "transporte", "ruta",
            "tipo_entrega", "material", "precio", "cantidad", "unidad"
        ]

        bronze_df = spark.createDataFrame(rows, columns)

        catalog_df = spark.createDataFrame(
            [("MAT001",), ("MAT001",), ("MAT002",)],
            ["material"]
        )

        silver_df, quarantine_df = transform_deliveries(
            bronze_df, catalog_df
        )

        assert silver_df.count() == 2
        assert quarantine_df.count() == 1

        results = {
            row["material"]: row
            for row in silver_df.collect()
        }

        assert results["MAT001"]["cantidad_st"] == 40.0
        assert results["MAT001"]["is_routine_delivery"] is True

        assert results["MAT002"]["cantidad_st"] == 3.0
        assert results["MAT002"]["is_bonus_delivery"] is True

        reason = quarantine_df.first()["_quarantine_reason"]

        assert reason == (
            "invalid_quantity, null_price, unknown_material"
        )

    finally:
        spark.stop()

