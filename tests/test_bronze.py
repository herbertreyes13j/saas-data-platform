
"""Pruebas de integración para la capa Bronze."""

from collections import Counter

from delta import configure_spark_with_delta_pip
from omegaconf import OmegaConf
from pyspark.sql import SparkSession

from saas_pipeline.bronze import ingest_deliveries


def test_bronze_ingestion_and_reprocessing(tmp_path):
    """Valida ingesta, cuarentena, aislamiento e idempotencia."""

    # Preparar un CSV de prueba independiente.
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    csv_path = raw_dir / "global_mobility_data_entrega_productos.csv"

    csv_path.write_text(
        "pais,fecha_proceso,transporte,ruta,tipo_entrega,"
        "material,precio,cantidad,unidad\n"
        "GT,20250110,1,101,ZPRE,M001,10.50,2,CS\n"
        "GT,20250312,2,102,Z04,M002,20.00,1,ST\n"
        "GT,20250312,2,102,Z04,M002,20.00,1,ST\n"
        "GT,20250230,3,103,ZVE1,M003,5.00,1,CS\n"
        "SV,20250312,4,104,ZPRE,M004,12.00,1,ST\n",
        encoding="utf-8",
    )

    config = OmegaConf.create({
        "paths": {
            "raw": str(raw_dir),
            "bronze": str(tmp_path / "bronze"),
            "quarantine_root": str(tmp_path),
        },
        "execution": {
            "start_date": "2025-01-01",
            "end_date": "2025-06-30",
        },
    })

    builder = (
        SparkSession.builder
        .appName("BronzeIntegrationTest")
        .master("local[2]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    bronze_path = str(tmp_path / "bronze" / "gt" / "deliveries")
    quarantine_path = str(
        tmp_path / "bronze_quarantine" / "gt" / "deliveries"
    )

    business_columns = [
        "pais", "fecha_proceso", "transporte", "ruta",
        "tipo_entrega", "material", "precio", "cantidad", "unidad",
    ]

    def snapshot():
        """Obtiene una copia de los registros de negocio."""
        df = spark.read.format("delta").load(bronze_path)
        return Counter(
            tuple(row)
            for row in df.select(*business_columns).collect()
        )

    try:
        # Primera ejecución.
        result = ingest_deliveries(spark, config, "gt")

        assert result["bronze_records"] == 3
        assert result["quarantine_records"] == 1

        bronze = spark.read.format("delta").load(bronze_path)
        quarantine = spark.read.format("delta").load(quarantine_path)

        assert bronze.count() == 3
        assert quarantine.count() == 1
        assert bronze.filter("_tenant_id != 'gt'").count() == 0

        # Bronze conserva los duplicados originales.
        assert bronze.filter("fecha_proceso = '20250312'").count() == 2

        for column in [
            "_tenant_id",
            "_ingestion_timestamp",
            "_source_file",
            "_batch_id",
        ]:
            assert column in bronze.columns

        before = snapshot()

        # Segunda ejecución: no debe duplicar registros.
        ingest_deliveries(spark, config, "gt")
        assert snapshot() == before

        # Reprocesar únicamente marzo.
        config.execution.start_date = "2025-03-01"
        config.execution.end_date = "2025-03-31"

        partial = ingest_deliveries(spark, config, "gt")

        assert partial["bronze_records"] == 2
        assert snapshot() == before

    finally:
        spark.stop()
