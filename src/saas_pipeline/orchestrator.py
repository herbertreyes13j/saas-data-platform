"""Orquestación del procesamiento por tenant."""

from pathlib import Path

from pyspark.sql import functions as F

from saas_pipeline.bronze import ingest_deliveries, ingest_materials
from saas_pipeline.silver import (
    apply_merge_key_checks,
    merge_fact_deliveries,
    transform_deliveries,
)
from saas_pipeline.silver_materials import (
    enrich_deliveries_with_materials,
    merge_materials,
    prepare_materials,
    validate_material_history,
)

def ingest_shared_catalog(spark, config):
    """Carga una sola vez el catálogo compartido en Bronze."""

    raw_path = str(
        Path(config.paths.raw) / "materials_catalog.csv"
    )

    bronze_path = str(
        Path(config.paths.bronze)
        / "shared"
        / "materials_catalog"
    )

    ingest_materials(spark, raw_path, bronze_path)

def run_tenant(spark, config, tenant):
    """Ejecuta Bronze y Silver para un tenant."""

    bronze_base = Path(config.paths.bronze)
    silver_base = Path(config.paths.silver)

    materials_bronze = str(
        bronze_base / "shared" / "materials_catalog"
    )
    materials_silver = str(
        silver_base / tenant / "dim_materials"
    )
    fact_path = str(
        silver_base / tenant / "fact_deliveries"
    )

    # 1. Ingestar fuentes en Bronze.


    ingest_deliveries(spark, config, tenant)

    # 2. Construir la dimensión histórica.
    materials_bronze_df = (
        spark.read.format("delta").load(materials_bronze)
    )

    materials_df = prepare_materials(materials_bronze_df)

    if validate_material_history(materials_df).limit(1).count():
        raise ValueError("Invalid SCD2 material history")

    merge_materials(spark, materials_df, materials_silver)

    # 3. Leer únicamente el rango solicitado.
    start = config.execution.start_date.replace("-", "")
    end = config.execution.end_date.replace("-", "")

    bronze_df = (
        spark.read.format("delta")
        .load(str(bronze_base / tenant / "deliveries"))
        .filter(F.col("fecha_proceso").between(start, end))
    )

    catalog_df = spark.read.format("delta").load(
        materials_silver
    )

    # 4. Aplicar las reglas de Silver.
    silver_df, quarantine_df = transform_deliveries(
        bronze_df, catalog_df
    )

    valid_df, quarantine_df = apply_merge_key_checks(
        silver_df, quarantine_df
    )

    # 5. Enriquecer con SCD2.
    enriched_df = enrich_deliveries_with_materials(
        valid_df, catalog_df
    )

    missing_version = F.col("material_valid_from").isNull()

    unmatched_df = (
        enriched_df.filter(missing_version)
        .withColumn(
            "_quarantine_reason",
            F.lit("missing_historical_material_version"),
        )
    )

    fact_df = enriched_df.filter(~missing_version)

    quarantine_df = quarantine_df.unionByName(
        unmatched_df,
        allowMissingColumns=True,
    )

    # 6. Persistir la tabla de hechos.
    merge_fact_deliveries(
        spark,
        fact_df,
        fact_path,
        tenant=tenant,
        start_date=config.execution.start_date,
        end_date=config.execution.end_date,
    )

    # 7. Persistir la cuarentena del rango procesado.
    quarantine_path = str(
        Path(config.paths.quarantine_root)
        / "silver_quarantine"
        / tenant
        / "fact_deliveries"
    )

    (
        quarantine_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "replaceWhere",
            f"fecha_proceso >= '{start}' "
            f"AND fecha_proceso <= '{end}'",
        )
        .partitionBy("fecha_proceso")
        .save(quarantine_path)
    )

    return {
        "tenant": tenant,
        "fact_count": fact_df.count(),
        "quarantine_count": quarantine_df.count(),
    }