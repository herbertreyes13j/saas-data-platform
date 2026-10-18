
"""Ingesta de entregas desde RAW hacia Bronze."""

from pathlib import Path
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    DateType, BooleanType
)


DELIVERIES_SCHEMA = StructType([
    StructField("pais", StringType(), True),
    StructField("fecha_proceso", StringType(), True),
    StructField("transporte", LongType(), True),
    StructField("ruta", LongType(), True),
    StructField("tipo_entrega", StringType(), True),
    StructField("material", StringType(), True),
    StructField("precio", DecimalType(38, 18), True),
    StructField("cantidad", DecimalType(38, 18), True),
    StructField("unidad", StringType(), True),
])


def ingest_deliveries(spark, config, tenant):
    """Ingesta entregas por tenant, con cuarentena e idempotencia."""

    raw_path = (
        Path(config.paths.raw)
        / "global_mobility_data_entrega_productos.csv"
    )

    batch_id = str(uuid4())

    # Leer CSV utilizando un esquema explícito.
    df = (
        spark.read
        .option("header", "true")
        .schema(DELIVERIES_SCHEMA)
        .csv(str(raw_path))
    )

    # Agregar columnas técnicas.
    df = (
        df.withColumn(
            "_tenant_id",
            F.lower(F.trim(F.col("pais")))
        )
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp()
        )
        .withColumn(
            "_source_file",
            F.input_file_name()
        )
        .withColumn(
            "_batch_id",
            F.lit(batch_id)
        )
    )

    # Mantener solamente el tenant solicitado.
    df = df.filter(F.col("_tenant_id") == tenant)

    # Comprobar que la fecha tenga formato YYYYMMDD
    # y represente una fecha real.
    valid_date = F.coalesce(
        F.col("fecha_proceso").rlike(r"^[0-9]{8}$")
        & F.to_date(
            F.col("fecha_proceso"), "yyyyMMdd"
        ).isNotNull(),
        F.lit(False),
    )

    valid_df = df.filter(valid_date)

    quarantine_df = (
        df.filter(~valid_date)
        .withColumn(
            "_quarantine_reason",
            F.lit("invalid_fecha_proceso")
        )
    )

    # Separar registros con fechas inválidas.
    quarantine_path = (
        Path(config.paths.quarantine_root)
        / "bronze_quarantine"
        / tenant
        / "deliveries"
    )

    (
        quarantine_df.write
        .format("delta")
        .mode("overwrite")
        .save(str(quarantine_path))
    )

    # Aplicar rango de fechas solicitado.
    start_key = config.execution.start_date.replace("-", "")
    end_key = config.execution.end_date.replace("-", "")

    bronze_df = valid_df.filter(
        F.col("fecha_proceso").between(start_key, end_key)
    )

    bronze_path = (
        Path(config.paths.bronze)
        / tenant
        / "deliveries"
    )

    # Sobrescribir únicamente las fechas del rango.
    replace_condition = (
        f"fecha_proceso >= '{start_key}' "
        f"AND fecha_proceso <= '{end_key}'"
    )

    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_condition)
        .partitionBy("fecha_proceso")
        .save(str(bronze_path))
    )

    return {
        "tenant": tenant,
        "bronze_records": bronze_df.count(),
        "quarantine_records": quarantine_df.count(),
        "bronze_path": str(bronze_path),
    }

MATERIALS_CATALOG_SCHEMA = StructType([
    StructField("material", StringType(), True),
    StructField("descripcion", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("precio_base", DecimalType(38, 18), True),
    StructField("valid_from", DateType(), True),
    StructField("valid_to", DateType(), True),
    StructField("is_current", BooleanType(), True),
])


def ingest_materials(spark, raw_path, bronze_path):
    """Ingiere el catálogo histórico de materiales hacia Bronze."""

    raw_df = (
        spark.read
        .option("header", "true")
        .schema(MATERIALS_CATALOG_SCHEMA)
        .csv(raw_path)
    )

    bronze_df = (
        raw_df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit(str(uuid4())))
    )

    (
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(bronze_path)
)