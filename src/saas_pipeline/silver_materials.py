"""Preparación del catálogo histórico de materiales."""

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql import Window
from delta.tables import DeltaTable


def prepare_materials(bronze_df):
    """Convierte los campos del catálogo a sus tipos de negocio."""

    return (
        bronze_df
        .withColumn(
            "precio_base",
            F.col("precio_base").cast(DecimalType(38, 18))
        )
        .withColumn(
            "valid_from",
            F.to_date("valid_from", "yyyy-MM-dd")
        )
        .withColumn(
            "valid_to",
            F.to_date("valid_to", "yyyy-MM-dd")
        )
        .withColumn(
            "is_current",
            F.col("is_current").cast("boolean")
        )
    )

def validate_material_history(df):
    """Identifica inconsistencias en las versiones históricas."""

    history_window = (
        Window.partitionBy("material")
        .orderBy("valid_from", "valid_to")
        .rowsBetween(Window.unboundedPreceding, -1)
    )

    material_window = Window.partitionBy("material")

    checked_df = (
        df.withColumn(
            "_previous_valid_to",
            F.max("valid_to").over(history_window)
        )
        .withColumn(
            "_current_versions",
            F.sum(
                F.when(F.col("is_current"), 1)
                .otherwise(0)
            ).over(material_window)
        )
    )

    invalid_df = checked_df.filter(
        F.col("material").isNull()
        | F.col("precio_base").isNull()
        | F.col("valid_from").isNull()
        | F.col("valid_to").isNull()
        | F.col("is_current").isNull()
        | (F.col("valid_from") > F.col("valid_to"))
        | (F.col("valid_from") <= F.col("_previous_valid_to"))
        | (F.col("_current_versions") > 1)
    )

    return invalid_df

def merge_materials(spark, materials_df, silver_path):
    """Inserta o actualiza versiones históricas de materiales."""

    dimension_df = materials_df.select(
        "material",
        "descripcion",
        "categoria",
        "precio_base",
        "valid_from",
        "valid_to",
        "is_current",
    )

    if DeltaTable.isDeltaTable(spark, silver_path):
        target = DeltaTable.forPath(spark, silver_path)

        (
            target.alias("target")
            .merge(
                dimension_df.alias("source"),
                """
                target.material = source.material
                AND target.valid_from = source.valid_from
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:
        (
            dimension_df.write
            .format("delta")
            .mode("errorifexists")
            .save(silver_path)
        )

def enrich_deliveries_with_materials(deliveries_df, materials_df):
    """Enriquece las entregas con su versión histórica de material."""

    deliveries = deliveries_df.alias("d")
    materials = materials_df.alias("m")

    delivery_date = F.to_date(
        F.col("d.fecha_proceso"),
        "yyyyMMdd"
    )

    join_condition = (
        (F.col("d.material") == F.col("m.material"))
        & delivery_date.between(
            F.col("m.valid_from"),
            F.col("m.valid_to")
        )
    )

    return (
        deliveries.join(
            materials,
            on=join_condition,
            how="left"
        )
        .select(
            "d.*",
            F.col("m.descripcion").alias("material_descripcion"),
            F.col("m.categoria").alias("material_categoria"),
            F.col("m.precio_base").alias("material_precio_base"),
            F.col("m.valid_from").alias("material_valid_from"),
            F.col("m.valid_to").alias("material_valid_to"),
        )
    )