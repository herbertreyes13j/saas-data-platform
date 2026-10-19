"""Transformaciones de la capa Gold."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_daily_metrics(fact_df: DataFrame) -> DataFrame:
    """Calcula métricas diarias por país y tipo de entrega."""

    gold_df = (
        fact_df
        .groupBy(
            "_tenant_id",
            "fecha_proceso",
            "tipo_entrega",
        )
        .agg(
            F.sum("cantidad_st").alias("total_units"),
            F.sum(
                F.col("cantidad_st") * F.col("precio")
            ).alias("total_revenue"),
            F.countDistinct("ruta").alias("active_routes"),
            F.countDistinct("transporte").alias("active_transports"),
        )
        .withColumnRenamed("_tenant_id", "tenant_id")
    )

    return gold_df


def save_gold_metrics(
    gold_df: DataFrame,
    gold_path: str,
    start_date: str,
    end_date: str,
) -> None:
    """Guarda Gold con reemplazo de particiones por rango de fechas."""

    # Convertir YYYY-MM-DD a YYYYMMDD.
    start_key = start_date.replace("-", "")
    end_key = end_date.replace("-", "")

    # Limitar el reemplazo al período solicitado.
    replace_condition = (
        f"fecha_proceso >= '{start_key}' "
        f"AND fecha_proceso <= '{end_key}'"
    )

    # Conservar únicamente las fechas del reprocesamiento.
    scoped_df = gold_df.filter(
        F.col("fecha_proceso").between(start_key, end_key)
    )

    (
        scoped_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_condition)
        .partitionBy("fecha_proceso")
        .save(gold_path)
    )
