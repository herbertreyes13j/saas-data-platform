"""Transformaciones de negocio para la capa Silver."""
from pyspark.sql import Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable

def normalize_units(df):
    """Convierte cantidades de CS y ST a unidades individuales."""

    return df.withColumn(
        "cantidad_st",
        F.when(
            F.col("unidad") == "CS",
            F.col("cantidad") * 20
        )
        .when(
            F.col("unidad") == "ST",
            F.col("cantidad")
        )
        .otherwise(F.lit(None))
    )

def filter_delivery_types(df):
    """Conserva únicamente los tipos de entrega permitidos."""

    allowed_types = ["ZPRE", "ZVE1", "Z04", "Z05"]

    return df.filter(
        F.col("tipo_entrega").isin(allowed_types)
    )

def add_delivery_flags(df):
    """Agrega indicadores de rutina y bonificación."""

    return (
        df.withColumn(
            "is_routine_delivery",
            F.col("tipo_entrega").isin("ZPRE", "ZVE1")
        )
        .withColumn(
            "is_bonus_delivery",
            F.col("tipo_entrega").isin("Z04", "Z05")
        )
    )

BUSINESS_COLUMNS = [
    "pais",
    "fecha_proceso",
    "transporte",
    "ruta",
    "tipo_entrega",
    "material",
    "precio",
    "cantidad",
    "unidad",
]


def deduplicate_deliveries(df):
    """Elimina duplicados exactos de negocio."""

    return df.dropDuplicates(BUSINESS_COLUMNS)

def validate_quantities(df):
    """Separa entregas con cantidades válidas e inválidas."""

    invalid_quantity = (
        F.col("cantidad").isNull()
        | (F.col("cantidad") <= 0)
    )

    valid_df = df.filter(~invalid_quantity)

    quarantine_df = (
        df.filter(invalid_quantity)
        .withColumn(
            "_quarantine_reason",
            F.lit("invalid_quantity")
        )
    )

    return valid_df, quarantine_df

def validate_prices(df):
    """Separa entregas con precios válidos y nulos."""

    invalid_price = F.col("precio").isNull()

    valid_df = df.filter(~invalid_price)

    quarantine_df = (
        df.filter(invalid_price)
        .withColumn(
            "_quarantine_reason",
            F.lit("null_price")
        )
    )

    return valid_df, quarantine_df

def validate_materials(df, catalog_df):
    """Separa entregas según la existencia del material."""

    known_materials = (
        catalog_df
        .select("material")
        .distinct()
    )

    valid_df = df.join(
        known_materials,
        on="material",
        how="left_semi"
    )

    quarantine_df = (
        df.join(
            known_materials,
            on="material",
            how="left_anti"
        )
        .withColumn(
            "_quarantine_reason",
            F.lit("unknown_material")
        )
    )

    return valid_df, quarantine_df

def validate_business_rules(df, catalog_df):
    """Valida las reglas de negocio y registra los motivos de rechazo."""

    known_materials = (
        catalog_df
        .select("material")
        .distinct()
        .withColumn("_material_exists", F.lit(True))
    )

    checked_df = df.join(
        known_materials,
        on="material",
        how="left"
    )

    invalid_quantity = (
        F.col("cantidad").isNull()
        | (F.col("cantidad") <= 0)
    )

    invalid_price = F.col("precio").isNull()

    unknown_material = F.col("_material_exists").isNull()

    checked_df = (
        checked_df.withColumn(
            "_quarantine_reason",
            F.concat_ws(
                ", ",
                F.when(invalid_quantity, "invalid_quantity"),
                F.when(invalid_price, "null_price"),
                F.when(unknown_material, "unknown_material"),
            )
        )
        .drop("_material_exists")
    )

    valid_df = checked_df.filter(
        F.col("_quarantine_reason") == ""
    ).drop("_quarantine_reason")

    quarantine_df = checked_df.filter(
        F.col("_quarantine_reason") != ""
    )

    return valid_df, quarantine_df

def transform_deliveries(bronze_df, catalog_df):
    """Aplica las transformaciones de negocio de Silver."""

    # 1. Eliminar duplicados exactos.
    deduplicated_df = deduplicate_deliveries(bronze_df)

    # 2. Conservar tipos de entrega permitidos.
    filtered_df = filter_delivery_types(deduplicated_df)

    # 3. Separar registros válidos y en cuarentena.
    valid_df, quarantine_df = validate_business_rules(
        filtered_df,
        catalog_df
    )

    # 4. Normalizar cantidades.
    normalized_df = normalize_units(valid_df)

    # 5. Agregar indicadores de entrega.
    silver_df = add_delivery_flags(normalized_df)

    return silver_df, quarantine_df

def save_prepared_silver(
    silver_df,
    quarantine_df,
    silver_path,
    quarantine_path,
):
    """Guarda los resultados preparados en formato Delta."""

    silver_df.write.format("delta").mode("overwrite").save(
        silver_path
    )

    quarantine_df.write.format("delta").mode("overwrite").save(
        quarantine_path
    )

MERGE_KEY = [
    "_tenant_id",
    "fecha_proceso",
    "transporte",
    "ruta",
    "material",
    "tipo_entrega",
]


def separate_merge_key_conflicts(df):
    """Separa registros que comparten la clave del MERGE."""

    window = Window.partitionBy(*MERGE_KEY)

    checked_df = df.withColumn(
        "_key_count",
        F.count(F.lit(1)).over(window)
    )

    valid_df = (
        checked_df
        .filter(F.col("_key_count") == 1)
        .drop("_key_count")
    )

    quarantine_df = (
        checked_df
        .filter(F.col("_key_count") > 1)
        .drop("_key_count")
        .withColumn(
            "_quarantine_reason",
            F.lit("duplicate_merge_key")
        )
    )

    return valid_df, quarantine_df

def separate_null_merge_keys(df):
    """Separa registros con campos nulos en la clave de negocio."""

    null_key = (
        F.col("_tenant_id").isNull()
        | F.col("fecha_proceso").isNull()
        | F.col("transporte").isNull()
        | F.col("ruta").isNull()
        | F.col("material").isNull()
        | F.col("tipo_entrega").isNull()
    )

    valid_df = df.filter(~null_key)

    quarantine_df = (
        df.filter(null_key)
        .withColumn(
            "_quarantine_reason",
            F.lit("null_merge_key")
        )
    )

    return valid_df, quarantine_df

def apply_merge_key_checks(silver_df, quarantine_df):
    """Valida las claves de negocio y consolida la cuarentena."""

    non_null_df, null_keys_df = separate_null_merge_keys(
        silver_df
    )

    unique_df, conflicts_df = separate_merge_key_conflicts(
        non_null_df
    )

    all_quarantine_df = (
        quarantine_df
        .unionByName(null_keys_df, allowMissingColumns=True)
        .unionByName(conflicts_df, allowMissingColumns=True)
    )

    return unique_df, all_quarantine_df


def merge_fact_deliveries(
    spark, fact_df, silver_path, tenant, start_date, end_date
):
    """Inserta o actualiza entregas usando su clave de negocio."""

    if DeltaTable.isDeltaTable(spark, silver_path):

        target = DeltaTable.forPath(spark, silver_path)

        (
            target.alias("target")
            .merge(
                fact_df.alias("source"),
                """
                target._tenant_id = source._tenant_id
                AND target.fecha_proceso = source.fecha_proceso
                AND target.transporte = source.transporte
                AND target.ruta = source.ruta
                AND target.material = source.material
                AND target.tipo_entrega = source.tipo_entrega
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete(
            condition=(
                f"target._tenant_id = '{tenant}' "
                f"AND target.fecha_proceso BETWEEN "
                f"'{start_date.replace('-', '')}' "
                f"AND '{end_date.replace('-', '')}'"
            )
        )
            .execute()
        )

    else:
        (
            fact_df.write
            .format("delta")
            .mode("errorifexists")
            .partitionBy("fecha_proceso")
            .save(silver_path)
        )
