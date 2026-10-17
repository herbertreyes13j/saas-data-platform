"""Transformaciones de negocio para la capa Silver."""

from pyspark.sql import functions as F


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