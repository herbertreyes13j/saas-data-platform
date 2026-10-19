"""Versión refactorizada del procesamiento de entregas."""

from pyspark.sql import SparkSession, functions as F


# En la versión anterior se utilizaba pandas para leer el CSV,
# sin definir los tipos de datos.
# Acá definimos un esquema para que Spark trabaje con los tipos
# correctos desde el inicio y evitar problemas de inferencia.
SCHEMA = """
    pais STRING,
    fecha_proceso STRING,
    transporte BIGINT,
    ruta BIGINT,
    tipo_entrega STRING,
    material STRING,
    precio DECIMAL(18,4),
    cantidad DECIMAL(18,4),
    unidad STRING
"""

# Antes los tipos válidos estaban escritos dentro de un if
# y solamente se contemplaban ZPRE y ZVE1.
# Los centralizamos e incluimos Z04 y Z05 según las reglas
# del negocio, facilitando futuros cambios.
ALLOWED_TYPES = ["ZPRE", "ZVE1", "Z04", "Z05"]


def process(
    spark: SparkSession,
    file_path: str,
    country: str,
    output_path: str,
):
    """Procesa las entregas de un país."""

    # Antes la sesión Spark se creaba globalmente y el país
    # y las rutas estaban definidos directamente en el código.
    # Ahora los recibimos como parámetros para que la función
    # sea reutilizable y más fácil de probar.

    # En la versión anterior se utilizaba pd.read_csv(),
    # cargando los datos en memoria de una sola máquina.
    # Lo cambiamos por Spark para aprovechar su capacidad
    # de procesamiento distribuido.
    df = (
        spark.read
        .option("header", True)
        .schema(SCHEMA)
        .csv(file_path)
        .filter(F.upper(F.col("pais")) == country.upper())
        .dropDuplicates()
    )

    # El código anterior tampoco eliminaba duplicados,
    # lo que podía provocar que una entrega se contara
    # más de una vez. Ahora los eliminamos desde la lectura.

    # Antes se utilizaban condiciones if para verificar
    # los tipos de entrega, lo que complicaba su mantenimiento.
    # Con isin() hacemos el filtro directamente en Spark.
    df = df.filter(
        F.col("tipo_entrega").isin(*ALLOWED_TYPES)
    )

    # En la versión anterior no se validaban cantidades,
    # precios ni unidades antes de calcular los ingresos.
    # Esto podía generar métricas incorrectas.
    # Ahora verificamos primero los campos necesarios.
    valid = (
        (F.col("cantidad") > 0)
        & F.col("precio").isNotNull()
        & F.col("unidad").isin("CS", "ST")
        & F.col("material").isNotNull()
        & F.to_date(
            "fecha_proceso", "yyyyMMdd"
        ).isNotNull()
    )

    # Como algunas condiciones pueden devolver NULL,
    # lo interpretamos como un registro inválido.
    valid = F.coalesce(valid, F.lit(False))

    # Antes los registros inválidos no se identificaban
    # ni se separaban para una revisión posterior.
    # Ahora los enviamos a un DataFrame de cuarentena,
    # evitando que afecten los resultados.
    quarantine = df.filter(~valid)
    df = df.filter(valid)

    # Antes se utilizaba iterrows() para recorrer cada fila
    # y calcular las cantidades mediante condiciones if.
    # Esto era menos eficiente porque procesaba los registros
    # secuencialmente y desaprovechaba Spark.
    #
    # Ahora usamos withColumn() y when() para normalizar
    # las cantidades directamente en el DataFrame.
    # CS equivale a 20 unidades y ST a una unidad individual.
    df = df.withColumn(
        "cantidad_st",
        F.when(
            F.col("unidad") == "CS",
            F.col("cantidad") * 20,
        ).otherwise(F.col("cantidad")),
    )

    # Antes se calculaba el total dentro del mismo bucle.
    # Ahora aprovechamos las operaciones de Spark para
    # multiplicar la cantidad normalizada por el precio
    # de cada transacción.
    result = (
        df.withColumn(
            "total",
            F.col("cantidad_st") * F.col("precio"),
        )
        .select(
            "pais",
            F.col("fecha_proceso").alias("fecha"),
            "material",
            "cantidad_st",
            "total",
        )
    )

    # En la versión anterior se utilizaba Parquet con una
    # ruta fija dentro de /tmp/output/.
    # Ahora parametrizamos la ruta y utilizamos Delta Lake,
    # que ofrece soporte transaccional y versionado.
    #
    # Mantenemos overwrite porque este ejemplo procesa
    # una carga completa por país. Para reprocesamientos
    # parciales sería necesario usar MERGE o replaceWhere.
    result.write.format("delta").mode("overwrite").save(
        f"{output_path}/{country.lower()}"
    )

    # También guardamos los registros inválidos para
    # revisarlos posteriormente en lugar de perderlos.
    quarantine.write.format("delta").mode("overwrite").save(
        f"{output_path}/{country.lower()}_quarantine"
    )

    # Antes solamente se imprimía "done".
    # Ahora devolvemos el resultado para que pueda
    # utilizarse en otras funciones o pruebas.
    return result
