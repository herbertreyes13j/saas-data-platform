# Code Review — Pipeline de entregas

## Contexto

Se revisó `mentoring/bad_code.py`, una implementación inicial
para procesar entregas de productos por país.

El objetivo de esta revisión es identificar riesgos de
escalabilidad, calidad, mantenibilidad e idempotencia,
y proponer mejoras alineadas con la arquitectura SAAS
basada en PySpark y Delta Lake.

Las observaciones están ordenadas por prioridad.

## 1. Uso de pandas para procesamiento distribuido

**Prioridad:** Alta

**Problema:**
El código utiliza `pd.read_csv()` para cargar el archivo
completo en memoria, aunque el proyecto utiliza Apache Spark.

**Impacto:**
El procesamiento depende de la memoria de una sola máquina
y no aprovecha la ejecución distribuida de Spark.

**Corrección:**
Leer los archivos mediante `spark.read`, utilizando un
esquema explícito y transformaciones nativas de PySpark.

## 2. Procesamiento fila por fila con iterrows()

**Prioridad:** Alta

**Problema:**
La función recorre todos los registros mediante
`df.iterrows()` y construye una lista de diccionarios.

**Impacto:**
El procesamiento secuencial introduce sobrecarga,
dificulta la escalabilidad y desaprovecha el optimizador
de consultas de Spark.

**Corrección:**
Reemplazar los bucles por operaciones declarativas:
`filter`, `withColumn`, `when` y `select`.

## 3. Ausencia de validaciones y manejo de anomalías

**Prioridad:** Crítica

**Problema:**
No se comprueban cantidades nulas, negativas o iguales
a cero, precios nulos, fechas inválidas ni materiales
desconocidos.

**Impacto:**
Los datos incorrectos pueden generar métricas erróneas,
y no existe trazabilidad de los registros rechazados.

**Corrección:**
Aplicar validaciones antes de publicar los datos
analíticos y enviar las anomalías correspondientes
a cuarentena con una razón identificable.

## 4. Conversión de unidades incompleta

**Prioridad:** Alta

**Problema:**
Se multiplica por 20 cuando `unidad == "CS"`, pero
cualquier otro valor se interpreta automáticamente
como unidades individuales.

**Impacto:**
Valores desconocidos, como `XX`, pueden producir
cantidades normalizadas incorrectas.

**Corrección:**
Aceptar explícitamente `CS` y `ST`, aplicar sus
conversiones correspondientes y tratar las unidades
desconocidas como registros inválidos.

## 5. Reglas de negocio hardcoded

**Prioridad:** Media-alta

**Problema:**
Los tipos de entrega válidos están escritos directamente
dentro del condicional de la función.

Además, el código solo considera ZPRE y ZVE1,
sin contemplar Z04 y Z05.

**Impacto:**
La implementación omite entregas válidas y obliga a
modificar la lógica cada vez que cambian las reglas.

**Corrección:**
Centralizar los tipos permitidos y utilizar expresiones
de filtrado declarativas. Diferenciar entregas
rutinarias y bonificaciones según las reglas del negocio.

## 6. Falta de enriquecimiento histórico SCD Type 2

**Prioridad:** Alta

**Problema:**
El código no valida la existencia del material ni
selecciona su versión histórica correspondiente.

**Impacto:**
No es posible garantizar que las transacciones estén
relacionadas con la versión correcta del catálogo
según su fecha de procesamiento.

**Corrección:**
Integrar `dim_materials` mediante un join temporal
utilizando `material` y el intervalo
`valid_from` / `valid_to`.

Los materiales sin correspondencia deben tratarse
como anomalías visibles.

## 7. Escritura sin estrategia de reprocesamiento parcial

**Prioridad:** Crítica

**Problema:**
El código sobrescribe una ruta Parquet fija mediante
`mode("overwrite")`.

**Impacto:**
Una carga completa puede producir el mismo resultado
al repetirse, pero el diseño no protege las fechas
ajenas a un reprocesamiento parcial.

Además, no utiliza las garantías transaccionales
de Delta Lake.

**Corrección:**
Utilizar Delta Lake con estrategias apropiadas
para cada capa:

- Bronze: reemplazo controlado por partición.
- Silver: MERGE mediante claves de negocio.
- Gold: recálculo por fecha mediante replaceWhere.

## 8. Acoplamiento, observabilidad y ejecución automática

**Prioridad:** Media-alta

**Problema:**
La sesión Spark se crea globalmente, el archivo y país
están hardcoded en la llamada final, y la función
se ejecuta automáticamente al importar el módulo.

El procesamiento tampoco registra identificadores
de ejecución, métricas ni errores estructurados.

**Impacto:**
Se dificulta reutilizar el código, probar funciones
aisladamente e identificar fallos durante una ejecución.

**Corrección:**
Separar lectura, transformaciones, validaciones y
persistencia en funciones independientes.

Inyectar la sesión Spark y recibir rutas, tenant
y rango de fechas mediante parámetros o configuración.

Utilizar logs estructurados e identificadores de lote.

## Conclusión

La implementación inicial expresa una intención funcional,
pero presenta riesgos importantes para un sistema de datos
multi-tenant: procesamiento no distribuido, ausencia de
controles de calidad y escritura sin aislamiento temporal.

El refactor debe priorizar la corrección de los datos,
la trazabilidad y la idempotencia antes de optimizaciones
secundarias.

Se recomienda preservar la claridad de las reglas de
negocio, implementar pruebas automatizadas y reutilizar
los componentes existentes de la plataforma SAAS.


---

## Cómo se lo explicaría al junior

Primero organizaría una reunión breve con él para revisar juntos
el código y las observaciones que encontré.

Empezaría reconociendo que su solución es fácil de entender y
que la idea principal del requerimiento está clara. Considero
que es un buen punto de partida, pero le explicaría que existen
formas más eficientes y mantenibles de conseguir el mismo resultado.

Le mostraría algunos ejemplos de la revisión, como el uso de
transformaciones nativas de Spark en lugar de procesar los datos
fila por fila. La idea sería explicarle cómo estas prácticas nos
ayudan a aprovechar mejor los recursos y herramientas de la empresa,
además de reducir problemas de rendimiento y mantenimiento.

También le haría énfasis en que no debemos desarrollar pensando
únicamente en que el código funcione hoy. Los sistemas evolucionan,
los requerimientos cambian y pueden surgir migraciones hacia otras
tecnologías. Por eso es importante procurar que nuestras soluciones
sean claras, reutilizables y fáciles de adaptar en el futuro.

Finalmente, le recomendaría investigar las diferencias entre
pandas y PySpark, las transformaciones de DataFrames y las buenas
prácticas de código reutilizable. Mi objetivo sería que entendiera
las razones detrás de cada cambio y pudiera aplicar ese criterio
en futuros desarrollos.
