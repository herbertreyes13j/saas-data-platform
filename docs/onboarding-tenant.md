# Onboarding de un nuevo tenant — Proyecto SAAS

## Objetivo

Documentar los pasos necesarios para incorporar un nuevo tenant
a la plataforma de datos, conservando las reglas de negocio,
el aislamiento de información y la arquitectura Medallion.

Como ejemplo, utilizaremos Costa Rica (`cr`).

Actualmente el pipeline soporta seis tenants:
`ec`, `gt`, `hn`, `jm`, `pe` y `sv`.

## 1. Registrar la configuración del tenant

Crear el archivo:

`config/tenants/cr.yaml`

Con el siguiente contenido:

```yaml
tenant:
  id: cr
```

La configuración del tenant se combina con:

- `config/base.yaml`: parámetros compartidos.
- `config/env/dev.yaml`: configuración de desarrollo.
- `config/env/qa.yaml`: configuración de QA.
- `config/env/main.yaml`: ambiente principal.

De esta manera, el nuevo tenant utiliza las mismas reglas
generales, pero puede incorporar configuraciones particulares.

## 2. Habilitar el nuevo tenant

En la implementación actual existe una lista explícita de
tenants permitidos.

Actualizar `VALID_TENANTS` en `src/saas_pipeline/config.py`:

```python
VALID_TENANTS = {"sv", "hn", "ec", "jm", "gt", "pe", "cr"}
```

También agregar `cr` a las opciones de `--tenant`
en `src/saas_pipeline/cli.py`.

Esto es necesario porque el MVP valida los tenants mediante
listas explícitas. Una mejora futura sería descubrirlos
automáticamente desde los archivos de configuración.

## 3. Preparar los datos de origen

El archivo transaccional debe incluir registros del nuevo país
utilizando el código `CR` en la columna `pais`.

El archivo de entrada es:

`data/raw/global_mobility_data_entrega_productos.csv`

El catálogo compartido se encuentra en:

`data/raw/materials_catalog.csv`

Es importante verificar que los materiales utilizados por
el nuevo tenant existan en el catálogo y dispongan de
versiones históricas válidas para las fechas procesadas.

No se requiere crear un pipeline diferente para cada país.

## 4. Validar la configuración

Antes de ejecutar Spark, comprobar que los parámetros
del tenant se cargan correctamente:

```bash
PYTHONPATH=src python -m saas_pipeline.cli \
  --env dev \
  --tenant cr \
  --validate-only
```

Este comando valida la configuración sin procesar datos.

Si falla, revisar el archivo YAML y el registro del tenant.

## 5. Ejecutar el pipeline

Una vez preparada la información RAW:

```bash
PYTHONPATH=src python -m saas_pipeline.cli \
  --env dev \
  --tenant cr \
  --start-date 2025-01-01 \
  --end-date 2025-06-30
```

El pipeline utiliza las mismas transformaciones y controles
que los demás tenants.

Los datos se almacenan en rutas independientes:

```text
data/bronze/cr/deliveries/
data/silver/cr/dim_materials/
data/silver/cr/fact_deliveries/
data/gold/cr/daily_metrics_by_delivery_type/
data/bronze_quarantine/cr/deliveries/
data/silver_quarantine/cr/fact_deliveries/
```

Las rutas se construyen mediante la configuración existente.
No es necesario escribirlas manualmente en el código.

## 6. Validar los resultados

Después de la ejecución, comprobar:

1. Que Bronze contiene únicamente registros del tenant.
2. Que las cantidades CS se convierten correctamente a ST.
3. Que los tipos de entrega corresponden a las reglas válidas.
4. Que las transacciones se enriquecen con la versión
   histórica correcta del material (SCD Type 2).
5. Que los registros inválidos se conservan en cuarentena.
6. Que las validaciones aparecen en `quality_logs`.
7. Que Gold contiene las métricas agregadas esperadas.

También se recomienda repetir la ejecución del mismo rango
y comprobar que no se duplican los registros.

## 7. Infraestructura en Databricks

En un entorno cloud se necesitaría aprovisionar:

- `saas_dev.bronze_cr`
- `saas_dev.silver_cr`
- `saas_dev.gold_cr`

También serían necesarios los directorios en ADLS Gen2,
external locations y los permisos correspondientes.

La propuesta de aprovisionamiento se encuentra en
[Infraestructura con Terraform](infra.md).

El alcance actual utiliza rutas locales y no crea recursos
reales en Azure ni Unity Catalog.

## 8. Consideraciones para producción

Antes de habilitar un tenant en producción se recomienda:

- Validar los permisos y el aislamiento de datos.
- Confirmar la disponibilidad de sus fuentes.
- Ejecutar pruebas en desarrollo y QA.
- Revisar los controles de calidad y sus umbrales.
- Verificar el reprocesamiento de períodos históricos.
- Documentar los responsables del tenant.

El objetivo es que incorporar nuevos países requiera
principalmente configuración y validación, sin duplicar
la lógica de transformación del pipeline.

## 9. Limitación actual

El onboarding todavía requiere registrar manualmente
el tenant en la configuración de Python y la CLI.

Para una siguiente etapa se propone descubrir tenants
desde YAML y automatizar su aprovisionamiento mediante
Terraform, manteniendo controles de autorización.

Esta decisión también se explica en
[Observaciones de arquitectura](observations.md).
