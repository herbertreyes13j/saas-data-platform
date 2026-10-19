# SAAS Data Platform — Senior Data Engineer

Implementación de una plataforma de datos multi-tenant para el
procesamiento de entregas de productos, utilizando PySpark y Delta Lake.

El proyecto sigue la arquitectura Medallion (Bronze, Silver y Gold)
y permite ejecutar el mismo pipeline para distintos tenants,
ambientes y rangos de fechas mediante configuración jerárquica.

## 1. Arquitectura

```text
RAW (CSV)
   |
   v
BRONZE (Delta)
   |-- Ingesta por tenant
   |-- Columnas técnicas
   |-- Control de fechas y cuarentena
   |
   v
SILVER (Delta)
   |-- Deduplicación
   |-- Normalización CS -> ST
   |-- Validaciones y cuarentena
   |-- Catálogo histórico SCD Type 2
   |-- Enriquecimiento temporal
   |-- MERGE de entregas
   |
   v
GOLD (Delta)
   |-- daily_metrics_by_delivery_type
   |-- total_units
   |-- total_revenue
   |-- active_routes
   |-- active_transports

SHARED
   |-- quality_logs (Delta)
```

Cada tenant dispone de rutas de almacenamiento independientes.
Los resultados de calidad se centralizan en una tabla compartida.

La ejecución local simula la arquitectura Databricks + ADLS Gen2
+ Unity Catalog descrita en el enunciado.

## 2. Tecnologías

Versiones utilizadas en el proyecto:

| Componente | Versión |
|---|---|
| Python | 3.11 |
| Java | 17 |
| PySpark | 3.5.3 |
| Delta Lake (delta-spark) | 3.2.0 |
| OmegaConf | 2.3.0 |
| Pytest | 8.3.5 |
| Ruff | 0.11.13 |

Las dependencias Python están definidas en `requirements.txt`.

## 3. Estructura del repositorio

```text
saas-data-platform/
├── .github/workflows/ci.yml
├── config/
│   ├── base.yaml
│   ├── env/
│   │   ├── dev.yaml
│   │   ├── qa.yaml
│   │   └── main.yaml
│   └── tenants/
├── docs/
│   ├── infra.md
│   ├── observations.md
│   └── onboarding-tenant.md
├── mentoring/
│   ├── bad_code.py
│   ├── good_code.py
│   └── code_review.md
├── src/saas_pipeline/
│   ├── bronze.py
│   ├── silver.py
│   ├── silver_materials.py
│   ├── gold.py
│   ├── quality.py
│   ├── quality_logs.py
│   ├── orchestrator.py
│   ├── config.py
│   └── cli.py
├── tests/
├── requirements.txt
└── README.md
```

El directorio `data/` se genera y utiliza localmente.
No está versionado en Git.

## 4. Requisitos previos

Para ejecutar el proyecto localmente se necesita:

- Python 3.11.
- Java 17.
- Un entorno Linux o WSL Ubuntu.
- Acceso a Internet durante la instalación de dependencias.
- Los dos CSV originales suministrados con la prueba técnica.

Los CSV no están incluidos en este repositorio.

## 5. Instalación

Clonar el repositorio:

```bash
git clone https://github.com/herbertreyes13j/saas-data-platform.git
cd saas-data-platform
```

Crear y activar el entorno virtual:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

Instalar las dependencias:

```bash
python -m pip install -r requirements.txt
```

Configurar el directorio de código:

```bash
export PYTHONPATH=src
export SPARK_LOCAL_IP=127.0.0.1
```

Comprobar las versiones:

```bash
python --version
java -version
```

La primera ejecución de Spark con Delta Lake puede necesitar
descargar dependencias adicionales.

## 6. Preparación de los datos RAW

Crear el directorio:

```bash
mkdir -p data/raw
```

Colocar los archivos originales de la prueba en:

```text
data/raw/global_mobility_data_entrega_productos.csv
data/raw/materials_catalog.csv
```

Se deben conservar sus nombres y encabezados originales.

Estos archivos son necesarios para ejecutar el pipeline completo.
Las pruebas unitarias pueden ejecutarse sin ellos.

## 7. Configuración

La configuración se construye mediante OmegaConf:

1. `config/base.yaml`: valores compartidos.
2. `config/env/<env>.yaml`: rutas específicas del ambiente.
3. `config/tenants/<tenant>.yaml`: configuración del tenant.

Ambientes disponibles:

| Ambiente | Uso |
|---|---|
| dev | Desarrollo local |
| qa | Validación lógica |
| main | Ambiente principal, equivalente conceptual a producción |

Los tres ambientes están simulados mediante rutas locales.
`main` como ambiente no debe confundirse con la rama Git `main`.

Tenants implementados:

`ec`, `gt`, `hn`, `jm`, `pe`, `sv`.

Parámetros importantes:

- `execution.start_date` y `execution.end_date`.
- `execution.tenant`.
- `execution.fail_fast`.
- `quality.fail_on_critical`.
- Rutas de Bronze, Silver, Gold y cuarentena.

Si `quality.fail_on_critical` está activado, una validación
crítica fallida impide continuar con la persistencia Silver/Gold
del tenant afectado.

## 8. Ejecución del pipeline

Desde la raíz del repositorio, con el entorno activado:

```bash
export PYTHONPATH=src
```

Validar la configuración sin ejecutar Spark:

```bash
python -m saas_pipeline.cli \
  --env dev \
  --tenant gt \
  --validate-only
```

Ejecutar un tenant:

```bash
python -m saas_pipeline.cli \
  --env dev \
  --tenant gt \
  --start-date 2025-01-01 \
  --end-date 2025-06-30
```

Ejecutar todos los tenants:

```bash
python -m saas_pipeline.cli \
  --env dev \
  --tenant all \
  --start-date 2025-01-01 \
  --end-date 2025-06-30
```

La ejecución multi-tenant utiliza una única sesión Spark y
procesa cada país mediante su configuración correspondiente.

## 9. Resultados generados

Ejemplos de rutas para el tenant `gt` en ambiente `dev`:

```text
data/bronze/gt/deliveries/
data/silver/gt/dim_materials/
data/silver/gt/fact_deliveries/
data/gold/gt/daily_metrics_by_delivery_type/
data/bronze_quarantine/gt/deliveries/
data/silver_quarantine/gt/fact_deliveries/
data/shared/quality_logs/
```

Todos los resultados persistidos utilizan formato Delta Lake.

### Tabla Gold

Granularidad:

`tenant_id + fecha_proceso + tipo_entrega`

Métricas:

- `total_units`: suma de cantidades normalizadas a ST.
- `total_revenue`: cantidad ST multiplicada por el precio
  de la transacción.
- `active_routes`: cantidad de rutas distintas.
- `active_transports`: cantidad de transportes distintos.

## 10. Calidad e idempotencia

El pipeline incorpora controles sobre datos de negocio,
claves de MERGE y resultados de Silver.

Las anomalías se separan en cuarentena y los controles
ejecutados se registran en `quality_logs`.

La estrategia de reprocesamiento depende de la capa:

- Bronze: reemplazo controlado por rango de fechas.
- Silver fact_deliveries: MERGE por clave de negocio.
- Silver dim_materials: MERGE por material y valid_from.
- Gold: recálculo y reemplazo de las fechas procesadas.

El enriquecimiento de materiales utiliza la versión histórica
vigente en la fecha de cada entrega, no simplemente la versión
marcada como actual.

## 11. Pruebas y calidad del código

Ejecutar las pruebas:

```bash
PYTHONPATH=src python -m pytest -q
```

Ejecutar el linter:

```bash
ruff check src tests
ruff check mentoring/good_code.py
```

El archivo `mentoring/bad_code.py` contiene intencionalmente
el código original del Anexo A y se conserva sin modificaciones.

El workflow `.github/workflows/ci.yml` ejecuta automáticamente
en push y pull request:

- Validación con Ruff.
- Validación sintáctica y estructural de los YAML.
- Pruebas automatizadas con Pytest.

## 12. Onboarding de un tenant nuevo

Para incorporar un tenant adicional:

1. Crear su configuración en `config/tenants/`.
2. Registrar su identificador en los tenants admitidos
   por `config.py` y la CLI.
3. Asegurar que los datos RAW incluyan su código de país.
4. Validar la configuración del nuevo tenant.
5. Ejecutar el pipeline y verificar sus tablas Delta.
6. Revisar los resultados de calidad y cuarentena.

En un despliegue sobre Databricks también sería necesario
crear los schemas correspondientes en Unity Catalog,
las ubicaciones de ADLS y los permisos de acceso.

Documentación relacionada:

- [Onboarding de tenants](docs/onboarding-tenant.md)
- [Infraestructura Terraform](docs/infra.md)

## 13. Qué dejé fuera y por qué

### Infraestructura cloud real

El alcance utiliza Spark y Delta Lake localmente.
No se aprovisionaron recursos reales en Azure Databricks,
ADLS Gen2 o Unity Catalog.

Se incluye un snippet Terraform ilustrativo en `docs/infra.md`.

### Ingesta streaming

No se implementó Auto Loader ni un consumidor de streaming.
El origen proporcionado consiste en archivos CSV estáticos,
por lo que se priorizó un pipeline batch reproducible.

### Dashboard y segunda tabla Gold

Se desarrolló la tabla Gold solicitada como alcance base.
Los dashboards y agregaciones adicionales son extensiones
posibles, pero no necesarios para demostrar la arquitectura.

### Automatización completa del onboarding cloud

Los tenants soportados se gestionan mediante configuración,
pero el alta de uno nuevo requiere registrar su identificador
en las validaciones de la aplicación.

La creación automática de recursos cloud y permisos queda
planteada como evolución del módulo Terraform.

### Pruebas de integración cloud

Las pruebas automatizadas verifican la lógica local del
pipeline. No se realizaron pruebas contra un workspace
real de Databricks ni servicios productivos de Azure.

## 14. Decisiones y revisión técnica

- [Observaciones de arquitectura](docs/observations.md)
- [Revisión de código y mentoring](mentoring/code_review.md)
- [Refactor del código junior](mentoring/good_code.py)

El ejercicio de mentoring se mantiene independiente del
pipeline principal para mostrar claramente los cambios
aplicados sobre el código original del Anexo A.
