
# Observaciones de arquitectura — Proyecto SAAS

## 1. Particionado de Bronze por tenant

**Observación:**
La arquitectura define aislamiento por tenant mediante rutas
independientes y también menciona particionar Bronze por
fecha_proceso y tenant_id.

**Implementación:**
Se utiliza una tabla Delta independiente por tenant:

data/bronze/<tenant>/deliveries/

Cada tabla está particionada físicamente por fecha_proceso.

**Justificación:**
La ruta ya proporciona aislamiento físico por tenant.
Agregar tenant_id como segunda partición dentro de una
tabla exclusiva para ese tenant podría generar redundancia.

**Trade-off:**
La implementación sigue la estructura de rutas solicitada,
pero no utiliza dos columnas físicas de particionamiento.
Si se requiere estrictamente particionar por ambas columnas,
será necesario ajustar la estrategia de almacenamiento.

## 2. Cuarentena de fechas inválidas

**Observación:**
Los registros con fecha_proceso nula o inválida no pueden
asignarse a una partición de fecha ni asociarse con
seguridad a un rango de procesamiento.

**Implementación:**
Los registros se conservan en una tabla Delta de
bronze_quarantine por tenant, incluyendo el motivo del error.

La cuarentena se reconstruye desde el CSV completo del
tenant en cada ejecución, incluso en reprocesos parciales.

**Justificación:**
Esto evita perder registros y duplicarlos al reejecutar
la ingesta del mismo archivo de origen.

**Trade-off:**
Esta estrategia supone que el CSV disponible representa
el conjunto completo de datos de origen. Si la ingesta
se vuelve incremental, será necesario utilizar una
estrategia de MERGE o identificación de lotes para
preservar el historial de cuarentena.
