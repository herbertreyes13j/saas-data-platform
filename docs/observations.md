
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

## 3. Onboarding y registro de nuevos tenants

**Observación:**
La arquitectura busca facilitar la incorporación de nuevas unidades
de negocio sin modificar el funcionamiento del pipeline.

Actualmente, la implementación utiliza una lista explícita de
tenants permitidos en config.py y en los argumentos de la CLI.

Esto funciona para los seis países del alcance, pero incorporar
uno nuevo también requiere modificar código.

**Implementación:**
Se mantuvo la lista explícita de tenants porque el alcance inicial
es conocido y esta validación evita ejecutar accidentalmente
países que todavía no han sido configurados.

Cada tenant dispone de su propio archivo YAML y de rutas
independientes para las capas de datos.

**Propuesta de mejora:**
En una siguiente iteración, los tenants podrían descubrirse
desde los archivos disponibles en config/tenants/.

El proceso de onboarding incluiría validación de configuración,
pruebas de aislamiento y aprovisionamiento de infraestructura
mediante Terraform.

Esto permitiría incorporar nuevos tenants con menos cambios
en el código de la aplicación.

**Trade-off:**
El registro explícito proporciona mayor control sobre cuáles
tenants están habilitados y facilita validar el alcance actual.

El descubrimiento dinámico reduce el mantenimiento manual,
pero requiere controles adicionales para evitar habilitar
tenants sin autorización o con configuraciones incompletas.

Por ese motivo, se conserva la implementación actual como MVP
y se propone automatizar el onboarding en una etapa posterior.
