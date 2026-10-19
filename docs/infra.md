# Infraestructura como código — Terraform

## Objetivo

Describir el aprovisionamiento necesario para incorporar un tenant
a la plataforma SAAS sobre Azure Databricks y ADLS Gen2.

En esta prueba, la infraestructura se simula mediante rutas locales.
El siguiente código es ilustrativo y no se despliega en Azure.

## Recursos por tenant

Para incorporar un tenant, Terraform permitiría:

1. Crear tres schemas en Unity Catalog: bronze, silver y gold.
2. Crear directorios separados para cada capa en ADLS Gen2.
3. Registrar external locations para gobernar el acceso a ADLS.
4. Crear un secret scope para la gestión de credenciales.
5. Aplicar permisos de acceso según los grupos autorizados.

Por ejemplo, para tenant_id = "gt" y environment = "dev":

- saas_dev.bronze_gt
- saas_dev.silver_gt
- saas_dev.gold_gt

Los datos se almacenarían bajo:

- bronze/gt/
- silver/gt/
- gold/gt/

## Snippet Terraform — módulo de onboarding

```hcl
variable "tenant_id" { type = string }
variable "environment" { type = string }
variable "storage_account_id" { type = string }
variable "storage_account_name" { type = string }
variable "filesystem_name" { type = string }
variable "storage_credential_name" { type = string }

locals {
  catalog_name = "saas_${var.environment}"
  layers       = toset(["bronze", "silver", "gold"])
}

resource "azurerm_storage_data_lake_gen2_path" "tenant" {
  for_each           = local.layers
  path               = "${each.value}/${var.tenant_id}"
  filesystem_name    = var.filesystem_name
  storage_account_id = var.storage_account_id
  resource           = "directory"
}

resource "databricks_schema" "tenant" {
  for_each     = local.layers
  catalog_name = local.catalog_name
  name         = "${each.value}_${var.tenant_id}"
  comment      = "SAAS tenant ${var.tenant_id}: ${each.value}"
}

resource "databricks_external_location" "tenant" {
  for_each = local.layers
  name     = "saas_${var.environment}_${each.value}_${var.tenant_id}"
  url      = "abfss://${var.filesystem_name}@${var.storage_account_name}.dfs.core.windows.net/${each.value}/${var.tenant_id}"
  credential_name = var.storage_credential_name
  depends_on      = [azurerm_storage_data_lake_gen2_path.tenant]
}

resource "databricks_secret_scope" "tenant" {
  name = "saas_${var.environment}_${var.tenant_id}"
}

output "tenant_schemas" {
  value = [for layer in local.layers : databricks_schema.tenant[layer].id]
}
```

## Supuestos y consideraciones

- Los providers de AzureRM y Databricks están configurados previamente.
- El catálogo `saas_<env>` ya existe en Unity Catalog.
- La cuenta ADLS Gen2 y su filesystem ya existen.
- Los directorios padre `bronze`, `silver` y `gold` ya existen.
- El storage credential de Unity Catalog ya está configurado.
- La identidad de despliegue dispone de los permisos necesarios.
- Los grants, ACL y asignaciones RBAC deben configurarse según
  los grupos autorizados antes de habilitar acceso al tenant.
- El secret scope no contiene secretos por sí mismo. En producción
  se recomienda integrarlo con Azure Key Vault.
- La identidad utilizada por Unity Catalog debe tener acceso
  al almacenamiento de Azure correspondiente.

## Relación con la implementación local

El pipeline desarrollado en esta prueba utiliza Delta Lake sobre
rutas locales, por ejemplo:

`data/silver/gt/fact_deliveries/`

En Databricks, la tabla correspondiente estaría gobernada mediante
Unity Catalog:

`saas_dev.silver_gt.fact_deliveries`

La estrategia multi-tenant se conserva, pero el almacenamiento y
los permisos pasan a ser gestionados por servicios cloud.

## Alcance

Este snippet demuestra cómo se automatizaría el onboarding.
No incluye despliegue real, configuración de identidades,
grants completos ni ejecución de terraform plan.

Estas integraciones quedan fuera del MVP porque el enunciado
solicita únicamente un ejemplo ilustrativo.
