# Pipeline Retail Gobernado con CI/CD (Databricks Asset Bundle)
**Universidad Latina de Costa Rica — Técnico en Ingeniería de Datos**  
**Estudiante:** Johan Ávalos Campos (`johan_avalos`)

---

## 📌 Descripción del Proyecto
Pipeline de ingeniería de datos de extremo a extremo (E2E) implementado con **Databricks Asset Bundles (DAB)** y **Lakeflow Declarative Pipelines (SDP)** bajo una **Arquitectura Medallón** en **Unity Catalog Serverless**.

El proyecto integra:
1. **Ingesta de Hechos (Bronze):** Dataset de retail del Databricks Marketplace (`databricks_simulated_retail_customer_data.v01.sales_orders`).
2. **Ingesta de Dimensión CDC con Auto Loader (Bronze):** Ingesta continua en JSON desde volúmenes de Unity Catalog.
3. **Calidad y Estandarización (Silver):** Reglas de calidad mediante *Expectations* (Warn, Drop, Fail) y gestión de cambios históricos con **SCD Tipo 2** (`APPLY CHANGES INTO`).
4. **Capa Analítica y Semántica (Gold & Metric View):** Vistas materializadas que agregan métricas de negocio por tier de membresía y ubicación geográfica.
5. **Orquestación (Lakeflow Jobs):** Workflow con ejecución de pipeline, evaluación condicional de estado (*If/Else*) y notificaciones por correo electrónico.
6. **CI/CD Automatizado:** GitHub Actions para ambientes diferenciados (`dev` y `prod`) dentro del mismo workspace.

---

## 🏗️ Estructura del Repositorio

```text
├── .github/
│   └── workflows/
│       ├── deploy_dev.yml          # CI/CD para branch 'dev' (Target Development)
│       └── deploy_prod.yml         # CI/CD para branch 'main' (Target Production)
├── data_cdc/
│   ├── batch_1_customers.json      # Lote 1 inicial (10 inserts)
│   └── batch_2_customers.json      # Lote 2 con insert, update y delete
├── resources/
│   ├── lakeflow_job.yml            # Definición del Job orquestador con tareas condicionales
│   └── pipeline_declarativo.yml    # Definición del pipeline SDP serverless
├── src/
│   └── transformations.sql         # Consultas SQL declarativas (Bronze -> Silver -> Gold -> Metric View)
├── databricks.yml                  # Configuración principal del Databricks Asset Bundle
└── README.md                       # Documentación técnica de despliegue
```

---

## 🚀 Guía de Despliegue y Ejecución

### 1. Prerrequisitos
- Databricks CLI v1.11.0 o superior instalado.
- Acceso a un Workspace de Databricks con Unity Catalog habilitado.
- Perfil de autenticación configurado (`databricks auth login`).

### 2. Validación Local del Bundle
Para verificar la sintaxis del bundle antes de desplegar:
```bash
# Validar en desarrollo
databricks bundle validate -t development

# Validar en producción
databricks bundle validate -t production
```

### 3. Despliegue Manual (CLI)
```bash
# Desplegar en ambiente dev
databricks bundle deploy -t development

# Desplegar en ambiente prod
databricks bundle deploy -t production
```

### 4. Ejecución del Job Orquestador
```bash
# Correr el Job en dev
databricks bundle run job_retail_orchestrator_johan_avalos -t development

# Correr el Job en prod
databricks bundle run job_retail_orchestrator_johan_avalos -t production
```

---

## 🔒 Variables y Entornos

| Variable | Descripción | Target Development | Target Production |
|---|---|---|---|
| `catalog_name` | Catálogo de destino Unity Catalog | `dab_lab_dev` | `dab_lab_prod` |
| `schema_name` | Esquema de destino | `default` | `default` |
| `cdc_volume_path` | Ruta del volumen para Auto Loader | `/Volumes/dab_lab_dev/default/customers_cdc_raw` | `/Volumes/dab_lab_prod/default/customers_cdc_raw` |
