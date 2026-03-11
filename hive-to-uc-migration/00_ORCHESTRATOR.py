# Databricks notebook source

# MAGIC %md
# MAGIC # Orquestrador - Migração Hive → Unity Catalog
# MAGIC
# MAGIC Notebook principal que coordena a execução de todas as fases da migração.
# MAGIC Execute cada fase individualmente ou use este notebook para rodar o pipeline completo.
# MAGIC
# MAGIC ## Fases do Pipeline
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────┐
# MAGIC │  FASE 0: ASSESSMENT                                  │
# MAGIC │  00_assessment/01_hive_inventory.py                  │
# MAGIC │  00_assessment/02_compatibility_check.py             │
# MAGIC └──────────────────────────────────────────────────────┘
# MAGIC                         ↓
# MAGIC ┌──────────────────────────────────────────────────────┐
# MAGIC │  FASE 1: SETUP                                       │
# MAGIC │  01_setup/01_create_catalog_schema.py                │
# MAGIC │  01_setup/02_external_locations.py                   │
# MAGIC └──────────────────────────────────────────────────────┘
# MAGIC                         ↓
# MAGIC ┌──────────────────────────────────────────────────────┐
# MAGIC │  FASE 2: MIGRAÇÃO                                    │
# MAGIC │  02_migration/01_migrate_managed_tables.py           │
# MAGIC │  02_migration/02_migrate_external_tables.py          │
# MAGIC │  02_migration/03_migrate_views.py                    │
# MAGIC │  02_migration/04_migrate_permissions.py              │
# MAGIC └──────────────────────────────────────────────────────┘
# MAGIC                         ↓
# MAGIC ┌──────────────────────────────────────────────────────┐
# MAGIC │  FASE 3: VALIDAÇÃO                                   │
# MAGIC │  03_validation/01_validate_schema.py                 │
# MAGIC │  03_validation/02_validate_data.py                   │
# MAGIC │  03_validation/03_validate_permissions.py            │
# MAGIC └──────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros Globais

# COMMAND ----------

# =====================================================================
# CONFIGURAÇÃO PRINCIPAL - Ajuste antes de executar
# =====================================================================

dbutils.widgets.text("uc_catalog_name",       "serasa_prod",     "Catálogo UC de destino")
dbutils.widgets.text("inventory_database",    "migration_audit", "Database de auditoria/inventário")
dbutils.widgets.text("databases_filter",      "",                "Databases a migrar (vazio=todos)")
dbutils.widgets.text("storage_credential",    "serasa-adls-cred","Nome da Storage Credential")
dbutils.widgets.text("managed_identity_id",   "",                "Azure Managed Identity ID")
dbutils.widgets.text("dry_run",               "true",            "dry_run=true para simular tudo")

# Fases a executar (true/false)
dbutils.widgets.text("run_phase_assessment",  "true",  "Executar Fase 0 - Assessment?")
dbutils.widgets.text("run_phase_setup",       "true",  "Executar Fase 1 - Setup?")
dbutils.widgets.text("run_phase_migration",   "true",  "Executar Fase 2 - Migração?")
dbutils.widgets.text("run_phase_validation",  "true",  "Executar Fase 3 - Validação?")

uc_catalog_name     = dbutils.widgets.get("uc_catalog_name")
inventory_database  = dbutils.widgets.get("inventory_database")
databases_filter    = dbutils.widgets.get("databases_filter")
storage_credential  = dbutils.widgets.get("storage_credential")
managed_identity_id = dbutils.widgets.get("managed_identity_id")
dry_run             = dbutils.widgets.get("dry_run")

run_assessment  = dbutils.widgets.get("run_phase_assessment").lower() == "true"
run_setup       = dbutils.widgets.get("run_phase_setup").lower() == "true"
run_migration   = dbutils.widgets.get("run_phase_migration").lower() == "true"
run_validation  = dbutils.widgets.get("run_phase_validation").lower() == "true"

# Path base deste notebook (ajuste para o path no Databricks workspace)
NOTEBOOK_BASE_PATH = "/Workspace/Repos/serasa/hive-to-uc-migration"

print("=" * 60)
print("CONFIGURAÇÃO DA MIGRAÇÃO")
print("=" * 60)
print(f"Catálogo UC:       {uc_catalog_name}")
print(f"Audit Database:    {inventory_database}")
print(f"Databases filter:  '{databases_filter}' (vazio=todos)")
print(f"Dry Run:           {dry_run}")
print(f"\nFases:")
print(f"  0-Assessment:    {run_assessment}")
print(f"  1-Setup:         {run_setup}")
print(f"  2-Migration:     {run_migration}")
print(f"  3-Validation:    {run_validation}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utilitário de Execução

# COMMAND ----------

import json
from datetime import datetime

pipeline_results = {}
pipeline_start   = datetime.now()

def run_notebook(relative_path, params, phase_name):
    """Executa um notebook filho e registra o resultado."""
    full_path = f"{NOTEBOOK_BASE_PATH}/{relative_path}"
    start     = datetime.now()
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Iniciando: {phase_name}")
    print(f"  Path: {full_path}")

    try:
        result = dbutils.notebook.run(
            path    = full_path,
            timeout = 3600,  # 1 hora por notebook
            arguments = params
        )
        duration = (datetime.now() - start).total_seconds()
        result_dict = json.loads(result) if result else {}
        print(f"  OK: {phase_name} ({duration:.1f}s)")
        pipeline_results[phase_name] = {
            "status":     "SUCCESS",
            "duration_s": duration,
            "result":     result_dict
        }
        return result_dict

    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        error_msg = str(e)[:500]
        print(f"  ERRO: {phase_name}: {error_msg}")
        pipeline_results[phase_name] = {
            "status":     "ERROR",
            "duration_s": duration,
            "error":      error_msg
        }
        raise  # Propaga o erro para interromper o pipeline

# Parâmetros comuns a todos os notebooks
common_params = {
    "uc_catalog_name":    uc_catalog_name,
    "inventory_database": inventory_database,
    "databases_filter":   databases_filter,
    "dry_run":            dry_run,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fase 0: Assessment

# COMMAND ----------

if run_assessment:
    print("\n" + "=" * 60)
    print("FASE 0: ASSESSMENT")
    print("=" * 60)

    run_notebook(
        "00_assessment/01_hive_inventory",
        {**common_params, "output_database": inventory_database, "output_table": "hive_inventory"},
        "00.01 - Inventário Hive"
    )

    run_notebook(
        "00_assessment/02_compatibility_check",
        {**common_params, "report_table": "compatibility_report"},
        "00.02 - Verificação de Compatibilidade"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fase 1: Setup

# COMMAND ----------

if run_setup:
    print("\n" + "=" * 60)
    print("FASE 1: SETUP")
    print("=" * 60)

    run_notebook(
        "01_setup/01_create_catalog_schema",
        {**common_params, "catalog_comment": "Catálogo Serasa - migrado do Hive Metastore"},
        "01.01 - Criar Catálogo e Schemas"
    )

    run_notebook(
        "01_setup/02_external_locations",
        {
            **common_params,
            "storage_credential_name": storage_credential,
            "managed_identity_id":     managed_identity_id,
        },
        "01.02 - External Locations"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fase 2: Migração

# COMMAND ----------

if run_migration:
    print("\n" + "=" * 60)
    print("FASE 2: MIGRAÇÃO")
    print("=" * 60)

    run_notebook(
        "02_migration/01_migrate_managed_tables",
        {**common_params, "parallelism": "4"},
        "02.01 - Migrar Tabelas Managed"
    )

    run_notebook(
        "02_migration/02_migrate_external_tables",
        {**common_params, "convert_non_delta": "true"},
        "02.02 - Migrar Tabelas External"
    )

    run_notebook(
        "02_migration/03_migrate_views",
        common_params,
        "02.03 - Migrar Views"
    )

    run_notebook(
        "02_migration/04_migrate_permissions",
        common_params,
        "02.04 - Migrar Permissões"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fase 3: Validação

# COMMAND ----------

if run_validation:
    print("\n" + "=" * 60)
    print("FASE 3: VALIDAÇÃO")
    print("=" * 60)

    run_notebook(
        "03_validation/01_validate_schema",
        {**common_params, "fail_on_mismatch": "false"},
        "03.01 - Validar Schema"
    )

    run_notebook(
        "03_validation/02_validate_data",
        {**common_params, "sample_fraction": "0.01", "fail_on_mismatch": "false"},
        "03.02 - Validar Dados"
    )

    run_notebook(
        "03_validation/03_validate_permissions",
        common_params,
        "03.03 - Validar Permissões"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo Final do Pipeline

# COMMAND ----------

total_duration = (datetime.now() - pipeline_start).total_seconds()

print("\n" + "=" * 70)
print("RESUMO FINAL DO PIPELINE DE MIGRAÇÃO")
print("=" * 70)
print(f"Duração total: {total_duration:.0f}s ({total_duration/60:.1f} min)")
print(f"Dry Run:       {dry_run}")
print()

success_count = 0
error_count   = 0

for phase, result in pipeline_results.items():
    status  = result["status"]
    dur     = result.get("duration_s", 0)
    symbol  = "OK" if status == "SUCCESS" else "ERRO"
    print(f"  [{symbol}] {phase} ({dur:.0f}s)")
    if status == "SUCCESS":
        success_count += 1
    else:
        error_count += 1
        if "error" in result:
            print(f"       → {result['error'][:100]}")

print(f"\nTotal: {success_count} OK, {error_count} ERRO(S)")

if dry_run == "true":
    print("\n" + "=" * 70)
    print("PIPELINE SIMULADO (dry_run=true) - Nenhum dado foi alterado.")
    print("Revise os logs e execute novamente com dry_run=false.")
    print("=" * 70)

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "dry_run":         dry_run,
    "total_phases":    len(pipeline_results),
    "success":         success_count,
    "errors":          error_count,
    "total_duration_s": total_duration,
    "phases":          {k: v["status"] for k, v in pipeline_results.items()}
}))
