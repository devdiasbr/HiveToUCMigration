# Databricks notebook source

# MAGIC %md
# MAGIC # 02.01 - Migrar Tabelas Delta MANAGED (DEEP CLONE)
# MAGIC
# MAGIC Migra tabelas Delta Managed do Hive Metastore para o Unity Catalog
# MAGIC utilizando `DEEP CLONE`, que copia fisicamente os dados e preserva:
# MAGIC - Histórico do Delta Log completo
# MAGIC - Schema (incluindo constraints)
# MAGIC - Propriedades da tabela
# MAGIC - Particionamento
# MAGIC
# MAGIC **Estratégia:**
# MAGIC ```sql
# MAGIC CREATE OR REPLACE TABLE <catalog>.<schema>.<table>
# MAGIC DEEP CLONE hive_metastore.<database>.<table>
# MAGIC [LOCATION '<uc_managed_path>']
# MAGIC ```
# MAGIC
# MAGIC **Nota:** O DEEP CLONE cria uma cópia independente. Após validação,
# MAGIC a tabela original no Hive pode ser removida ou mantida como fallback.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros de Configuração

# COMMAND ----------

dbutils.widgets.text("uc_catalog_name",     "serasa_prod",     "Nome do Catálogo UC")
dbutils.widgets.text("inventory_database",  "migration_audit", "Database com o inventário")
dbutils.widgets.text("databases_filter",    "",                "Databases a migrar (vazio=todos, ex: 'db1,db2')")
dbutils.widgets.text("tables_filter",       "",                "Tabelas a migrar (vazio=todas, ex: 'tb1,tb2')")
dbutils.widgets.text("dry_run",             "true",            "dry_run=true para simular")
dbutils.widgets.text("parallelism",         "4",               "Grau de paralelismo (threads)")

uc_catalog_name    = dbutils.widgets.get("uc_catalog_name")
inventory_database = dbutils.widgets.get("inventory_database")
databases_filter   = dbutils.widgets.get("databases_filter")
tables_filter      = dbutils.widgets.get("tables_filter")
dry_run            = dbutils.widgets.get("dry_run").lower() == "true"
parallelism        = int(dbutils.widgets.get("parallelism"))

print(f"Catálogo UC:    {uc_catalog_name}")
print(f"Dry Run:        {dry_run}")
print(f"Paralelismo:    {parallelism}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregar Lista de Tabelas a Migrar

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import Row

# Carrega inventário e filtra tabelas Delta MANAGED
inventory_df = spark.table(f"`{inventory_database}`.`hive_inventory`")
schema_map_df = spark.table(f"`{inventory_database}`.`schema_mapping`")

# Join para obter nome do schema UC correspondente
tables_to_migrate = (
    inventory_df
    .filter(col("object_type") == "MANAGED")
    .filter(col("is_delta") == True)
    .join(schema_map_df, inventory_df.database_name == schema_map_df.hive_database, "left")
    .select(
        inventory_df.database_name.alias("hive_db"),
        inventory_df.table_name,
        inventory_df.table_location,
        inventory_df.estimated_rows,
        inventory_df.estimated_bytes,
        schema_map_df.uc_schema,
        schema_map_df.uc_catalog
    )
)

# Aplica filtros se informados
if databases_filter.strip():
    db_list = [d.strip() for d in databases_filter.split(",")]
    tables_to_migrate = tables_to_migrate.filter(col("hive_db").isin(db_list))

if tables_filter.strip():
    tbl_list = [t.strip() for t in tables_filter.split(",")]
    tables_to_migrate = tables_to_migrate.filter(col("table_name").isin(tbl_list))

tables_list = tables_to_migrate.collect()
print(f"Tabelas Delta MANAGED a migrar: {len(tables_list)}")
display(tables_to_migrate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Executar DEEP CLONE

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import traceback

migration_results = []

def clone_table(row):
    hive_db    = row["hive_db"]
    table_name = row["table_name"]
    uc_schema  = row["uc_schema"] or hive_db  # fallback se mapeamento falhar
    uc_catalog = row["uc_catalog"] or uc_catalog_name

    source = f"hive_metastore.`{hive_db}`.`{table_name}`"
    target = f"`{uc_catalog}`.`{uc_schema}`.`{table_name}`"

    start_time = datetime.now()

    try:
        if dry_run:
            result = {
                "hive_table":      f"{hive_db}.{table_name}",
                "uc_table":        f"{uc_catalog}.{uc_schema}.{table_name}",
                "status":          "DRY_RUN",
                "duration_sec":    0,
                "rows_cloned":     row["estimated_rows"],
                "bytes_cloned":    row["estimated_bytes"],
                "error_message":   None,
                "migrated_at":     start_time.isoformat(),
            }
            print(f"[DRY RUN] DEEP CLONE {source} → {target}")
            return result

        # Executa DEEP CLONE
        clone_sql = f"CREATE OR REPLACE TABLE {target} DEEP CLONE {source}"
        clone_result = spark.sql(clone_sql).collect()

        # Extrai métricas do clone
        rows_cloned  = 0
        bytes_cloned = 0
        if clone_result:
            r = clone_result[0].asDict()
            rows_cloned  = r.get("num_source_rows_copied",      r.get("numFilesAdded", 0))
            bytes_cloned = r.get("operation_metrics", {}).get("numBytes", 0) if isinstance(r.get("operation_metrics"), dict) else 0

        duration = (datetime.now() - start_time).total_seconds()
        print(f"  OK: {source} → {target} ({duration:.1f}s)")

        return {
            "hive_table":    f"{hive_db}.{table_name}",
            "uc_table":      f"{uc_catalog}.{uc_schema}.{table_name}",
            "status":        "SUCCESS",
            "duration_sec":  duration,
            "rows_cloned":   rows_cloned,
            "bytes_cloned":  bytes_cloned,
            "error_message": None,
            "migrated_at":   start_time.isoformat(),
        }

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        error_msg = f"{type(e).__name__}: {str(e)[:500]}"
        print(f"  ERRO: {source} → {target}: {error_msg}")
        return {
            "hive_table":    f"{hive_db}.{table_name}",
            "uc_table":      f"{uc_catalog}.{uc_schema}.{table_name}",
            "status":        "ERROR",
            "duration_sec":  duration,
            "rows_cloned":   0,
            "bytes_cloned":  0,
            "error_message": error_msg,
            "migrated_at":   start_time.isoformat(),
        }

# Execução com paralelismo
print(f"\nIniciando migração com paralelismo={parallelism}...")
print(f"Total: {len(tables_list)} tabelas\n")

if parallelism > 1:
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = {executor.submit(clone_table, row): row for row in tables_list}
        for future in as_completed(futures):
            migration_results.append(future.result())
else:
    for row in tables_list:
        migration_results.append(clone_table(row))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Resumo da Migração

# COMMAND ----------

success = [r for r in migration_results if r["status"] == "SUCCESS"]
errors  = [r for r in migration_results if r["status"] == "ERROR"]
dry_runs = [r for r in migration_results if r["status"] == "DRY_RUN"]

print("=" * 60)
print("RESUMO DA MIGRAÇÃO - TABELAS MANAGED")
print("=" * 60)
print(f"  Sucesso:   {len(success)}")
print(f"  Erro:      {len(errors)}")
print(f"  Dry Run:   {len(dry_runs)}")

if success:
    total_secs  = sum(r["duration_sec"] for r in success)
    total_rows  = sum(r.get("rows_cloned", 0) or 0 for r in success)
    print(f"\n  Tempo total: {total_secs:.1f}s")
    print(f"  Linhas migradas: {total_rows:,}")

if errors:
    print(f"\n--- ERROS ---")
    for e in errors:
        print(f"  {e['hive_table']} → {e['uc_table']}")
        print(f"    {e['error_message']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Salvar Log de Migração

# COMMAND ----------

results_df = spark.createDataFrame(migration_results)
display(results_df)

(results_df
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"`{inventory_database}`.`migration_log`"))

print(f"Log salvo em: {inventory_database}.migration_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificar Tabelas Migradas no UC

# COMMAND ----------

if not dry_run and success:
    print("Verificando tabelas criadas no UC:")
    for r in success[:10]:  # Amostra das primeiras 10
        try:
            uc_table = r["uc_table"]
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM {uc_table}").collect()[0]["cnt"]
            print(f"  OK: {uc_table} → {count:,} linhas")
        except Exception as e:
            print(f"  ERRO ao verificar {uc_table}: {e}")

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
    "total":          len(migration_results),
    "success":        len(success),
    "errors":         len(errors),
    "dry_run":        len(dry_runs),
    "error_tables":   [e["hive_table"] for e in errors]
}))
