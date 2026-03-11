# Databricks notebook source

# MAGIC %md
# MAGIC # 02.03 - Migrar Views
# MAGIC
# MAGIC Extrai o DDL das views do Hive Metastore, adapta as referências
# MAGIC para o novo namespace UC (`catalog.schema.table`) e recria no UC.
# MAGIC
# MAGIC **Processo:**
# MAGIC 1. Extrai DDL original da view (`SHOW CREATE TABLE`)
# MAGIC 2. Substitui referências `db.table` → `catalog.schema.table`
# MAGIC 3. Testa o DDL adaptado (sem criar)
# MAGIC 4. Cria a view no UC
# MAGIC
# MAGIC **Atenção:** Views complexas com subqueries, CTEs e funções
# MAGIC customizadas podem precisar de ajuste manual.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

dbutils.widgets.text("uc_catalog_name",    "serasa_prod",     "Nome do Catálogo UC")
dbutils.widgets.text("inventory_database", "migration_audit", "Database com o inventário")
dbutils.widgets.text("databases_filter",   "",                "Databases a migrar (vazio=todos)")
dbutils.widgets.text("dry_run",            "true",            "dry_run=true para simular")

uc_catalog_name    = dbutils.widgets.get("uc_catalog_name")
inventory_database = dbutils.widgets.get("inventory_database")
databases_filter   = dbutils.widgets.get("databases_filter")
dry_run            = dbutils.widgets.get("dry_run").lower() == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregar Views do Inventário

# COMMAND ----------

from pyspark.sql.functions import col
from datetime import datetime
import re

inventory_df  = spark.table(f"`{inventory_database}`.`hive_inventory`")
schema_map_df = spark.table(f"`{inventory_database}`.`schema_mapping`")

views_df = (
    inventory_df
    .filter(col("object_type") == "VIEW")
    .join(schema_map_df, inventory_df.database_name == schema_map_df.hive_database, "left")
    .select(
        inventory_df.database_name.alias("hive_db"),
        inventory_df.table_name,
        schema_map_df.uc_schema,
    )
)

if databases_filter.strip():
    views_df = views_df.filter(col("hive_db").isin([d.strip() for d in databases_filter.split(",")]))

views_list = views_df.collect()
print(f"Views a migrar: {len(views_list)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extrair e Adaptar DDLs

# COMMAND ----------

# Carrega mapeamento de schemas para substituição
schema_mapping = {
    row["hive_database"]: row["uc_schema"]
    for row in schema_map_df.collect()
}

def extract_view_ddl(hive_db, view_name):
    """Extrai o DDL SELECT da view (sem CREATE VIEW header)."""
    try:
        full_ddl = spark.sql(f"SHOW CREATE TABLE `{hive_db}`.`{view_name}`").collect()[0][0]
        # Extrai apenas o SELECT (após AS)
        match = re.search(r'\bAS\b\s*(.*)', full_ddl, re.DOTALL | re.IGNORECASE)
        if match:
            return full_ddl, match.group(1).strip()
        return full_ddl, full_ddl
    except Exception as e:
        return None, str(e)

def adapt_view_ddl(ddl, catalog_name, schema_mapping):
    """
    Substitui referências `db`.`table` e db.table no DDL
    para `catalog`.`schema`.`table`.
    """
    adapted = ddl

    # Pattern: `database`.`table` → `catalog`.`schema`.`table`
    def replace_backtick_ref(m):
        db  = m.group(1)
        tbl = m.group(2)
        schema = schema_mapping.get(db, db)
        return f"`{catalog_name}`.`{schema}`.`{tbl}`"

    adapted = re.sub(r'`([^`]+)`\.`([^`]+)`', replace_backtick_ref, adapted)

    # Pattern: database.table (sem backticks) → `catalog`.`schema`.`table`
    def replace_dotted_ref(m):
        db  = m.group(1)
        tbl = m.group(2)
        if db in schema_mapping:
            schema = schema_mapping[db]
            return f"`{catalog_name}`.`{schema}`.`{tbl}`"
        return m.group(0)  # Não substitui se não conhece o database

    adapted = re.sub(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b',
                     replace_dotted_ref, adapted)

    return adapted

# Extrai e adapta DDLs
view_ddls = []
for row in views_list:
    hive_db    = row["hive_db"]
    view_name  = row["table_name"]
    uc_schema  = row["uc_schema"] or hive_db

    full_ddl, select_body = extract_view_ddl(hive_db, view_name)

    if full_ddl is None:
        view_ddls.append({
            "hive_db":    hive_db,
            "view_name":  view_name,
            "uc_schema":  uc_schema,
            "original_ddl": None,
            "adapted_ddl":  None,
            "create_sql":   None,
            "error":        select_body  # contém mensagem de erro
        })
        continue

    adapted_select = adapt_view_ddl(select_body, uc_catalog_name, schema_mapping)

    # Monta o CREATE VIEW no UC
    create_view_sql = f"""CREATE OR REPLACE VIEW `{uc_catalog_name}`.`{uc_schema}`.`{view_name}`
AS
{adapted_select}"""

    view_ddls.append({
        "hive_db":      hive_db,
        "view_name":    view_name,
        "uc_schema":    uc_schema,
        "original_ddl": full_ddl,
        "adapted_ddl":  adapted_select,
        "create_sql":   create_view_sql,
        "error":        None
    })

print(f"DDLs extraídos: {len([v for v in view_ddls if v['create_sql']])}")
print(f"Erros na extração: {len([v for v in view_ddls if v['error']])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Exibir DDLs Adaptados para Revisão

# COMMAND ----------

# Mostra as views com suas DDLs para revisão antes de criar
print("=" * 70)
print("DDLs ADAPTADOS PARA REVISÃO:")
print("=" * 70)
for v in view_ddls:
    if v["error"]:
        print(f"\n[ERRO] {v['hive_db']}.{v['view_name']}: {v['error']}")
    else:
        print(f"\n--- {v['hive_db']}.{v['view_name']} → {uc_catalog_name}.{v['uc_schema']}.{v['view_name']} ---")
        print(v["create_sql"])
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Criar Views no UC

# COMMAND ----------

migration_results = []

for v in view_ddls:
    hive_db   = v["hive_db"]
    view_name = v["view_name"]
    uc_schema = v["uc_schema"]

    start_time = datetime.now()

    if v["error"]:
        migration_results.append({
            "hive_view":     f"{hive_db}.{view_name}",
            "uc_view":       f"{uc_catalog_name}.{uc_schema}.{view_name}",
            "status":        "EXTRACTION_ERROR",
            "duration_sec":  0,
            "error_message": v["error"],
            "migrated_at":   start_time.isoformat(),
        })
        continue

    try:
        if dry_run:
            # Em dry_run valida a sintaxe do SELECT sem criar
            # (cria uma view temporária para validar)
            test_sql = f"CREATE OR REPLACE TEMP VIEW _validate_{view_name} AS {v['adapted_ddl']}"
            try:
                spark.sql(test_sql)
                spark.sql(f"DROP VIEW IF EXISTS _validate_{view_name}")
                syntax_ok = True
                syntax_error = None
            except Exception as se:
                syntax_ok = False
                syntax_error = str(se)[:300]

            status = "DRY_RUN_OK" if syntax_ok else "DRY_RUN_SYNTAX_ERROR"
            print(f"  [{status}] {hive_db}.{view_name}")
            if syntax_error:
                print(f"    Erro de sintaxe: {syntax_error}")
        else:
            spark.sql(v["create_sql"])
            status = "SUCCESS"
            print(f"  [OK] {hive_db}.{view_name} → {uc_catalog_name}.{uc_schema}.{view_name}")

        migration_results.append({
            "hive_view":     f"{hive_db}.{view_name}",
            "uc_view":       f"{uc_catalog_name}.{uc_schema}.{view_name}",
            "status":        status,
            "duration_sec":  (datetime.now() - start_time).total_seconds(),
            "error_message": syntax_error if dry_run and not syntax_ok else None,
            "migrated_at":   start_time.isoformat(),
        })

    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)[:500]}"
        print(f"  [ERRO] {hive_db}.{view_name}: {error_msg}")
        migration_results.append({
            "hive_view":     f"{hive_db}.{view_name}",
            "uc_view":       f"{uc_catalog_name}.{uc_schema}.{view_name}",
            "status":        "ERROR",
            "duration_sec":  (datetime.now() - start_time).total_seconds(),
            "error_message": error_msg,
            "migrated_at":   start_time.isoformat(),
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resumo e Log

# COMMAND ----------

success   = [r for r in migration_results if "SUCCESS" in r["status"] or "DRY_RUN_OK" in r["status"]]
errors    = [r for r in migration_results if "ERROR" in r["status"]]

print("=" * 60)
print("RESUMO - VIEWS")
print("=" * 60)
print(f"  OK:    {len(success)}")
print(f"  Erro:  {len(errors)}")

if errors:
    print("\n--- Views com Erro (requerem ajuste manual) ---")
    for e in errors:
        print(f"  {e['hive_view']}: {e['error_message']}")

# Salva log
results_df = spark.createDataFrame(migration_results)
display(results_df)

(results_df
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"`{inventory_database}`.`migration_log`"))

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
    "total":        len(migration_results),
    "success":      len(success),
    "errors":       len(errors),
    "error_views":  [e["hive_view"] for e in errors]
}))
