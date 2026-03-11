# Databricks notebook source

# MAGIC %md
# MAGIC # 03.01 - Validar Schema (Hive vs Unity Catalog)
# MAGIC
# MAGIC Compara os schemas das tabelas migradas entre o Hive Metastore e o UC,
# MAGIC verificando:
# MAGIC - Número de colunas
# MAGIC - Nomes e tipos das colunas
# MAGIC - Ordem das colunas
# MAGIC - Particionamento
# MAGIC - Constraints (se aplicável)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

dbutils.widgets.text("uc_catalog_name",    "serasa_prod",     "Nome do Catálogo UC")
dbutils.widgets.text("inventory_database", "migration_audit", "Database com o inventário")
dbutils.widgets.text("databases_filter",   "",                "Databases a validar (vazio=todos)")
dbutils.widgets.text("fail_on_mismatch",   "false",           "Falhar se encontrar diferenças?")

uc_catalog_name    = dbutils.widgets.get("uc_catalog_name")
inventory_database = dbutils.widgets.get("inventory_database")
databases_filter   = dbutils.widgets.get("databases_filter")
fail_on_mismatch   = dbutils.widgets.get("fail_on_mismatch").lower() == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregar Lista de Tabelas Migradas

# COMMAND ----------

from pyspark.sql.functions import col
from datetime import datetime

inventory_df  = spark.table(f"`{inventory_database}`.`hive_inventory`")
schema_map_df = spark.table(f"`{inventory_database}`.`schema_mapping`")

# Filtra apenas tabelas (não views) que foram migradas
tables_df = (
    inventory_df
    .filter(col("object_type").isin(["MANAGED", "EXTERNAL"]))
    .join(schema_map_df, inventory_df.database_name == schema_map_df.hive_database, "left")
    .select(
        inventory_df.database_name.alias("hive_db"),
        inventory_df.table_name,
        schema_map_df.uc_schema,
    )
)

if databases_filter.strip():
    tables_df = tables_df.filter(
        col("hive_db").isin([d.strip() for d in databases_filter.split(",")])
    )

tables_list = tables_df.collect()
print(f"Tabelas a validar: {len(tables_list)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Comparar Schemas

# COMMAND ----------

def get_schema_info(table_ref):
    """Retorna dict {col_name: data_type} de uma tabela."""
    try:
        rows = spark.sql(f"DESCRIBE TABLE `{table_ref.replace('.', '`.`')}`").collect()
        schema = {}
        order  = []
        for i, r in enumerate(rows):
            col_name = r["col_name"].strip()
            data_type = r["data_type"].strip() if r["data_type"] else ""
            if col_name and not col_name.startswith("#") and data_type:
                schema[col_name] = data_type
                order.append(col_name)
        return schema, order, None
    except Exception as e:
        return None, None, str(e)

validation_results = []

for row in tables_list:
    hive_db    = row["hive_db"]
    table_name = row["table_name"]
    uc_schema  = row["uc_schema"] or hive_db

    hive_ref = f"hive_metastore.{hive_db}.{table_name}"
    uc_ref   = f"{uc_catalog_name}.{uc_schema}.{table_name}"

    # Obtém schemas
    hive_schema, hive_order, hive_err = get_schema_info(hive_ref)
    uc_schema_cols, uc_order, uc_err  = get_schema_info(uc_ref)

    if hive_err or uc_err:
        validation_results.append({
            "hive_table":     hive_ref,
            "uc_table":       uc_ref,
            "status":         "ERROR",
            "num_cols_hive":  0,
            "num_cols_uc":    0,
            "missing_cols":   "",
            "extra_cols":     "",
            "type_mismatches": "",
            "order_mismatch": False,
            "error_message":  f"Hive: {hive_err} | UC: {uc_err}",
            "validated_at":   datetime.now().isoformat(),
        })
        print(f"  ERRO: {table_name}: hive={hive_err} | uc={uc_err}")
        continue

    # Compara schemas
    hive_cols = set(hive_schema.keys())
    uc_cols   = set(uc_schema_cols.keys())

    missing_in_uc   = hive_cols - uc_cols   # Colunas no Hive mas não no UC
    extra_in_uc     = uc_cols - hive_cols    # Colunas no UC mas não no Hive

    # Verifica tipos de dados
    type_mismatches = {}
    for col_name in hive_cols & uc_cols:
        hive_type = hive_schema[col_name].lower().replace(" ", "")
        uc_type   = uc_schema_cols[col_name].lower().replace(" ", "")
        # Normaliza tipos equivalentes
        type_equivalences = {
            "int": "integer", "long": "bigint",
            "float": "real", "bool": "boolean",
        }
        hive_type_norm = type_equivalences.get(hive_type, hive_type)
        uc_type_norm   = type_equivalences.get(uc_type, uc_type)
        if hive_type_norm != uc_type_norm:
            type_mismatches[col_name] = f"hive={hive_type} / uc={uc_type}"

    # Verifica ordem das colunas
    common_cols   = [c for c in hive_order if c in uc_cols]
    uc_common     = [c for c in uc_order if c in hive_cols]
    order_mismatch = common_cols != uc_common

    # Status geral
    has_issues = bool(missing_in_uc or extra_in_uc or type_mismatches or order_mismatch)
    status     = "MISMATCH" if has_issues else "OK"

    if has_issues:
        print(f"  DIFF: {table_name}")
        if missing_in_uc:
            print(f"    Colunas ausentes no UC: {missing_in_uc}")
        if type_mismatches:
            print(f"    Tipos diferentes: {type_mismatches}")
    else:
        print(f"  OK:   {table_name} ({len(hive_cols)} colunas)")

    validation_results.append({
        "hive_table":      hive_ref,
        "uc_table":        uc_ref,
        "status":          status,
        "num_cols_hive":   len(hive_schema),
        "num_cols_uc":     len(uc_schema_cols),
        "missing_cols":    str(sorted(missing_in_uc)),
        "extra_cols":      str(sorted(extra_in_uc)),
        "type_mismatches": str(type_mismatches),
        "order_mismatch":  order_mismatch,
        "error_message":   None,
        "validated_at":    datetime.now().isoformat(),
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Resumo da Validação de Schema

# COMMAND ----------

ok_count       = len([r for r in validation_results if r["status"] == "OK"])
mismatch_count = len([r for r in validation_results if r["status"] == "MISMATCH"])
error_count    = len([r for r in validation_results if r["status"] == "ERROR"])

print("=" * 60)
print("RESUMO - VALIDAÇÃO DE SCHEMA")
print("=" * 60)
print(f"  OK:       {ok_count}")
print(f"  Diff:     {mismatch_count}")
print(f"  Erro:     {error_count}")

# Exibe mismatches
mismatches = [r for r in validation_results if r["status"] == "MISMATCH"]
if mismatches:
    print("\n--- Tabelas com Diferenças de Schema ---")
    results_df = spark.createDataFrame(validation_results)
    display(results_df.filter(col("status") == "MISMATCH"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Salvar Resultados de Validação

# COMMAND ----------

results_df = spark.createDataFrame(validation_results)
display(results_df)

(results_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{inventory_database}`.`validation_schema_report`"))

print(f"Relatório salvo em: {inventory_database}.validation_schema_report")

# COMMAND ----------

# Falha se configurado e há diferenças
if fail_on_mismatch and (mismatch_count > 0 or error_count > 0):
    raise Exception(
        f"Validação de schema falhou: {mismatch_count} diferenças, {error_count} erros. "
        f"Consulte {inventory_database}.validation_schema_report para detalhes."
    )

import json
dbutils.notebook.exit(json.dumps({
    "total":     len(validation_results),
    "ok":        ok_count,
    "mismatch":  mismatch_count,
    "errors":    error_count,
    "report":    f"{inventory_database}.validation_schema_report"
}))
