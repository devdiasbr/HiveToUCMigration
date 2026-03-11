# Databricks notebook source

# MAGIC %md
# MAGIC # 03.02 - Validar Dados (Contagem e Amostragem)
# MAGIC
# MAGIC Valida a integridade dos dados migrados comparando:
# MAGIC - Contagem de linhas (Hive vs UC)
# MAGIC - Hash de amostras aleatórias
# MAGIC - Checksum de colunas numéricas (SUM, MIN, MAX)
# MAGIC - Comparação de partições

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

dbutils.widgets.text("uc_catalog_name",      "serasa_prod",     "Nome do Catálogo UC")
dbutils.widgets.text("inventory_database",   "migration_audit", "Database com o inventário")
dbutils.widgets.text("databases_filter",     "",                "Databases a validar (vazio=todos)")
dbutils.widgets.text("sample_fraction",      "0.01",            "Fração amostral para hash check (0.01=1%)")
dbutils.widgets.text("count_tolerance_pct",  "0",               "Tolerância de diferença em % na contagem")
dbutils.widgets.text("fail_on_mismatch",     "false",           "Falhar se encontrar diferenças?")

uc_catalog_name     = dbutils.widgets.get("uc_catalog_name")
inventory_database  = dbutils.widgets.get("inventory_database")
databases_filter    = dbutils.widgets.get("databases_filter")
sample_fraction     = float(dbutils.widgets.get("sample_fraction"))
count_tolerance_pct = float(dbutils.widgets.get("count_tolerance_pct"))
fail_on_mismatch    = dbutils.widgets.get("fail_on_mismatch").lower() == "true"

print(f"Catálogo UC:      {uc_catalog_name}")
print(f"Sample fraction:  {sample_fraction * 100:.1f}%")
print(f"Tolerância count: {count_tolerance_pct}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregar Lista de Tabelas

# COMMAND ----------

from pyspark.sql.functions import col, count, sum as spark_sum, min as spark_min, max as spark_max
from pyspark.sql.functions import hash as spark_hash, abs as spark_abs
from pyspark.sql import functions as F
from datetime import datetime

inventory_df  = spark.table(f"`{inventory_database}`.`hive_inventory`")
schema_map_df = spark.table(f"`{inventory_database}`.`schema_mapping`")

tables_df = (
    inventory_df
    .filter(col("object_type").isin(["MANAGED", "EXTERNAL"]))
    .join(schema_map_df, inventory_df.database_name == schema_map_df.hive_database, "left")
    .select(
        inventory_df.database_name.alias("hive_db"),
        inventory_df.table_name,
        inventory_df.has_partitions,
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
# MAGIC ## 2. Validação de Contagem de Linhas

# COMMAND ----------

count_results = []

print("Validando contagens...\n")

for row in tables_list:
    hive_db    = row["hive_db"]
    table_name = row["table_name"]
    uc_schema  = row["uc_schema"] or hive_db

    hive_ref = f"hive_metastore.`{hive_db}`.`{table_name}`"
    uc_ref   = f"`{uc_catalog_name}`.`{uc_schema}`.`{table_name}`"

    start = datetime.now()

    try:
        hive_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {hive_ref}").collect()[0]["cnt"]
        uc_count   = spark.sql(f"SELECT COUNT(*) as cnt FROM {uc_ref}").collect()[0]["cnt"]

        diff       = abs(hive_count - uc_count)
        diff_pct   = (diff / hive_count * 100) if hive_count > 0 else 0

        within_tolerance = diff_pct <= count_tolerance_pct
        status = "OK" if within_tolerance else "COUNT_MISMATCH"

        symbol = "OK" if status == "OK" else "DIFF"
        print(f"  [{symbol}] {table_name}: hive={hive_count:,} / uc={uc_count:,} "
              f"(diff={diff:,} / {diff_pct:.4f}%)")

    except Exception as e:
        hive_count = -1
        uc_count   = -1
        diff       = -1
        diff_pct   = -1
        status     = "ERROR"
        print(f"  [ERRO] {table_name}: {e}")

    count_results.append({
        "hive_table":    hive_ref,
        "uc_table":      uc_ref,
        "hive_count":    hive_count,
        "uc_count":      uc_count,
        "diff":          diff,
        "diff_pct":      round(diff_pct, 6),
        "status":        status,
        "validated_at":  start.isoformat(),
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validação por Checksum de Colunas Numéricas

# COMMAND ----------

checksum_results = []

print("\nValidando checksums de colunas numéricas...\n")

NUMERIC_TYPES = {"int", "integer", "long", "bigint", "double", "float", "decimal", "short", "tinyint"}

for row in tables_list:
    hive_db    = row["hive_db"]
    table_name = row["table_name"]
    uc_schema  = row["uc_schema"] or hive_db

    hive_ref = f"hive_metastore.`{hive_db}`.`{table_name}`"
    uc_ref   = f"`{uc_catalog_name}`.`{uc_schema}`.`{table_name}`"

    try:
        # Obtém colunas numéricas
        hive_df = spark.table(hive_ref)
        numeric_cols = [
            f.name for f in hive_df.schema.fields
            if any(t in str(f.dataType).lower() for t in NUMERIC_TYPES)
        ][:5]  # Limita às 5 primeiras para performance

        if not numeric_cols:
            continue

        # Calcula checksums
        agg_exprs = []
        for c in numeric_cols:
            agg_exprs.extend([
                F.sum(F.col(f"`{c}`")).alias(f"sum_{c}"),
                F.min(F.col(f"`{c}`")).alias(f"min_{c}"),
                F.max(F.col(f"`{c}`")).alias(f"max_{c}"),
            ])

        hive_checksum = spark.table(hive_ref).agg(*agg_exprs).collect()[0].asDict()
        uc_checksum   = spark.table(uc_ref).agg(*agg_exprs).collect()[0].asDict()

        # Compara
        mismatches = {}
        for key in hive_checksum:
            h_val = hive_checksum[key]
            u_val = uc_checksum.get(key)
            if h_val != u_val:
                mismatches[key] = f"hive={h_val} / uc={u_val}"

        status = "OK" if not mismatches else "CHECKSUM_MISMATCH"
        print(f"  [{status}] {table_name} (cols: {numeric_cols})")
        if mismatches:
            for k, v in mismatches.items():
                print(f"    DIFF: {k}: {v}")

        checksum_results.append({
            "hive_table":       hive_ref,
            "uc_table":         uc_ref,
            "checked_columns":  str(numeric_cols),
            "mismatches":       str(mismatches),
            "status":           status,
            "validated_at":     datetime.now().isoformat(),
        })

    except Exception as e:
        print(f"  [ERRO] {table_name}: {e}")
        checksum_results.append({
            "hive_table":       hive_ref,
            "uc_table":         uc_ref,
            "checked_columns":  "",
            "mismatches":       str(e)[:300],
            "status":           "ERROR",
            "validated_at":     datetime.now().isoformat(),
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validação de Partições

# COMMAND ----------

partition_results = []
partitioned_tables = [r for r in tables_list if r["has_partitions"]]

if partitioned_tables:
    print(f"\nValidando partições em {len(partitioned_tables)} tabelas...\n")

    for row in partitioned_tables:
        hive_db    = row["hive_db"]
        table_name = row["table_name"]
        uc_schema  = row["uc_schema"] or hive_db

        try:
            hive_parts = spark.sql(
                f"SHOW PARTITIONS hive_metastore.`{hive_db}`.`{table_name}`"
            ).count()
            uc_parts = spark.sql(
                f"SHOW PARTITIONS `{uc_catalog_name}`.`{uc_schema}`.`{table_name}`"
            ).count()

            status = "OK" if hive_parts == uc_parts else "PARTITION_MISMATCH"
            print(f"  [{status}] {table_name}: hive={hive_parts} / uc={uc_parts} partições")

            partition_results.append({
                "table":          table_name,
                "hive_partitions": hive_parts,
                "uc_partitions":  uc_parts,
                "status":         status,
            })

        except Exception as e:
            print(f"  [ERRO partições] {table_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resumo Final e Salvar Relatório

# COMMAND ----------

print("\n" + "=" * 60)
print("RESUMO - VALIDAÇÃO DE DADOS")
print("=" * 60)

count_ok       = len([r for r in count_results if r["status"] == "OK"])
count_diff     = len([r for r in count_results if r["status"] == "COUNT_MISMATCH"])
count_err      = len([r for r in count_results if r["status"] == "ERROR"])
checksum_ok    = len([r for r in checksum_results if r["status"] == "OK"])
checksum_diff  = len([r for r in checksum_results if r["status"] == "CHECKSUM_MISMATCH"])

print(f"\nContagem de linhas:")
print(f"  OK:       {count_ok}")
print(f"  Diff:     {count_diff}")
print(f"  Erro:     {count_err}")
print(f"\nChecksum numérico:")
print(f"  OK:       {checksum_ok}")
print(f"  Diff:     {checksum_diff}")

# Salva relatório de contagem
if count_results:
    counts_df = spark.createDataFrame(count_results)
    (counts_df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(f"`{inventory_database}`.`validation_count_report`"))

if checksum_results:
    checksum_df = spark.createDataFrame(checksum_results)
    (checksum_df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(f"`{inventory_database}`.`validation_checksum_report`"))

display(counts_df if count_results else spark.createDataFrame([]))

# COMMAND ----------

total_issues = count_diff + count_err + checksum_diff

if fail_on_mismatch and total_issues > 0:
    raise Exception(
        f"Validação de dados falhou: {count_diff} diferenças de contagem, "
        f"{checksum_diff} diferenças de checksum, {count_err} erros."
    )

import json
dbutils.notebook.exit(json.dumps({
    "tables_validated":  len(tables_list),
    "count_ok":          count_ok,
    "count_mismatch":    count_diff,
    "count_errors":      count_err,
    "checksum_ok":       checksum_ok,
    "checksum_mismatch": checksum_diff,
    "total_issues":      total_issues,
}))
