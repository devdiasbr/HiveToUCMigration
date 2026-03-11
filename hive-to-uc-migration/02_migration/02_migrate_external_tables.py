# Databricks notebook source

# MAGIC %md
# MAGIC # 02.02 - Migrar Tabelas EXTERNAL (Delta e Não-Delta)
# MAGIC
# MAGIC Migra tabelas EXTERNAL do Hive para o Unity Catalog.
# MAGIC
# MAGIC **Estratégias por tipo:**
# MAGIC
# MAGIC | Formato | Estratégia | Notas |
# MAGIC |---------|-----------|-------|
# MAGIC | Delta External | `CREATE TABLE ... LOCATION` | Registra no UC sem mover dados |
# MAGIC | Parquet External | `CTAS` com conversão Delta | Converte e registra no UC |
# MAGIC | ORC / Avro / CSV | `CTAS` com conversão Delta | Lê no Hive, escreve Delta no UC |
# MAGIC
# MAGIC **Importante:** Para tabelas Delta External, os dados ficam no path original.
# MAGIC O UC passa a gerenciar os metadados sem mover os arquivos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros de Configuração

# COMMAND ----------

dbutils.widgets.text("uc_catalog_name",      "serasa_prod",     "Nome do Catálogo UC")
dbutils.widgets.text("inventory_database",   "migration_audit", "Database com o inventário")
dbutils.widgets.text("databases_filter",     "",                "Databases a migrar (vazio=todos)")
dbutils.widgets.text("tables_filter",        "",                "Tabelas a migrar (vazio=todas)")
dbutils.widgets.text("convert_non_delta",    "true",            "Converter não-Delta para Delta?")
dbutils.widgets.text("conversion_base_path", "",                "Path base para tabelas convertidas (ex: abfss://...)")
dbutils.widgets.text("dry_run",              "true",            "dry_run=true para simular")

uc_catalog_name      = dbutils.widgets.get("uc_catalog_name")
inventory_database   = dbutils.widgets.get("inventory_database")
databases_filter     = dbutils.widgets.get("databases_filter")
tables_filter        = dbutils.widgets.get("tables_filter")
convert_non_delta    = dbutils.widgets.get("convert_non_delta").lower() == "true"
conversion_base_path = dbutils.widgets.get("conversion_base_path").rstrip("/")
dry_run              = dbutils.widgets.get("dry_run").lower() == "true"

print(f"Catálogo UC:          {uc_catalog_name}")
print(f"Converter não-Delta:  {convert_non_delta}")
print(f"Path conversão:       {conversion_base_path or '(mesmo path da tabela original)'}")
print(f"Dry Run:              {dry_run}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregar Tabelas EXTERNAL do Inventário

# COMMAND ----------

from pyspark.sql.functions import col
from datetime import datetime

inventory_df  = spark.table(f"`{inventory_database}`.`hive_inventory`")
schema_map_df = spark.table(f"`{inventory_database}`.`schema_mapping`")

tables_df = (
    inventory_df
    .filter(col("object_type") == "EXTERNAL")
    .join(schema_map_df, inventory_df.database_name == schema_map_df.hive_database, "left")
    .select(
        inventory_df.database_name.alias("hive_db"),
        inventory_df.table_name,
        inventory_df.table_location,
        inventory_df.data_provider,
        inventory_df.is_delta,
        inventory_df.has_partitions,
        inventory_df.partition_columns,
        inventory_df.estimated_rows,
        inventory_df.estimated_bytes,
        schema_map_df.uc_schema,
    )
)

if databases_filter.strip():
    tables_df = tables_df.filter(col("hive_db").isin([d.strip() for d in databases_filter.split(",")]))

if tables_filter.strip():
    tables_df = tables_df.filter(col("table_name").isin([t.strip() for t in tables_filter.split(",")]))

tables_list = tables_df.collect()
delta_ext    = [r for r in tables_list if r["is_delta"]]
non_delta    = [r for r in tables_list if not r["is_delta"]]

print(f"Total tabelas EXTERNAL: {len(tables_list)}")
print(f"  Delta External:       {len(delta_ext)}")
print(f"  Não-Delta (conversão):{len(non_delta)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Migrar Tabelas Delta EXTERNAL
# MAGIC
# MAGIC Registra as tabelas no UC apontando para o mesmo path — sem mover dados.

# COMMAND ----------

def execute_sql(sql_stmt, dry_run=True, description=""):
    if dry_run:
        print(f"[DRY RUN] {description}")
        return None
    else:
        print(f"[EXEC] {description}")
        return spark.sql(sql_stmt)

migration_results = []

print("=== MIGRANDO TABELAS DELTA EXTERNAL ===\n")

for row in delta_ext:
    hive_db    = row["hive_db"]
    table_name = row["table_name"]
    location   = row["table_location"]
    uc_schema  = row["uc_schema"] or hive_db

    source = f"hive_metastore.`{hive_db}`.`{table_name}`"
    target = f"`{uc_catalog_name}`.`{uc_schema}`.`{table_name}`"

    start_time = datetime.now()

    try:
        # Obtém o DDL da tabela original para preservar schema completo
        if not dry_run:
            original_ddl = spark.sql(f"SHOW CREATE TABLE {source}").collect()[0][0]
        else:
            original_ddl = f"-- DDL da tabela {source} (dry_run)"

        # Para tabelas Delta External: registra no UC no mesmo location
        # Usa SYNC para garantir que o metastore UC reflita a tabela exatamente
        register_sql = f"""
            CREATE TABLE IF NOT EXISTS {target}
            USING DELTA
            LOCATION '{location}'
        """

        execute_sql(
            register_sql.strip(),
            dry_run=dry_run,
            description=f"Registrar {source} → {target} (LOCATION: {location})"
        )

        # Após registrar, sincroniza as propriedades
        if not dry_run:
            spark.sql(f"MSCK REPAIR TABLE {target}")

        duration = (datetime.now() - start_time).total_seconds()
        status   = "DRY_RUN" if dry_run else "SUCCESS"
        print(f"  {status}: {hive_db}.{table_name} → {uc_schema}.{table_name}")

        migration_results.append({
            "hive_table":    f"{hive_db}.{table_name}",
            "uc_table":      f"{uc_catalog_name}.{uc_schema}.{table_name}",
            "strategy":      "REGISTER_DELTA_EXTERNAL",
            "status":        status,
            "duration_sec":  duration,
            "error_message": None,
            "migrated_at":   start_time.isoformat(),
        })

    except Exception as e:
        duration  = (datetime.now() - start_time).total_seconds()
        error_msg = f"{type(e).__name__}: {str(e)[:500]}"
        print(f"  ERRO: {hive_db}.{table_name}: {error_msg}")
        migration_results.append({
            "hive_table":    f"{hive_db}.{table_name}",
            "uc_table":      f"{uc_catalog_name}.{uc_schema}.{table_name}",
            "strategy":      "REGISTER_DELTA_EXTERNAL",
            "status":        "ERROR",
            "duration_sec":  duration,
            "error_message": error_msg,
            "migrated_at":   start_time.isoformat(),
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Migrar Tabelas Não-Delta (CTAS com Conversão para Delta)

# COMMAND ----------

if not convert_non_delta:
    print("Conversão de não-Delta desativada (convert_non_delta=false). Pulando.")
else:
    print("=== MIGRANDO TABELAS NÃO-DELTA (CTAS → DELTA) ===\n")

    for row in non_delta:
        hive_db    = row["hive_db"]
        table_name = row["table_name"]
        location   = row["table_location"]
        provider   = row["data_provider"]
        uc_schema  = row["uc_schema"] or hive_db

        source = f"hive_metastore.`{hive_db}`.`{table_name}`"
        target = f"`{uc_catalog_name}`.`{uc_schema}`.`{table_name}`"

        # Define path de destino para a versão Delta
        if conversion_base_path:
            target_location = f"{conversion_base_path}/{uc_schema}/{table_name}"
        else:
            target_location = None  # UC managed location

        start_time = datetime.now()

        try:
            # Obtém schema da tabela original
            if not dry_run:
                schema_rows = spark.sql(f"DESCRIBE TABLE `{hive_db}`.`{table_name}`").collect()
                partition_cols = [r["col_name"] for r in schema_rows
                                  if r["col_name"].startswith("# Partition")]
            else:
                partition_cols = []

            if target_location:
                ctas_sql = f"""
                    CREATE OR REPLACE TABLE {target}
                    USING DELTA
                    LOCATION '{target_location}'
                    AS SELECT * FROM {source}
                """
            else:
                ctas_sql = f"""
                    CREATE OR REPLACE TABLE {target}
                    USING DELTA
                    AS SELECT * FROM {source}
                """

            print(f"  {provider} → Delta: {hive_db}.{table_name}")
            execute_sql(
                ctas_sql.strip(),
                dry_run=dry_run,
                description=f"CTAS {source} → {target} (formato: {provider})"
            )

            # Otimiza tabela recém-criada
            if not dry_run:
                spark.sql(f"OPTIMIZE {target}")

            duration = (datetime.now() - start_time).total_seconds()
            status   = "DRY_RUN" if dry_run else "SUCCESS"

            migration_results.append({
                "hive_table":    f"{hive_db}.{table_name}",
                "uc_table":      f"{uc_catalog_name}.{uc_schema}.{table_name}",
                "strategy":      f"CTAS_FROM_{provider.upper()}",
                "status":        status,
                "duration_sec":  duration,
                "error_message": None,
                "migrated_at":   start_time.isoformat(),
            })

        except Exception as e:
            duration  = (datetime.now() - start_time).total_seconds()
            error_msg = f"{type(e).__name__}: {str(e)[:500]}"
            print(f"  ERRO: {hive_db}.{table_name}: {error_msg}")
            migration_results.append({
                "hive_table":    f"{hive_db}.{table_name}",
                "uc_table":      f"{uc_catalog_name}.{uc_schema}.{table_name}",
                "strategy":      f"CTAS_FROM_{provider.upper()}",
                "status":        "ERROR",
                "duration_sec":  duration,
                "error_message": error_msg,
                "migrated_at":   start_time.isoformat(),
            })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resumo e Log

# COMMAND ----------

success  = [r for r in migration_results if r["status"] in ("SUCCESS",)]
errors   = [r for r in migration_results if r["status"] == "ERROR"]
dry_runs = [r for r in migration_results if r["status"] == "DRY_RUN"]

print("=" * 60)
print("RESUMO - TABELAS EXTERNAL")
print("=" * 60)
print(f"  Sucesso:   {len(success)}")
print(f"  Erro:      {len(errors)}")
print(f"  Dry Run:   {len(dry_runs)}")

if errors:
    print("\n--- ERROS ---")
    for e in errors:
        print(f"  {e['hive_table']}: {e['error_message']}")

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
    "total":   len(migration_results),
    "success": len(success),
    "errors":  len(errors),
    "error_tables": [e["hive_table"] for e in errors]
}))
