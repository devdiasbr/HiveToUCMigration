# Databricks notebook source

# MAGIC %md
# MAGIC # 00.01 - Inventário do Ambiente Hive Metastore
# MAGIC
# MAGIC Este notebook realiza o levantamento completo do ambiente Hive Metastore legado,
# MAGIC catalogando todos os databases, tabelas, views, propriedades e localização dos dados.
# MAGIC
# MAGIC **Pré-requisitos:**
# MAGIC - Cluster com acesso ao Hive Metastore legado
# MAGIC - Permissão de leitura em todos os databases/tabelas
# MAGIC
# MAGIC **Output:**
# MAGIC - DataFrame com inventário completo salvo como tabela Delta para consulta posterior

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros de Configuração

# COMMAND ----------

dbutils.widgets.text("output_database", "migration_audit", "Database de saída para o inventário")
dbutils.widgets.text("output_table", "hive_inventory", "Tabela de saída do inventário")
dbutils.widgets.text("databases_filter", "", "Databases a inventariar (vazio = todos, ex: 'db1,db2,db3')")

output_database = dbutils.widgets.get("output_database")
output_table    = dbutils.widgets.get("output_table")
databases_filter = dbutils.widgets.get("databases_filter")

print(f"Output: {output_database}.{output_table}")
print(f"Filtro databases: '{databases_filter}' (vazio = todos)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Listar todos os Databases no Hive Metastore

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime

# Lista todos os databases disponíveis
all_databases = [db.databaseName for db in spark.sql("SHOW DATABASES").collect()]

# Aplica filtro se informado
if databases_filter.strip():
    filter_list = [d.strip() for d in databases_filter.split(",")]
    databases_to_scan = [db for db in all_databases if db in filter_list]
    print(f"Databases filtrados ({len(databases_to_scan)}): {databases_to_scan}")
else:
    databases_to_scan = all_databases
    print(f"Total de databases encontrados: {len(databases_to_scan)}")

print("\nDatabases a inventariar:")
for db in databases_to_scan:
    print(f"  - {db}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Inventariar Tabelas e Views

# COMMAND ----------

import json

inventory_rows = []
scan_timestamp = datetime.now().isoformat()

for db_name in databases_to_scan:
    print(f"\nProcessando database: {db_name}")

    try:
        # Detalhes do database
        db_detail = spark.sql(f"DESCRIBE DATABASE EXTENDED `{db_name}`").collect()
        db_location = ""
        db_owner    = ""
        for row in db_detail:
            if row["database_description_item"] == "Location":
                db_location = row["database_description_value"]
            if row["database_description_item"] == "Owner":
                db_owner = row["database_description_value"]

        # Lista tabelas e views
        tables = spark.sql(f"SHOW TABLES IN `{db_name}`").collect()
        print(f"  Encontradas {len(tables)} tabelas/views")

        for tbl in tables:
            table_name = tbl.tableName
            is_temp    = tbl.isTemporary

            if is_temp:
                continue  # Ignora tabelas temporárias

            try:
                # Detalhes da tabela
                detail_rows = spark.sql(f"DESCRIBE TABLE EXTENDED `{db_name}`.`{table_name}`").collect()

                table_type        = ""
                table_provider    = ""
                table_location    = ""
                table_owner       = ""
                serde_lib         = ""
                input_format      = ""
                output_format     = ""
                table_properties  = ""
                partition_cols    = []
                num_cols          = 0
                col_section       = True

                for detail in detail_rows:
                    col_name = detail["col_name"].strip()
                    data_type = detail["data_type"].strip() if detail["data_type"] else ""

                    # Quando chega na seção de metadados, para de contar colunas
                    if col_name.startswith("# "):
                        col_section = False

                    if col_section and col_name and not col_name.startswith("#"):
                        num_cols += 1

                    if col_name == "Type":
                        table_type = data_type
                    elif col_name == "Provider":
                        table_provider = data_type
                    elif col_name == "Location":
                        table_location = data_type
                    elif col_name == "Owner":
                        table_owner = data_type
                    elif col_name == "Serde Library":
                        serde_lib = data_type
                    elif col_name == "InputFormat":
                        input_format = data_type
                    elif col_name == "OutputFormat":
                        output_format = data_type
                    elif col_name == "Table Properties":
                        table_properties = data_type
                    elif col_name == "# Partition Information":
                        col_section = False
                    elif "# Partition Columns" in col_name:
                        pass
                    elif detail["col_name"].startswith(" ") and not col_section:
                        clean = col_name
                        if clean and not clean.startswith("#"):
                            partition_cols.append(clean)

                # Tenta obter estatísticas (pode falhar em tabelas sem stats)
                try:
                    stats = spark.sql(f"ANALYZE TABLE `{db_name}`.`{table_name}` COMPUTE STATISTICS NOSCAN")
                    desc_stats = spark.sql(f"DESCRIBE TABLE EXTENDED `{db_name}`.`{table_name}`").collect()
                    num_rows = 0
                    size_bytes = 0
                    for s in desc_stats:
                        if s["col_name"] == "Statistics":
                            stats_str = s["data_type"] or ""
                            import re
                            rows_match = re.search(r"(\d+) rows", stats_str)
                            size_match = re.search(r"(\d+) bytes", stats_str)
                            if rows_match:
                                num_rows = int(rows_match.group(1))
                            if size_match:
                                size_bytes = int(size_match.group(1))
                except Exception:
                    num_rows  = -1
                    size_bytes = -1

                inventory_rows.append({
                    "scan_timestamp":   scan_timestamp,
                    "database_name":    db_name,
                    "database_location": db_location,
                    "database_owner":   db_owner,
                    "table_name":       table_name,
                    "object_type":      table_type,         # MANAGED, EXTERNAL, VIEW
                    "data_provider":    table_provider,     # delta, parquet, hive, etc.
                    "table_location":   table_location,
                    "table_owner":      table_owner,
                    "serde_library":    serde_lib,
                    "input_format":     input_format,
                    "output_format":    output_format,
                    "table_properties": table_properties,
                    "partition_columns": json.dumps(partition_cols),
                    "num_columns":      num_cols,
                    "estimated_rows":   num_rows,
                    "estimated_bytes":  size_bytes,
                    "is_delta":         ("delta" in table_provider.lower() or "DeltaSource" in serde_lib),
                    "has_partitions":   len(partition_cols) > 0,
                })

                print(f"    OK: {table_name} ({table_type}, {table_provider})")

            except Exception as e:
                print(f"    ERRO em {table_name}: {e}")
                inventory_rows.append({
                    "scan_timestamp":   scan_timestamp,
                    "database_name":    db_name,
                    "database_location": db_location,
                    "database_owner":   db_owner,
                    "table_name":       table_name,
                    "object_type":      "ERROR",
                    "data_provider":    "",
                    "table_location":   "",
                    "table_owner":      "",
                    "serde_library":    "",
                    "input_format":     "",
                    "output_format":    "",
                    "table_properties": "",
                    "partition_columns": "[]",
                    "num_columns":      0,
                    "estimated_rows":   -1,
                    "estimated_bytes":  -1,
                    "is_delta":         False,
                    "has_partitions":   False,
                })

    except Exception as e:
        print(f"  ERRO ao processar database {db_name}: {e}")

print(f"\nTotal de objetos inventariados: {len(inventory_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Criar DataFrame do Inventário

# COMMAND ----------

inventory_df = spark.createDataFrame(inventory_rows)
display(inventory_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resumo do Inventário

# COMMAND ----------

from pyspark.sql.functions import count, sum as spark_sum, when

print("=" * 60)
print("RESUMO DO INVENTÁRIO HIVE METASTORE")
print("=" * 60)

# Total por tipo de objeto
print("\n--- Objetos por Tipo ---")
inventory_df.groupBy("object_type").agg(count("*").alias("quantidade")) \
    .orderBy("quantidade", ascending=False).show()

# Total por provedor de dados
print("\n--- Objetos por Formato ---")
inventory_df.groupBy("data_provider").agg(count("*").alias("quantidade")) \
    .orderBy("quantidade", ascending=False).show()

# Tabelas Delta vs não-Delta
delta_count  = inventory_df.filter(col("is_delta") == True).count()
non_delta    = inventory_df.filter(col("is_delta") == False).count()
print(f"\n--- Delta vs Não-Delta ---")
print(f"  Delta:     {delta_count}")
print(f"  Não-Delta: {non_delta}")

# Tabelas com partições
with_part  = inventory_df.filter(col("has_partitions") == True).count()
no_part    = inventory_df.filter(col("has_partitions") == False).count()
print(f"\n--- Particionamento ---")
print(f"  Com partições:    {with_part}")
print(f"  Sem partições:    {no_part}")

# Por database
print("\n--- Objetos por Database ---")
inventory_df.groupBy("database_name").agg(count("*").alias("total_objetos")) \
    .orderBy("total_objetos", ascending=False).show(50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Salvar Inventário como Tabela Delta

# COMMAND ----------

# Cria database de auditoria se não existir
spark.sql(f"CREATE DATABASE IF NOT EXISTS `{output_database}`")

# Salva o inventário
(inventory_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{output_database}`.`{output_table}`"))

print(f"Inventário salvo em: {output_database}.{output_table}")
print(f"Total de registros: {inventory_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Exportar Lista para Migração
# MAGIC
# MAGIC Gera listas separadas de objetos por prioridade de migração.

# COMMAND ----------

# Tabelas Delta Managed - migração mais simples (DEEP CLONE)
delta_managed = inventory_df.filter(
    (col("object_type") == "MANAGED") & (col("is_delta") == True)
)
print(f"Tabelas Delta MANAGED (DEEP CLONE): {delta_managed.count()}")

# Tabelas Delta External - requer sync de external location
delta_external = inventory_df.filter(
    (col("object_type") == "EXTERNAL") & (col("is_delta") == True)
)
print(f"Tabelas Delta EXTERNAL (sync location): {delta_external.count()}")

# Tabelas não-Delta - requer conversão ou CTAS
non_delta_tables = inventory_df.filter(
    col("is_delta") == False
).filter(col("object_type") != "VIEW")
print(f"Tabelas Não-Delta (CTAS/conversão): {non_delta_tables.count()}")

# Views - migração manual ou via script
views = inventory_df.filter(col("object_type") == "VIEW")
print(f"Views (migração de DDL): {views.count()}")

# COMMAND ----------

# Retorna contagens para uso em notebooks subsequentes
dbutils.notebook.exit(json.dumps({
    "total_objects":     len(inventory_rows),
    "delta_managed":     delta_managed.count(),
    "delta_external":    delta_external.count(),
    "non_delta_tables":  non_delta_tables.count(),
    "views":             views.count(),
    "databases_scanned": len(databases_to_scan),
    "output_table":      f"{output_database}.{output_table}"
}))
