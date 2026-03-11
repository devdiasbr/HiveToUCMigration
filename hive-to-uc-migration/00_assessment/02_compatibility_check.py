# Databricks notebook source

# MAGIC %md
# MAGIC # 00.02 - Verificação de Compatibilidade com Unity Catalog
# MAGIC
# MAGIC Analisa o inventário coletado e identifica incompatibilidades, riscos e
# MAGIC pontos de atenção antes da migração para o Unity Catalog.
# MAGIC
# MAGIC **Verificações realizadas:**
# MAGIC - Tipos de dados não suportados no UC
# MAGIC - Formatos de arquivo que requerem conversão
# MAGIC - Objetos com nomes inválidos para UC (caracteres especiais)
# MAGIC - Tabelas com propriedades customizadas que podem ser perdidas
# MAGIC - SerDes não suportadas no UC
# MAGIC - Tabelas com paths sobrepostos
# MAGIC - Views com referências a databases não migrados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros de Configuração

# COMMAND ----------

dbutils.widgets.text("inventory_database", "migration_audit", "Database com o inventário")
dbutils.widgets.text("inventory_table", "hive_inventory", "Tabela do inventário")
dbutils.widgets.text("report_table", "compatibility_report", "Tabela de saída do relatório")

inventory_database = dbutils.widgets.get("inventory_database")
inventory_table    = dbutils.widgets.get("inventory_table")
report_table       = dbutils.widgets.get("report_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregar Inventário

# COMMAND ----------

from pyspark.sql.functions import col, lit, udf, when, array, array_contains, lower
from pyspark.sql.types import StringType, ArrayType, BooleanType
import re

inventory_df = spark.table(f"`{inventory_database}`.`{inventory_table}`")
print(f"Total de objetos no inventário: {inventory_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Regras de Compatibilidade

# COMMAND ----------

# -------------------------------------------------------
# Regra 1: Formatos não suportados nativamente no UC
# -------------------------------------------------------
UNSUPPORTED_FORMATS = [
    "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
    "org.apache.hadoop.mapred.SequenceFileInputFormat",
    "com.databricks.spark.avro",         # Avro legado
]

FORMATS_NEED_CONVERSION = [
    "parquet",
    "orc",
    "csv",
    "json",
    "avro",
    "text",
]

# Serdes não compatíveis com UC
UNSUPPORTED_SERDES = [
    "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
    "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
    "org.apache.hadoop.hive.ql.io.RCFileSerDe",
]

# -------------------------------------------------------
# Regra 2: Nomes inválidos para UC (catálogo.schema.tabela)
# Nome deve ter apenas letras, números e underscores
# -------------------------------------------------------
def check_name_validity(name):
    if not name:
        return "INVALID_EMPTY"
    if not re.match(r'^[a-zA-Z0-9_]+$', name):
        return f"INVALID_CHARS: {name}"
    if len(name) > 255:
        return "INVALID_TOO_LONG"
    return "OK"

name_validity_udf = udf(check_name_validity, StringType())

# -------------------------------------------------------
# Regra 3: Palavras reservadas do UC
# -------------------------------------------------------
UC_RESERVED_WORDS = {
    "catalog", "schema", "database", "table", "view", "column",
    "function", "volume", "storage", "credential", "location",
    "share", "recipient", "provider"
}

def check_reserved_word(name):
    if name and name.lower() in UC_RESERVED_WORDS:
        return f"RESERVED_WORD: {name}"
    return "OK"

reserved_word_udf = udf(check_reserved_word, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Aplicar Verificações

# COMMAND ----------

from pyspark.sql.functions import concat_ws, collect_list, pandas_udf
import pyspark.sql.functions as F

checks_df = inventory_df.withColumn(
    "check_name_db",        name_validity_udf(col("database_name"))
).withColumn(
    "check_name_table",     name_validity_udf(col("table_name"))
).withColumn(
    "check_reserved_db",    reserved_word_udf(col("database_name"))
).withColumn(
    "check_reserved_table", reserved_word_udf(col("table_name"))
).withColumn(
    "needs_format_conversion",
    col("is_delta") == False
).withColumn(
    "unsupported_serde",
    F.array_contains(F.array(*[lit(s) for s in UNSUPPORTED_SERDES]), col("serde_library"))
).withColumn(
    "unsupported_input_format",
    F.array_contains(F.array(*[lit(f) for f in UNSUPPORTED_FORMATS]), col("input_format"))
).withColumn(
    "migration_complexity",
    when(col("object_type") == "VIEW", "MEDIUM")
    .when(
        (col("object_type") == "MANAGED") & (col("is_delta") == True), "LOW"
    )
    .when(
        (col("object_type") == "EXTERNAL") & (col("is_delta") == True), "LOW"
    )
    .when(
        col("needs_format_conversion") == True, "HIGH"
    )
    .otherwise("MEDIUM")
).withColumn(
    "recommended_strategy",
    when(
        (col("object_type") == "MANAGED") & (col("is_delta") == True),
        "DEEP CLONE para UC"
    )
    .when(
        (col("object_type") == "EXTERNAL") & (col("is_delta") == True),
        "Sync External Location + CREATE TABLE como External no UC"
    )
    .when(col("object_type") == "VIEW", "Recriar DDL da view no UC")
    .when(
        col("needs_format_conversion") == True,
        "CTAS com conversão para Delta no UC"
    )
    .otherwise("Avaliar caso a caso")
).withColumn(
    "has_issues",
    (col("check_name_db") != "OK") |
    (col("check_name_table") != "OK") |
    (col("check_reserved_db") != "OK") |
    (col("check_reserved_table") != "OK") |
    (col("unsupported_serde") == True) |
    (col("unsupported_input_format") == True)
)

display(checks_df.select(
    "database_name", "table_name", "object_type", "data_provider",
    "migration_complexity", "recommended_strategy",
    "needs_format_conversion", "has_issues",
    "check_name_db", "check_name_table"
).orderBy("has_issues", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Relatório de Problemas

# COMMAND ----------

print("=" * 70)
print("RELATÓRIO DE COMPATIBILIDADE - HIVE → UNITY CATALOG")
print("=" * 70)

total = checks_df.count()
with_issues  = checks_df.filter(col("has_issues") == True).count()
print(f"\nTotal de objetos analisados:  {total}")
print(f"Objetos com problemas:        {with_issues}")
print(f"Objetos sem problemas:        {total - with_issues}")

# Por complexidade
print("\n--- Complexidade de Migração ---")
checks_df.groupBy("migration_complexity").count().orderBy("migration_complexity").show()

# Por estratégia recomendada
print("\n--- Estratégia Recomendada ---")
checks_df.groupBy("recommended_strategy").count() \
    .orderBy("count", ascending=False).show(20, truncate=False)

# Objetos com nomes inválidos
invalid_names = checks_df.filter(
    (col("check_name_db") != "OK") | (col("check_name_table") != "OK")
)
if invalid_names.count() > 0:
    print(f"\n--- ALERTA: Objetos com Nomes Inválidos ({invalid_names.count()}) ---")
    display(invalid_names.select(
        "database_name", "table_name", "check_name_db", "check_name_table"
    ))

# Objetos com palavras reservadas
reserved = checks_df.filter(
    (col("check_reserved_db") != "OK") | (col("check_reserved_table") != "OK")
)
if reserved.count() > 0:
    print(f"\n--- ALERTA: Palavras Reservadas ({reserved.count()}) ---")
    display(reserved.select(
        "database_name", "table_name", "check_reserved_db", "check_reserved_table"
    ))

# Formatos não suportados
unsupported = checks_df.filter(
    (col("unsupported_serde") == True) | (col("unsupported_input_format") == True)
)
if unsupported.count() > 0:
    print(f"\n--- ALERTA: Formatos Não Suportados ({unsupported.count()}) ---")
    display(unsupported.select(
        "database_name", "table_name", "serde_library", "input_format"
    ))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificar DDL das Views para Referências Quebradas

# COMMAND ----------

views_df = inventory_df.filter(col("object_type") == "VIEW")

if views_df.count() > 0:
    print(f"Verificando {views_df.count()} views para referências...")
    view_issues = []

    for row in views_df.collect():
        db   = row["database_name"]
        view = row["table_name"]
        try:
            ddl_row = spark.sql(f"SHOW CREATE TABLE `{db}`.`{view}`").collect()
            ddl = ddl_row[0][0] if ddl_row else ""

            # Detecta referências a databases que podem não existir no UC
            refs = re.findall(r'`([^`]+)`\.`([^`]+)`', ddl)
            for ref_db, ref_tbl in refs:
                if ref_db != db:
                    view_issues.append({
                        "view_database": db,
                        "view_name":     view,
                        "ref_database":  ref_db,
                        "ref_table":     ref_tbl,
                        "ddl_snippet":   ddl[:500]
                    })
        except Exception as e:
            view_issues.append({
                "view_database": db,
                "view_name":     view,
                "ref_database":  "ERROR",
                "ref_table":     str(e)[:200],
                "ddl_snippet":   ""
            })

    if view_issues:
        print(f"\nViews com referências cross-database ({len(view_issues)} referências):")
        view_issues_df = spark.createDataFrame(view_issues)
        display(view_issues_df)
    else:
        print("Nenhuma referência cross-database encontrada nas views.")
else:
    print("Nenhuma view encontrada no inventário.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Salvar Relatório de Compatibilidade

# COMMAND ----------

(checks_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{inventory_database}`.`{report_table}`"))

print(f"Relatório salvo em: {inventory_database}.{report_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gerar Plano de Migração

# COMMAND ----------

import json

# Agrupa objetos por estratégia para o plano de migração
migration_plan = {}
for row in checks_df.groupBy("recommended_strategy", "migration_complexity") \
        .count().collect():
    strategy = row["recommended_strategy"]
    migration_plan[strategy] = {
        "count":      row["count"],
        "complexity": row["migration_complexity"]
    }

print("\n" + "=" * 70)
print("PLANO DE MIGRAÇÃO RECOMENDADO")
print("=" * 70)
print("\nFase 1 - Complexidade BAIXA (Delta Tables):")
print("  1.1 Configurar Catalogo e Schemas no UC")
print("  1.2 Configurar External Locations")
print("  1.3 DEEP CLONE de tabelas Delta Managed")
print("  1.4 Sync de tabelas Delta External")

print("\nFase 2 - Complexidade MÉDIA (Views e outros Delta):")
print("  2.1 Recriar Views no UC")
print("  2.2 Migrar tabelas Delta Managed restantes")

print("\nFase 3 - Complexidade ALTA (Não-Delta):")
print("  3.1 CTAS com conversão para Delta de tabelas Parquet/ORC/CSV")
print("  3.2 Validar dados após conversão")

print("\nFase 4 - Pós-Migração:")
print("  4.1 Migrar permissões (GRANTS)")
print("  4.2 Validar schema e contagem de registros")
print("  4.3 Atualizar pipelines/jobs para novos caminhos UC")
print("  4.4 Período de coexistência Hive/UC (shadow mode)")
print("  4.5 Descomissionamento do Hive Metastore")

dbutils.notebook.exit(json.dumps({
    "total_analyzed": total,
    "with_issues":    with_issues,
    "migration_plan": migration_plan,
    "report_table":   f"{inventory_database}.{report_table}"
}))
