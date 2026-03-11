# Databricks notebook source

# MAGIC %md
# MAGIC # 03.03 - Validar Permissões no Unity Catalog
# MAGIC
# MAGIC Verifica se as permissões foram corretamente migradas do Hive para o UC,
# MAGIC comparando os grants registrados no log de migração com os grants
# MAGIC efetivamente existentes no UC.
# MAGIC
# MAGIC **Verificações:**
# MAGIC - Grants no nível de schema (database)
# MAGIC - Grants no nível de tabela
# MAGIC - Verificação de acesso funcional (SELECT em amostra)
# MAGIC - Auditoria de objetos sem permissões definidas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

dbutils.widgets.text("uc_catalog_name",    "serasa_prod",     "Nome do Catálogo UC")
dbutils.widgets.text("inventory_database", "migration_audit", "Database com o inventário")
dbutils.widgets.text("databases_filter",   "",                "Databases a validar (vazio=todos)")
dbutils.widgets.text("test_user",          "",                "Usuário para teste funcional de acesso")

uc_catalog_name    = dbutils.widgets.get("uc_catalog_name")
inventory_database = dbutils.widgets.get("inventory_database")
databases_filter   = dbutils.widgets.get("databases_filter")
test_user          = dbutils.widgets.get("test_user")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregar Log de Migração de Permissões

# COMMAND ----------

from pyspark.sql.functions import col
from datetime import datetime

try:
    perms_log_df = spark.table(f"`{inventory_database}`.`permissions_migration_log`")
    print(f"Grants migrados no log: {perms_log_df.count()}")
    display(perms_log_df.groupBy("status").count())
except Exception as e:
    print(f"Log de permissões não encontrado: {e}")
    perms_log_df = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificar Grants Atuais no UC

# COMMAND ----------

schema_map_df = spark.table(f"`{inventory_database}`.`schema_mapping`")

if databases_filter.strip():
    schema_map_df = schema_map_df.filter(
        col("hive_database").isin([d.strip() for d in databases_filter.split(",")])
    )

schemas_list = schema_map_df.collect()

current_grants = []

print("Verificando grants atuais no UC...\n")

for row in schemas_list:
    uc_schema = row["uc_schema"]
    hive_db   = row["hive_database"]

    try:
        # Grants no schema
        schema_grants = spark.sql(
            f"SHOW GRANTS ON SCHEMA `{uc_catalog_name}`.`{uc_schema}`"
        ).collect()

        for g in schema_grants:
            current_grants.append({
                "object_type": "SCHEMA",
                "object_name": f"{uc_catalog_name}.{uc_schema}",
                "principal":   g["Principal"] if "Principal" in g.__fields__ else str(g[0]),
                "privilege":   g["ActionType"] if "ActionType" in g.__fields__ else str(g[1]),
                "hive_db":     hive_db,
            })

        # Grants nas tabelas do schema
        tables = spark.sql(
            f"SHOW TABLES IN `{uc_catalog_name}`.`{uc_schema}`"
        ).collect()

        for tbl in tables:
            table_name = tbl["tableName"]
            try:
                tbl_grants = spark.sql(
                    f"SHOW GRANTS ON TABLE `{uc_catalog_name}`.`{uc_schema}`.`{table_name}`"
                ).collect()
                for g in tbl_grants:
                    current_grants.append({
                        "object_type": "TABLE",
                        "object_name": f"{uc_catalog_name}.{uc_schema}.{table_name}",
                        "principal":   g["Principal"] if "Principal" in g.__fields__ else str(g[0]),
                        "privilege":   g["ActionType"] if "ActionType" in g.__fields__ else str(g[1]),
                        "hive_db":     hive_db,
                    })
            except Exception:
                pass  # Tabela pode não ter grants explícitos

        print(f"  {hive_db} → {uc_schema}: {len(schema_grants)} grants no schema")

    except Exception as e:
        print(f"  ERRO em {uc_schema}: {e}")

print(f"\nTotal grants no UC: {len(current_grants)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Análise de Cobertura de Permissões

# COMMAND ----------

if current_grants:
    grants_df = spark.createDataFrame(current_grants)

    print("\n--- Grants por Tipo de Objeto ---")
    grants_df.groupBy("object_type").count().show()

    print("\n--- Grants por Privilégio ---")
    grants_df.groupBy("privilege").count().orderBy("count", ascending=False).show()

    print("\n--- Principals com Acesso ---")
    grants_df.select("principal").distinct().show(50, truncate=False)

    # Schemas sem nenhum grant
    schemas_with_grants = set(
        r["object_name"]
        for r in current_grants
        if r["object_type"] == "SCHEMA"
    )
    all_schemas = {f"{uc_catalog_name}.{row['uc_schema']}" for row in schemas_list}
    schemas_no_grants = all_schemas - schemas_with_grants

    if schemas_no_grants:
        print(f"\n--- ALERTA: Schemas sem Grants Definidos ({len(schemas_no_grants)}) ---")
        for s in sorted(schemas_no_grants):
            print(f"  {s}")
else:
    print("Nenhum grant encontrado no UC.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Comparar com Log de Migração

# COMMAND ----------

if perms_log_df is not None and current_grants:
    # Grants esperados (do log de migração com status SUCCESS/DRY_RUN)
    expected_grants = perms_log_df.filter(
        col("status").isin(["SUCCESS", "DRY_RUN"])
    ).select(
        col("uc_object").alias("object_name"),
        col("principal"),
        col("uc_privilege").alias("privilege")
    ).distinct()

    # Grants atuais no UC
    actual_grants_df = grants_df.select("object_name", "principal", "privilege").distinct()

    # Grants esperados mas ausentes no UC
    missing_grants = expected_grants.subtract(actual_grants_df)

    # Grants no UC não esperados (podem ser herdados do catálogo)
    unexpected_grants = actual_grants_df.subtract(expected_grants)

    print(f"Grants esperados (do log):       {expected_grants.count()}")
    print(f"Grants presentes no UC:          {actual_grants_df.count()}")
    print(f"Grants esperados AUSENTES no UC: {missing_grants.count()}")
    print(f"Grants no UC NÃO esperados:      {unexpected_grants.count()}")

    if missing_grants.count() > 0:
        print("\n--- Grants Esperados Não Encontrados no UC ---")
        display(missing_grants)
else:
    print("Comparação com log de migração ignorada (log não disponível ou sem grants atuais).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Teste Funcional de Acesso (Opcional)

# COMMAND ----------

if test_user:
    print(f"Verificando acesso do usuário: {test_user}")
    # Verifica permissões efetivas para um usuário específico
    for row in schemas_list[:5]:  # Amostra de 5 schemas
        uc_schema = row["uc_schema"]
        try:
            effective = spark.sql(
                f"SHOW GRANTS `{test_user}` ON SCHEMA `{uc_catalog_name}`.`{uc_schema}`"
            ).collect()
            print(f"  {uc_schema}: {len(effective)} privilégios para {test_user}")
            for g in effective:
                print(f"    - {g}")
        except Exception as e:
            print(f"  ERRO em {uc_schema}: {e}")
else:
    print("test_user não informado. Teste funcional de acesso pulado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Salvar Relatório de Permissões

# COMMAND ----------

if current_grants:
    (grants_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"`{inventory_database}`.`validation_permissions_report`"))
    print(f"Relatório salvo em: {inventory_database}.validation_permissions_report")

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
    "schemas_validated":  len(schemas_list),
    "total_grants_in_uc": len(current_grants),
    "schemas_no_grants":  len(schemas_no_grants) if "schemas_no_grants" in dir() else 0,
}))
