# Databricks notebook source

# MAGIC %md
# MAGIC # 02.04 - Migrar Permissões (GRANTS)
# MAGIC
# MAGIC Extrai as permissões do Hive Metastore e as recria no Unity Catalog.
# MAGIC
# MAGIC **Mapeamento de privilégios Hive → UC:**
# MAGIC
# MAGIC | Hive Privilege | UC Privilege |
# MAGIC |---------------|-------------|
# MAGIC | SELECT | SELECT |
# MAGIC | INSERT | MODIFY |
# MAGIC | UPDATE | MODIFY |
# MAGIC | DELETE | MODIFY |
# MAGIC | ALL PRIVILEGES | ALL PRIVILEGES |
# MAGIC | CREATE | CREATE TABLE |
# MAGIC | USAGE | USE SCHEMA |
# MAGIC | OWNERSHIP | (transferir via ALTER TABLE SET OWNER) |
# MAGIC
# MAGIC **Nota:** O UC usa um modelo de privilégios hierárquico.
# MAGIC Permissões no catálogo se propagam para schemas e tabelas.

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
# MAGIC ## 1. Extrair Permissões do Hive

# COMMAND ----------

from pyspark.sql.functions import col
from datetime import datetime

# Mapeamento de privilégios Hive → UC
PRIVILEGE_MAPPING = {
    "SELECT":         "SELECT",
    "INSERT":         "MODIFY",
    "UPDATE":         "MODIFY",
    "DELETE":         "MODIFY",
    "ALL PRIVILEGES": "ALL PRIVILEGES",
    "ALL":            "ALL PRIVILEGES",
    "CREATE":         "CREATE TABLE",
    "USAGE":          "USE SCHEMA",
    "READ_METADATA":  "BROWSE",
    "MODIFY":         "MODIFY",
}

inventory_df  = spark.table(f"`{inventory_database}`.`hive_inventory`")
schema_map_df = spark.table(f"`{inventory_database}`.`schema_mapping`")

# Databases a processar
databases = inventory_df.select("database_name").distinct()
if databases_filter.strip():
    databases = databases.filter(col("database_name").isin(
        [d.strip() for d in databases_filter.split(",")]
    ))

db_list = [r["database_name"] for r in databases.collect()]

# Extrai permissões de todos os objetos
all_grants = []

for db_name in db_list:
    print(f"\nExtraindo permissões de: {db_name}")

    # Permissões no database
    try:
        grants = spark.sql(f"SHOW GRANTS ON DATABASE `{db_name}`").collect()
        for g in grants:
            all_grants.append({
                "hive_object_type": "DATABASE",
                "hive_database":    db_name,
                "hive_object":      db_name,
                "principal":        g.Principal if hasattr(g, 'Principal') else g[0],
                "privilege":        g.ActionType if hasattr(g, 'ActionType') else g[1],
                "grant_option":     False,
            })
    except Exception as e:
        print(f"  Aviso: não foi possível extrair grants do database {db_name}: {e}")

    # Permissões nas tabelas/views
    tables = inventory_df.filter(col("database_name") == db_name).collect()
    for tbl in tables:
        table_name = tbl["table_name"]
        try:
            grants = spark.sql(f"SHOW GRANTS ON TABLE `{db_name}`.`{table_name}`").collect()
            for g in grants:
                all_grants.append({
                    "hive_object_type": tbl["object_type"],
                    "hive_database":    db_name,
                    "hive_object":      f"{db_name}.{table_name}",
                    "principal":        g.Principal if hasattr(g, 'Principal') else g[0],
                    "privilege":        g.ActionType if hasattr(g, 'ActionType') else g[1],
                    "grant_option":     False,
                })
        except Exception as e:
            # SHOW GRANTS pode falhar em objetos sem ACL explícita
            pass

print(f"\nTotal de grants extraídos: {len(all_grants)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Mapear para Privilégios UC

# COMMAND ----------

schema_map = {
    r["hive_database"]: r["uc_schema"]
    for r in schema_map_df.collect()
}

def map_to_uc_grant(grant_row):
    """Converte um grant Hive para formato UC."""
    obj_type  = grant_row["hive_object_type"]
    hive_obj  = grant_row["hive_object"]
    db_name   = grant_row["hive_database"]
    privilege = grant_row["privilege"].upper()
    principal = grant_row["principal"]

    # Mapeamento de privilégio
    uc_privilege = PRIVILEGE_MAPPING.get(privilege, privilege)

    # Mapeamento de objeto
    uc_schema = schema_map.get(db_name, db_name)

    if obj_type == "DATABASE":
        uc_object      = f"SCHEMA `{uc_catalog_name}`.`{uc_schema}`"
        uc_object_name = f"{uc_catalog_name}.{uc_schema}"
        uc_privilege   = "USE SCHEMA" if privilege in ("USAGE", "CREATE") else uc_privilege
    elif obj_type in ("MANAGED", "EXTERNAL"):
        table_name     = hive_obj.split(".")[-1]
        uc_object      = f"TABLE `{uc_catalog_name}`.`{uc_schema}`.`{table_name}`"
        uc_object_name = f"{uc_catalog_name}.{uc_schema}.{table_name}"
    elif obj_type == "VIEW":
        view_name      = hive_obj.split(".")[-1]
        uc_object      = f"VIEW `{uc_catalog_name}`.`{uc_schema}`.`{view_name}`"
        uc_object_name = f"{uc_catalog_name}.{uc_schema}.{view_name}"
    else:
        uc_object      = None
        uc_object_name = None

    if uc_object:
        grant_sql = f"GRANT {uc_privilege} ON {uc_object} TO `{principal}`"
    else:
        grant_sql = None

    return {**grant_row,
            "uc_object":    uc_object_name,
            "uc_privilege": uc_privilege,
            "grant_sql":    grant_sql}

mapped_grants = [map_to_uc_grant(g) for g in all_grants if all_grants]

# Filtra grants sem SQL mapeado
valid_grants   = [g for g in mapped_grants if g["grant_sql"]]
invalid_grants = [g for g in mapped_grants if not g["grant_sql"]]

print(f"Grants mapeados com sucesso: {len(valid_grants)}")
print(f"Grants sem mapeamento: {len(invalid_grants)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Aplicar Grants no UC

# COMMAND ----------

grant_results = []

for g in valid_grants:
    start_time = datetime.now()
    try:
        if dry_run:
            print(f"[DRY RUN] {g['grant_sql']}")
            status = "DRY_RUN"
            error_msg = None
        else:
            spark.sql(g["grant_sql"])
            print(f"  [OK] {g['grant_sql']}")
            status = "SUCCESS"
            error_msg = None
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)[:300]}"
        print(f"  [ERRO] {g['grant_sql']}: {error_msg}")
        status = "ERROR"

    grant_results.append({
        "hive_object":   g["hive_object"],
        "uc_object":     g["uc_object"],
        "principal":     g["principal"],
        "hive_privilege": g["privilege"],
        "uc_privilege":  g["uc_privilege"],
        "grant_sql":     g["grant_sql"],
        "status":        status,
        "error_message": error_msg,
        "applied_at":    start_time.isoformat(),
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Migrar Ownership de Tabelas

# COMMAND ----------

# MAGIC %md
# MAGIC > O ownership no UC é transferido via `ALTER TABLE/VIEW SET OWNER TO`.
# MAGIC > Requer que o executor seja metastore admin ou owner do objeto.

# COMMAND ----------

owner_results = []

# Verifica owners das tabelas no Hive
tables_with_owners = (
    inventory_df
    .filter(col("table_owner") != "")
    .filter(col("table_owner").isNotNull())
    .join(schema_map_df, inventory_df.database_name == schema_map_df.hive_database, "left")
    .select(
        inventory_df.database_name,
        inventory_df.table_name,
        inventory_df.table_owner,
        inventory_df.object_type,
        schema_map_df.uc_schema,
    )
    .collect()
)

if databases_filter.strip():
    db_filter_list = [d.strip() for d in databases_filter.split(",")]
    tables_with_owners = [t for t in tables_with_owners if t["database_name"] in db_filter_list]

print(f"Tabelas com owner definido: {len(tables_with_owners)}")

for t in tables_with_owners:
    db_name    = t["database_name"]
    table_name = t["table_name"]
    owner      = t["table_owner"]
    uc_schema  = t["uc_schema"] or db_name
    obj_type   = t["object_type"]

    if obj_type == "VIEW":
        alter_sql = f"ALTER VIEW `{uc_catalog_name}`.`{uc_schema}`.`{table_name}` SET OWNER TO `{owner}`"
    else:
        alter_sql = f"ALTER TABLE `{uc_catalog_name}`.`{uc_schema}`.`{table_name}` SET OWNER TO `{owner}`"

    try:
        if dry_run:
            print(f"[DRY RUN] {alter_sql}")
            status = "DRY_RUN"
            error_msg = None
        else:
            spark.sql(alter_sql)
            status = "SUCCESS"
            error_msg = None
    except Exception as e:
        error_msg = str(e)[:200]
        status = "ERROR"
        print(f"  ERRO owner {table_name}: {error_msg}")

    owner_results.append({
        "table":   f"{uc_catalog_name}.{uc_schema}.{table_name}",
        "owner":   owner,
        "sql":     alter_sql,
        "status":  status,
        "error":   error_msg,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resumo e Log

# COMMAND ----------

g_success = [r for r in grant_results if r["status"] == "SUCCESS"]
g_errors  = [r for r in grant_results if r["status"] == "ERROR"]
g_dry     = [r for r in grant_results if r["status"] == "DRY_RUN"]

print("=" * 60)
print("RESUMO - PERMISSÕES")
print("=" * 60)
print(f"  Grants aplicados: {len(g_success)}")
print(f"  Grants com erro:  {len(g_errors)}")
print(f"  Dry Run:          {len(g_dry)}")
print(f"\n  Ownership transferido: {len([o for o in owner_results if o['status'] == 'SUCCESS'])}")

if g_errors:
    print("\n--- Grants com Erro ---")
    for e in g_errors:
        print(f"  {e['grant_sql']}: {e['error_message']}")

# Salva log
if grant_results:
    grants_df = spark.createDataFrame(grant_results)
    (grants_df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(f"`{inventory_database}`.`permissions_migration_log`"))
    display(grants_df)

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
    "grants_total":   len(grant_results),
    "grants_success": len(g_success),
    "grants_errors":  len(g_errors),
    "ownership_set":  len([o for o in owner_results if o["status"] == "SUCCESS"]),
}))
