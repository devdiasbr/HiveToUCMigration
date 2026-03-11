# Databricks notebook source

# MAGIC %md
# MAGIC # 01.01 - Criar Catálogo e Schemas no Unity Catalog
# MAGIC
# MAGIC Cria a estrutura de catálogo e schemas no Unity Catalog espelhando
# MAGIC a estrutura de databases do Hive Metastore legado.
# MAGIC
# MAGIC **Pré-requisitos:**
# MAGIC - Metastore UC configurado e associado ao workspace
# MAGIC - Permissão `CREATE CATALOG` no Metastore
# MAGIC - Storage credential e external location configurados (notebook 01_02)
# MAGIC
# MAGIC **Convenção de nomenclatura:**
# MAGIC ```
# MAGIC Hive: <database>.<table>
# MAGIC UC:   <catalog>.<schema>.<table>
# MAGIC       ex: serasa_prod.<database>.<table>
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros de Configuração

# COMMAND ----------

dbutils.widgets.text("uc_catalog_name",      "serasa_prod",     "Nome do Catálogo UC a criar")
dbutils.widgets.text("catalog_comment",      "Catálogo principal Serasa - migrado do Hive Metastore", "Comentário do catálogo")
dbutils.widgets.text("catalog_storage_root", "",                "Storage root do catálogo (vazio = default managed)")
dbutils.widgets.text("inventory_database",   "migration_audit", "Database com o inventário")
dbutils.widgets.text("inventory_table",      "hive_inventory",  "Tabela do inventário")
dbutils.widgets.text("dry_run",              "true",            "dry_run=true para simular sem criar")

uc_catalog_name      = dbutils.widgets.get("uc_catalog_name")
catalog_comment      = dbutils.widgets.get("catalog_comment")
catalog_storage_root = dbutils.widgets.get("catalog_storage_root")
inventory_database   = dbutils.widgets.get("inventory_database")
inventory_table      = dbutils.widgets.get("inventory_table")
dry_run              = dbutils.widgets.get("dry_run").lower() == "true"

print(f"Catálogo UC:     {uc_catalog_name}")
print(f"Storage Root:    {catalog_storage_root or '(managed default)'}")
print(f"Dry Run:         {dry_run}")
print(f"Inventário:      {inventory_database}.{inventory_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificar Pré-requisitos

# COMMAND ----------

# Verificar se o metastore UC está configurado
try:
    metastore_info = spark.sql("SELECT current_metastore()").collect()[0][0]
    print(f"Metastore UC ativo: {metastore_info}")
except Exception as e:
    raise Exception(
        f"Metastore UC não encontrado. Configure o Unity Catalog antes de prosseguir. Erro: {e}"
    )

# Verificar se o catálogo já existe
existing_catalogs = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
catalog_exists = uc_catalog_name in existing_catalogs
print(f"\nCatálogos existentes: {existing_catalogs}")
print(f"Catálogo '{uc_catalog_name}' já existe: {catalog_exists}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Criar o Catálogo

# COMMAND ----------

def execute_sql(sql_stmt, dry_run=True, description=""):
    """Executa SQL ou apenas exibe no modo dry_run."""
    if dry_run:
        print(f"[DRY RUN] {description}")
        print(f"          SQL: {sql_stmt}")
    else:
        print(f"[EXEC] {description}")
        spark.sql(sql_stmt)
        print(f"       OK")

# Criar catálogo
if not catalog_exists:
    if catalog_storage_root:
        create_catalog_sql = f"""
            CREATE CATALOG IF NOT EXISTS `{uc_catalog_name}`
            MANAGED LOCATION '{catalog_storage_root}'
            COMMENT '{catalog_comment}'
        """
    else:
        create_catalog_sql = f"""
            CREATE CATALOG IF NOT EXISTS `{uc_catalog_name}`
            COMMENT '{catalog_comment}'
        """

    execute_sql(
        create_catalog_sql.strip(),
        dry_run=dry_run,
        description=f"Criar catálogo '{uc_catalog_name}'"
    )
else:
    print(f"Catálogo '{uc_catalog_name}' já existe - pulando criação.")
    if not dry_run:
        # Atualiza comentário se necessário
        spark.sql(f"COMMENT ON CATALOG `{uc_catalog_name}` IS '{catalog_comment}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Carregar Databases do Inventário

# COMMAND ----------

# Carrega lista de databases únicos do inventário
inventory_df = spark.table(f"`{inventory_database}`.`{inventory_table}`")

databases = inventory_df.select("database_name", "database_location", "database_owner") \
    .distinct() \
    .orderBy("database_name") \
    .collect()

print(f"Databases a criar como schemas no UC: {len(databases)}")
for db in databases:
    print(f"  - {db.database_name}  (owner: {db.database_owner})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Criar Schemas (Databases → UC Schemas)

# COMMAND ----------

import re

def sanitize_name(name):
    """Remove ou substitui caracteres inválidos para nomes UC."""
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    if sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return sanitized.lower()

schema_mapping = {}  # {hive_db: uc_schema}
creation_results = []

# Obtém schemas existentes no catálogo (se não for dry_run)
if not dry_run and not catalog_exists:
    existing_schemas = []
else:
    try:
        existing_schemas = [
            r.databaseName
            for r in spark.sql(f"SHOW SCHEMAS IN `{uc_catalog_name}`").collect()
        ]
    except Exception:
        existing_schemas = []

for db in databases:
    db_name     = db.database_name
    schema_name = sanitize_name(db_name)

    if schema_name != db_name:
        print(f"  RENAME: '{db_name}' → '{schema_name}' (caracteres inválidos removidos)")

    schema_mapping[db_name] = schema_name

    create_schema_sql = f"""
        CREATE SCHEMA IF NOT EXISTS `{uc_catalog_name}`.`{schema_name}`
        COMMENT 'Migrado do Hive database: {db_name}'
    """

    if schema_name in existing_schemas:
        print(f"  SKIP: Schema '{schema_name}' já existe")
        status = "ALREADY_EXISTS"
    else:
        execute_sql(
            create_schema_sql.strip(),
            dry_run=dry_run,
            description=f"Criar schema '{uc_catalog_name}.{schema_name}'"
        )
        status = "DRY_RUN" if dry_run else "CREATED"

    creation_results.append({
        "hive_database":    db_name,
        "uc_catalog":       uc_catalog_name,
        "uc_schema":        schema_name,
        "name_changed":     db_name != schema_name,
        "status":           status,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Salvar Mapeamento Hive → UC

# COMMAND ----------

mapping_df = spark.createDataFrame(creation_results)
display(mapping_df)

# Salva o mapeamento para uso nos notebooks de migração
(mapping_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{inventory_database}`.`schema_mapping`"))

print(f"\nMapeamento salvo em: {inventory_database}.schema_mapping")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Configurar Permissões Iniciais no Catálogo

# COMMAND ----------

# MAGIC %md
# MAGIC > **Ajuste os grupos/usuários abaixo conforme a política da sua organização.**

# COMMAND ----------

# Grupos de acesso (ajuste conforme necessário)
ADMIN_GROUP    = "data-engineers"
ANALYST_GROUP  = "data-analysts"
VIEWER_GROUP   = "data-viewers"

privilege_stmts = [
    # Permissões no catálogo
    (f"GRANT USE CATALOG ON CATALOG `{uc_catalog_name}` TO `{ADMIN_GROUP}`",
     f"GRANT USE CATALOG a {ADMIN_GROUP}"),
    (f"GRANT USE CATALOG ON CATALOG `{uc_catalog_name}` TO `{ANALYST_GROUP}`",
     f"GRANT USE CATALOG a {ANALYST_GROUP}"),
    (f"GRANT CREATE SCHEMA ON CATALOG `{uc_catalog_name}` TO `{ADMIN_GROUP}`",
     f"GRANT CREATE SCHEMA a {ADMIN_GROUP}"),
    # Permissões de leitura no catálogo inteiro para analistas
    (f"GRANT USE SCHEMA ON CATALOG `{uc_catalog_name}` TO `{ANALYST_GROUP}`",
     f"GRANT USE SCHEMA a {ANALYST_GROUP}"),
    (f"GRANT SELECT ON CATALOG `{uc_catalog_name}` TO `{ANALYST_GROUP}`",
     f"GRANT SELECT a {ANALYST_GROUP}"),
]

print("Permissões a serem aplicadas:")
for sql_stmt, desc in privilege_stmts:
    execute_sql(sql_stmt, dry_run=dry_run, description=desc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verificar Resultado

# COMMAND ----------

if not dry_run:
    print("Catálogos existentes:")
    spark.sql("SHOW CATALOGS").show()

    print(f"\nSchemas em '{uc_catalog_name}':")
    spark.sql(f"SHOW SCHEMAS IN `{uc_catalog_name}`").show(100)

import json
dbutils.notebook.exit(json.dumps({
    "catalog":             uc_catalog_name,
    "schemas_created":     len([r for r in creation_results if r["status"] == "CREATED"]),
    "schemas_existing":    len([r for r in creation_results if r["status"] == "ALREADY_EXISTS"]),
    "schemas_dry_run":     len([r for r in creation_results if r["status"] == "DRY_RUN"]),
    "total_schemas":       len(creation_results),
    "mapping_table":       f"{inventory_database}.schema_mapping",
    "dry_run":             dry_run
}))
