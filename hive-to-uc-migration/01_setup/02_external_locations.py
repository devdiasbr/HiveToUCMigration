# Databricks notebook source

# MAGIC %md
# MAGIC # 01.02 - Configurar Storage Credentials e External Locations
# MAGIC
# MAGIC Configura as credenciais de armazenamento e os external locations no
# MAGIC Unity Catalog necessários para que o UC possa acessar os dados externos
# MAGIC (tabelas EXTERNAL) migrados do Hive.
# MAGIC
# MAGIC **Fluxo:**
# MAGIC ```
# MAGIC 1. Criar Storage Credential (service principal / managed identity)
# MAGIC 2. Criar External Location apontando para os paths do Hive
# MAGIC 3. Validar acesso aos paths
# MAGIC 4. Configurar permissões nos External Locations
# MAGIC ```
# MAGIC
# MAGIC **Pré-requisitos:**
# MAGIC - Ser `metastore admin` no UC
# MAGIC - Service Principal com acesso de leitura/escrita aos storages
# MAGIC - Azure: permissão `Storage Blob Data Contributor` no ADLS Gen2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros de Configuração

# COMMAND ----------

dbutils.widgets.text("inventory_database",    "migration_audit", "Database com o inventário")
dbutils.widgets.text("inventory_table",        "hive_inventory",  "Tabela do inventário")
dbutils.widgets.text("storage_credential_name","serasa-adls-cred","Nome da Storage Credential")
dbutils.widgets.text("managed_identity_id",    "",               "Azure Managed Identity / SP Application ID")
dbutils.widgets.text("dry_run",                "true",           "dry_run=true para simular")

inventory_database     = dbutils.widgets.get("inventory_database")
inventory_table        = dbutils.widgets.get("inventory_table")
storage_credential_name = dbutils.widgets.get("storage_credential_name")
managed_identity_id    = dbutils.widgets.get("managed_identity_id")
dry_run                = dbutils.widgets.get("dry_run").lower() == "true"

print(f"Storage Credential: {storage_credential_name}")
print(f"Identity ID:        {managed_identity_id or '(não informado - usar se já existe)'}")
print(f"Dry Run:            {dry_run}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Criar Storage Credential

# COMMAND ----------

# MAGIC %md
# MAGIC > **AZURE:** Use Managed Identity (recomendado) ou Service Principal
# MAGIC > **AWS:** Use IAM Role
# MAGIC > **GCP:** Use Service Account

# COMMAND ----------

def execute_sql(sql_stmt, dry_run=True, description=""):
    if dry_run:
        print(f"[DRY RUN] {description}")
        print(f"          SQL: {sql_stmt[:200]}...")
    else:
        print(f"[EXEC] {description}")
        try:
            spark.sql(sql_stmt)
            print("       OK")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"       SKIP (já existe)")
            else:
                raise e

# ---- Azure Managed Identity (recomendado) ----
if managed_identity_id:
    create_credential_sql = f"""
        CREATE STORAGE CREDENTIAL IF NOT EXISTS `{storage_credential_name}`
        WITH AZURE_MANAGED_IDENTITY (
            CREDENTIAL '{managed_identity_id}'
        )
        COMMENT 'Credencial para acesso ao ADLS Gen2 - Serasa'
    """
# ---- Azure Service Principal ----
# create_credential_sql = f"""
#     CREATE STORAGE CREDENTIAL IF NOT EXISTS `{storage_credential_name}`
#     WITH AZURE_SERVICE_PRINCIPAL (
#         DIRECTORY_ID    = '<tenant-id>',
#         APPLICATION_ID  = '<app-id>',
#         CLIENT_SECRET   = secret('<secret-scope>', '<secret-key>')
#     )
# """
# ---- AWS IAM Role ----
# create_credential_sql = f"""
#     CREATE STORAGE CREDENTIAL IF NOT EXISTS `{storage_credential_name}`
#     WITH AWS_IAM_ROLE (ROLE_ARN = 'arn:aws:iam::<account>:role/<role-name>')
# """
else:
    create_credential_sql = f"-- Managed Identity ID não informado"
    print("AVISO: managed_identity_id não informado. Usando credencial existente se disponível.")

execute_sql(
    create_credential_sql,
    dry_run=dry_run,
    description=f"Criar storage credential '{storage_credential_name}'"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Identificar Paths Únicos do Inventário

# COMMAND ----------

from pyspark.sql.functions import col, substring_index
import re

inventory_df = spark.table(f"`{inventory_database}`.`{inventory_table}`")

# Extrai paths únicos das tabelas EXTERNAL
external_paths = (
    inventory_df
    .filter(col("object_type") == "EXTERNAL")
    .filter(col("table_location") != "")
    .select("table_location", "database_name")
    .distinct()
    .collect()
)

# Agrupa paths por storage account (nível superior)
def extract_storage_root(path):
    """Extrai o root do storage a partir do path completo."""
    # ADLS Gen2: abfss://container@account.dfs.core.windows.net/path
    m = re.match(r'(abfss://[^@]+@[^/]+\.dfs\.core\.windows\.net/[^/]+)', path)
    if m:
        return m.group(1)
    # ADLS Gen1 / WASB: wasbs://container@account.blob.core.windows.net/path
    m = re.match(r'(wasbs?://[^@]+@[^/]+\.blob\.core\.windows\.net/[^/]+)', path)
    if m:
        return m.group(1)
    # S3: s3://bucket/path
    m = re.match(r'(s3[an]?://[^/]+)', path)
    if m:
        return m.group(1)
    # GCS: gs://bucket/path
    m = re.match(r'(gs://[^/]+)', path)
    if m:
        return m.group(1)
    # dbfs: dbfs:/mnt/...
    m = re.match(r'(dbfs:/mnt/[^/]+)', path)
    if m:
        return m.group(1)
    return path

# Identifica roots únicos
storage_roots = set()
for row in external_paths:
    root = extract_storage_root(row["table_location"])
    storage_roots.add(root)

print(f"Total de tabelas EXTERNAL: {len(external_paths)}")
print(f"\nStorage roots identificados ({len(storage_roots)}):")
for root in sorted(storage_roots):
    print(f"  {root}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Criar External Locations

# COMMAND ----------

# MAGIC %md
# MAGIC > Um External Location é criado por container/bucket.
# MAGIC > Todos os sub-paths dentro do container herdam o acesso.

# COMMAND ----------

external_location_results = []

for root_path in sorted(storage_roots):
    # Gera nome válido para o external location
    clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', root_path)
    clean_name = re.sub(r'_+', '_', clean_name).strip('_').lower()
    clean_name = clean_name[:127]  # Limite de tamanho

    create_ext_location_sql = f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS `{clean_name}`
        URL '{root_path}'
        WITH (STORAGE CREDENTIAL `{storage_credential_name}`)
        COMMENT 'External location para migração Hive → UC'
    """

    execute_sql(
        create_ext_location_sql.strip(),
        dry_run=dry_run,
        description=f"Criar external location '{clean_name}' → {root_path}"
    )

    external_location_results.append({
        "location_name": clean_name,
        "storage_path":  root_path,
        "credential":    storage_credential_name,
        "status":        "DRY_RUN" if dry_run else "CREATED"
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validar Acesso aos External Locations

# COMMAND ----------

if not dry_run:
    print("Validando acesso aos External Locations...")
    for loc in external_location_results:
        try:
            result = spark.sql(f"""
                VALIDATE STORAGE CREDENTIAL `{storage_credential_name}`
                ON LOCATION '{loc["storage_path"]}'
            """).collect()
            print(f"  OK: {loc['location_name']}")
        except Exception as e:
            print(f"  ERRO: {loc['location_name']} - {e}")
else:
    print("[DRY RUN] Validação de acesso pulada (dry_run=true)")

    # Lista external locations existentes para referência
    try:
        print("\nExternal Locations já configurados:")
        spark.sql("SHOW EXTERNAL LOCATIONS").show(50, truncate=False)
    except Exception as e:
        print(f"  Não foi possível listar external locations: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Configurar Permissões nos External Locations

# COMMAND ----------

ADMIN_GROUP = "data-engineers"

for loc in external_location_results:
    grant_sql = f"""
        GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `{loc["location_name"]}`
        TO `{ADMIN_GROUP}`
    """
    execute_sql(
        grant_sql,
        dry_run=dry_run,
        description=f"GRANT em external location '{loc['location_name']}'"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Salvar Mapeamento de External Locations

# COMMAND ----------

from pyspark.sql import Row

if external_location_results:
    ext_loc_df = spark.createDataFrame(external_location_results)
    (ext_loc_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"`{inventory_database}`.`external_locations_mapping`"))
    print(f"Mapeamento salvo em: {inventory_database}.external_locations_mapping")
    display(ext_loc_df)
else:
    print("Nenhum external location criado (sem tabelas EXTERNAL no inventário).")

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
    "storage_credential":    storage_credential_name,
    "external_locations":    len(external_location_results),
    "dry_run":               dry_run
}))
