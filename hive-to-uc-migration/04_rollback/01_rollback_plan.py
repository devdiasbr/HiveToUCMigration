# Databricks notebook source

# MAGIC %md
# MAGIC # 04.01 - Plano de Rollback: Hive ← Unity Catalog
# MAGIC
# MAGIC Notebook de contingência para reverter a migração em caso de problemas.
# MAGIC
# MAGIC **Estratégia de Rollback por tipo de objeto:**
# MAGIC
# MAGIC | Tipo | Rollback |
# MAGIC |------|---------|
# MAGIC | Delta Managed (DEEP CLONE) | Tabela original no Hive ainda existe → apenas redirecionar jobs |
# MAGIC | Delta External (registrado no UC) | DROP TABLE no UC → dados permanecem no storage |
# MAGIC | Não-Delta convertido (CTAS) | Tabela original no Hive ainda existe → redirecionar jobs |
# MAGIC | Views | DROP VIEW no UC → recriar no Hive se necessário |
# MAGIC | Grants | Revogar grants do UC |
# MAGIC
# MAGIC **IMPORTANTE:** As tabelas originais no Hive NÃO são deletadas durante a migração.
# MAGIC O rollback consiste em remover os objetos do UC e garantir que os pipelines
# MAGIC apontem de volta para o Hive Metastore.
# MAGIC
# MAGIC **Pré-condições para rollback:**
# MAGIC - Validar que a tabela original no Hive ainda existe e está íntegra
# MAGIC - Comunicar stakeholders antes de executar
# MAGIC - Executar fora do horário de pico

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

dbutils.widgets.text("uc_catalog_name",    "serasa_prod",     "Nome do Catálogo UC")
dbutils.widgets.text("inventory_database", "migration_audit", "Database com o inventário")
dbutils.widgets.text("databases_filter",   "",                "Databases para rollback (vazio=todos)")
dbutils.widgets.text("rollback_scope",     "FULL",            "FULL, TABLES_ONLY, VIEWS_ONLY, GRANTS_ONLY")
dbutils.widgets.text("dry_run",            "true",            "dry_run=true para simular (SEMPRE comece com true!)")
dbutils.widgets.text("confirm_rollback",   "false",           "Confirma rollback (deve ser 'true' para executar)")

uc_catalog_name    = dbutils.widgets.get("uc_catalog_name")
inventory_database = dbutils.widgets.get("inventory_database")
databases_filter   = dbutils.widgets.get("databases_filter")
rollback_scope     = dbutils.widgets.get("rollback_scope").upper()
dry_run            = dbutils.widgets.get("dry_run").lower() == "true"
confirm_rollback   = dbutils.widgets.get("confirm_rollback").lower() == "true"

if not dry_run and not confirm_rollback:
    raise Exception(
        "SEGURANÇA: Para executar o rollback real, defina confirm_rollback=true E dry_run=false. "
        "LEIA O PLANO COMPLETO ANTES DE CONFIRMAR."
    )

print(f"Catálogo UC:     {uc_catalog_name}")
print(f"Escopo:          {rollback_scope}")
print(f"Dry Run:         {dry_run}")
print(f"Confirmado:      {confirm_rollback}")
if not dry_run:
    print("\n⚠️  ATENÇÃO: ROLLBACK REAL SERÁ EXECUTADO!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificar Integridade das Tabelas Originais no Hive

# COMMAND ----------

from pyspark.sql.functions import col
from datetime import datetime

inventory_df  = spark.table(f"`{inventory_database}`.`hive_inventory`")
schema_map_df = spark.table(f"`{inventory_database}`.`schema_mapping`")

tables_df = (
    inventory_df
    .join(schema_map_df, inventory_df.database_name == schema_map_df.hive_database, "left")
    .select(
        inventory_df.database_name.alias("hive_db"),
        inventory_df.table_name,
        inventory_df.object_type,
        inventory_df.is_delta,
        inventory_df.estimated_rows,
        schema_map_df.uc_schema,
    )
)

if databases_filter.strip():
    tables_df = tables_df.filter(
        col("hive_db").isin([d.strip() for d in databases_filter.split(",")])
    )

tables_list = tables_df.collect()

print("Verificando tabelas originais no Hive...\n")

hive_integrity = []
for row in tables_list:
    hive_db    = row["hive_db"]
    table_name = row["table_name"]
    obj_type   = row["object_type"]

    try:
        if obj_type == "VIEW":
            spark.sql(f"SELECT 1 FROM hive_metastore.`{hive_db}`.`{table_name}` LIMIT 1")
        else:
            count = spark.sql(
                f"SELECT COUNT(*) as cnt FROM hive_metastore.`{hive_db}`.`{table_name}`"
            ).collect()[0]["cnt"]

        print(f"  OK: {hive_db}.{table_name}")
        hive_integrity.append({
            "table":  f"{hive_db}.{table_name}",
            "status": "OK",
            "error":  None
        })

    except Exception as e:
        print(f"  AUSENTE/ERRO: {hive_db}.{table_name}: {e}")
        hive_integrity.append({
            "table":  f"{hive_db}.{table_name}",
            "status": "MISSING_OR_ERROR",
            "error":  str(e)[:200]
        })

missing_in_hive = [r for r in hive_integrity if r["status"] != "OK"]
if missing_in_hive:
    print(f"\n⚠️  AVISO: {len(missing_in_hive)} tabelas ausentes ou com erro no Hive!")
    print("Rollback pode não restaurar completamente o acesso a estes objetos.")
else:
    print(f"\nTodas as {len(hive_integrity)} tabelas originais estão acessíveis no Hive.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Listar Objetos UC a Remover

# COMMAND ----------

def execute_sql(sql_stmt, dry_run=True, description=""):
    if dry_run:
        print(f"[DRY RUN] {description}")
        print(f"          SQL: {sql_stmt}")
        return True
    else:
        try:
            spark.sql(sql_stmt)
            print(f"  [OK] {description}")
            return True
        except Exception as e:
            if "does not exist" in str(e).lower() or "not found" in str(e).lower():
                print(f"  [SKIP] {description} (já não existe)")
                return True
            else:
                print(f"  [ERRO] {description}: {e}")
                return False

# Lista objetos UC existentes a remover
uc_objects_to_drop = []

print("Objetos UC a remover no rollback:\n")

for row in schema_map_df.collect():
    uc_schema = row["uc_schema"]
    hive_db   = row["hive_database"]

    # Filtra se necessário
    if databases_filter.strip():
        if hive_db not in [d.strip() for d in databases_filter.split(",")]:
            continue

    try:
        tables = spark.sql(
            f"SHOW TABLES IN `{uc_catalog_name}`.`{uc_schema}`"
        ).collect()

        for tbl in tables:
            is_view = tbl.get("isTemporary", False)
            obj_type = "VIEW" if is_view else "TABLE"
            obj_name = tbl["tableName"]

            uc_objects_to_drop.append({
                "uc_schema":    uc_schema,
                "object_name":  obj_name,
                "object_type":  obj_type,
                "hive_db":      hive_db,
            })

        print(f"  {uc_catalog_name}.{uc_schema}: {len(tables)} objetos")

    except Exception as e:
        print(f"  ERRO ao listar {uc_schema}: {e}")

print(f"\nTotal de objetos UC a remover: {len(uc_objects_to_drop)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Executar Rollback

# COMMAND ----------

rollback_results = []

# -------------------------------------------------------
# 3a. Remover Tabelas e Views do UC
# -------------------------------------------------------
if rollback_scope in ("FULL", "TABLES_ONLY", "VIEWS_ONLY"):
    print(f"\n=== REMOVENDO OBJETOS DO UC ({rollback_scope}) ===\n")

    for obj in uc_objects_to_drop:
        uc_schema   = obj["uc_schema"]
        object_name = obj["object_name"]
        object_type = obj["object_type"]

        if rollback_scope == "TABLES_ONLY" and object_type == "VIEW":
            continue
        if rollback_scope == "VIEWS_ONLY" and object_type == "TABLE":
            continue

        drop_sql = f"DROP {object_type} IF EXISTS `{uc_catalog_name}`.`{uc_schema}`.`{object_name}`"
        description = f"DROP {object_type} {uc_catalog_name}.{uc_schema}.{object_name}"

        success = execute_sql(drop_sql, dry_run=dry_run, description=description)

        rollback_results.append({
            "action":      f"DROP_{object_type}",
            "uc_object":   f"{uc_catalog_name}.{uc_schema}.{object_name}",
            "hive_object": f"{obj['hive_db']}.{object_name}",
            "status":      "DRY_RUN" if dry_run else ("SUCCESS" if success else "ERROR"),
            "rolled_back_at": datetime.now().isoformat(),
        })

# -------------------------------------------------------
# 3b. Remover Schemas UC (apenas se remover todos os objetos)
# -------------------------------------------------------
if rollback_scope == "FULL" and not databases_filter.strip():
    print("\n=== REMOVENDO SCHEMAS DO UC ===\n")

    for row in schema_map_df.collect():
        uc_schema = row["uc_schema"]
        drop_schema_sql = f"DROP SCHEMA IF EXISTS `{uc_catalog_name}`.`{uc_schema}` CASCADE"
        execute_sql(
            drop_schema_sql,
            dry_run=dry_run,
            description=f"DROP SCHEMA {uc_catalog_name}.{uc_schema}"
        )

    if rollback_scope == "FULL" and not databases_filter.strip():
        drop_catalog_sql = f"DROP CATALOG IF EXISTS `{uc_catalog_name}` CASCADE"
        execute_sql(
            drop_catalog_sql,
            dry_run=dry_run,
            description=f"DROP CATALOG {uc_catalog_name}"
        )

# -------------------------------------------------------
# 3c. Revogar Grants do UC
# -------------------------------------------------------
if rollback_scope in ("FULL", "GRANTS_ONLY"):
    print("\n=== REVOGANDO GRANTS DO UC ===\n")

    try:
        perms_log = spark.table(f"`{inventory_database}`.`permissions_migration_log`")
        applied_grants = perms_log.filter(col("status").isin(["SUCCESS"])).collect()

        for g in applied_grants:
            revoke_sql = g["grant_sql"].replace("GRANT", "REVOKE", 1).replace(" TO ", " FROM ")
            execute_sql(
                revoke_sql,
                dry_run=dry_run,
                description=f"REVOKE: {g['principal']} em {g['uc_object']}"
            )
    except Exception as e:
        print(f"Log de permissões não encontrado: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificar Acesso ao Hive Após Rollback

# COMMAND ----------

if not dry_run:
    print("\nVerificando acesso ao Hive após rollback...\n")
    ok_count   = 0
    fail_count = 0

    for row in tables_list[:20]:  # Amostra
        hive_db    = row["hive_db"]
        table_name = row["table_name"]
        try:
            spark.sql(
                f"SELECT 1 FROM hive_metastore.`{hive_db}`.`{table_name}` LIMIT 1"
            )
            print(f"  OK: hive_metastore.{hive_db}.{table_name}")
            ok_count += 1
        except Exception as e:
            print(f"  ERRO: hive_metastore.{hive_db}.{table_name}: {e}")
            fail_count += 1

    print(f"\nHive acessível: {ok_count} / {ok_count + fail_count} tabelas verificadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resumo do Rollback

# COMMAND ----------

success_rb = len([r for r in rollback_results if r["status"] == "SUCCESS"])
error_rb   = len([r for r in rollback_results if r["status"] == "ERROR"])
dry_rb     = len([r for r in rollback_results if r["status"] == "DRY_RUN"])

print("=" * 60)
print("RESUMO DO ROLLBACK")
print("=" * 60)
print(f"  Objetos removidos (SUCCESS): {success_rb}")
print(f"  Erros:                       {error_rb}")
print(f"  Dry Run (simulados):         {dry_rb}")

if dry_run:
    print("\n" + "=" * 60)
    print("DRY RUN CONCLUÍDO - Nenhuma alteração foi feita.")
    print("Para executar: defina dry_run=false E confirm_rollback=true")
    print("=" * 60)

# Salva log do rollback
if rollback_results:
    rb_df = spark.createDataFrame(rollback_results)
    (rb_df.write.format("delta").mode("append")
     .option("mergeSchema", "true")
     .saveAsTable(f"`{inventory_database}`.`rollback_log`"))
    print(f"\nLog de rollback salvo em: {inventory_database}.rollback_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Próximas Ações Pós-Rollback
# MAGIC
# MAGIC Após executar o rollback:
# MAGIC
# MAGIC 1. **Verificar pipelines/jobs** — Confirmar que todos os jobs estão usando `hive_metastore` novamente
# MAGIC 2. **Notificar equipes** — Comunicar que o ambiente voltou para o Hive
# MAGIC 3. **Analisar causa raiz** — Identificar e corrigir o motivo do rollback
# MAGIC 4. **Revisar plano de migração** — Ajustar a estratégia antes de tentar novamente
# MAGIC 5. **Documentar incidente** — Registrar o que ocorreu e as ações tomadas

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
    "rollback_scope":    rollback_scope,
    "dry_run":           dry_run,
    "objects_removed":   success_rb,
    "errors":            error_rb,
    "hive_tables_ok":    len([r for r in hive_integrity if r["status"] == "OK"]),
    "hive_tables_missing": len(missing_in_hive),
}))
