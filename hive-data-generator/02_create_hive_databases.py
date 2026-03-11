# Databricks notebook source

# MAGIC %md
# MAGIC # 02 - Criar Databases no Hive Metastore
# MAGIC
# MAGIC Cria a estrutura de databases que simula um ambiente real de dados da Serasa,
# MAGIC com diferentes domínios de negócio.

# COMMAND ----------

dbutils.widgets.text("base_path", "dbfs:/user/hive/warehouse", "Path base para os databases")
dbutils.widgets.text("drop_if_exists", "false", "Recriar databases (drop + create)?")

base_path      = dbutils.widgets.get("base_path").rstrip("/")
drop_if_exists = dbutils.widgets.get("drop_if_exists").lower() == "true"

# COMMAND ----------

# Definição dos databases simulando domínios da Serasa
DATABASES = {
    "serasa_clientes": {
        "comment": "Domínio de dados cadastrais de clientes PF e PJ",
        "location": f"{base_path}/serasa_clientes.db",
    },
    "serasa_credito": {
        "comment": "Domínio de análise e score de crédito",
        "location": f"{base_path}/serasa_credito.db",
    },
    "serasa_financeiro": {
        "comment": "Domínio de transações e movimentações financeiras",
        "location": f"{base_path}/serasa_financeiro.db",
    },
    "serasa_inadimplencia": {
        "comment": "Domínio de registros de inadimplência e negativação",
        "location": f"{base_path}/serasa_inadimplencia.db",
    },
    "serasa_produtos": {
        "comment": "Domínio de produtos e serviços contratados",
        "location": f"{base_path}/serasa_produtos.db",
    },
}

# COMMAND ----------

for db_name, db_config in DATABASES.items():
    if drop_if_exists:
        spark.sql(f"DROP DATABASE IF EXISTS `{db_name}` CASCADE")
        print(f"  DROP: {db_name}")

    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS `{db_name}`
        COMMENT '{db_config["comment"]}'
        LOCATION '{db_config["location"]}'
    """)
    print(f"  OK: {db_name}")

print(f"\n{len(DATABASES)} databases criados no Hive Metastore.")
spark.sql("SHOW DATABASES").show()
