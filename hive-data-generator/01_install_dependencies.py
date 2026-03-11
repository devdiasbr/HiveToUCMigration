# Databricks notebook source

# MAGIC %md
# MAGIC # 01 - Instalar Dependências
# MAGIC
# MAGIC Instala a biblioteca `faker` no cluster antes de executar os notebooks de geração de dados.

# COMMAND ----------

# MAGIC %pip install faker==24.0.0

# COMMAND ----------

dbutils.library.restartPython()
