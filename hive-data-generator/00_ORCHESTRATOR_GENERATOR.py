# Databricks notebook source

# MAGIC %md
# MAGIC # Orquestrador - Geração de Dados Faker no Hive
# MAGIC
# MAGIC Executa todos os notebooks de geração em sequência.
# MAGIC Após a execução, o ambiente Hive estará populado e pronto
# MAGIC para testes de migração para o Unity Catalog.
# MAGIC
# MAGIC ## Dados gerados
# MAGIC
# MAGIC | Database | Tabelas | Total aprox. |
# MAGIC |----------|---------|-------------|
# MAGIC | serasa_clientes | clientes_pf, clientes_pj, enderecos, contatos, vw_clientes_ativos | ~25k linhas |
# MAGIC | serasa_credito | score_historico, consultas_credito, limites_credito, vw_score_atual | ~290k linhas |
# MAGIC | serasa_financeiro | transacoes, contratos | ~115k linhas |
# MAGIC | serasa_inadimplencia | ocorrencias, protestos, vw_inadimplentes_ativos | ~9.6k linhas |
# MAGIC | serasa_produtos | catalogo_produtos, assinaturas, uso_produto, precos_historico | ~46k linhas |

# COMMAND ----------

dbutils.widgets.text("num_clientes_pf", "10000", "Qtd clientes PF (base para todos os domínios)")
dbutils.widgets.text("external_path",   "dbfs:/tmp/serasa_ext", "Path base para tabelas EXTERNAL")
dbutils.widgets.text("drop_if_exists",  "false", "Recriar tudo do zero?")

NUM_PF         = dbutils.widgets.get("num_clientes_pf")
EXTERNAL_PATH  = dbutils.widgets.get("external_path")
DROP_IF_EXISTS = dbutils.widgets.get("drop_if_exists")

NOTEBOOK_BASE = "/Workspace/Repos/serasa/hive-data-generator"

# COMMAND ----------

import json
from datetime import datetime

results = {}
start   = datetime.now()

def run(path, params, label):
    t0 = datetime.now()
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] {label}...")
    try:
        out = dbutils.notebook.run(f"{NOTEBOOK_BASE}/{path}", 1800, params)
        dur = (datetime.now() - t0).total_seconds()
        print(f"  OK ({dur:.0f}s)")
        results[label] = {"status": "OK", "duration_s": dur, "output": json.loads(out or "{}")}
    except Exception as e:
        dur = (datetime.now() - t0).total_seconds()
        print(f"  ERRO ({dur:.0f}s): {e}")
        results[label] = {"status": "ERRO", "duration_s": dur, "error": str(e)[:300]}
        raise

common = {"num_clientes_pf": NUM_PF, "drop_if_exists": DROP_IF_EXISTS}

# COMMAND ----------

run("02_create_hive_databases", {"drop_if_exists": DROP_IF_EXISTS},                            "Criar Databases")
run("03_generate_clientes",     {**common, "external_path": f"{EXTERNAL_PATH}/clientes"},      "serasa_clientes")
run("04_generate_credito",      {**common, "meses_historico": "24", "num_consultas": "50000"}, "serasa_credito")
run("05_generate_financeiro_inadimplencia", {**common, "num_transacoes": "100000", "num_contratos": "15000", "num_ocorrencias": "8000", "external_path": f"{EXTERNAL_PATH}/financeiro"}, "serasa_financeiro + serasa_inadimplencia")
run("06_generate_produtos",     {**common, "external_path": f"{EXTERNAL_PATH}/produtos"},      "serasa_produtos")

# COMMAND ----------

total = (datetime.now() - start).total_seconds()
print(f"\n{'='*55}")
print(f"GERAÇÃO CONCLUÍDA — {total:.0f}s ({total/60:.1f} min)")
print(f"{'='*55}")
for label, r in results.items():
    sym = "OK" if r["status"] == "OK" else "ERRO"
    print(f"  [{sym}] {label} ({r['duration_s']:.0f}s)")

print("\nAmbiente Hive pronto para testes de migração.")
