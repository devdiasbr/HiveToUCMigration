# Databricks notebook source

# MAGIC %md
# MAGIC # 04 - Gerar Dados: serasa_credito
# MAGIC
# MAGIC Cria e popula as tabelas do domínio de crédito.
# MAGIC
# MAGIC **Tabelas geradas:**
# MAGIC | Tabela | Tipo | Formato | Partição |
# MAGIC |--------|------|---------|----------|
# MAGIC | `score_historico` | MANAGED | Delta | ano_mes |
# MAGIC | `consultas_credito` | MANAGED | Delta | ano_mes |
# MAGIC | `limites_credito` | MANAGED | Delta | — |
# MAGIC | `vw_score_atual` | VIEW | — | — |

# COMMAND ----------

# MAGIC %pip install faker==24.0.0 --quiet

# COMMAND ----------

dbutils.widgets.text("num_clientes_pf",  "10000", "Qtd clientes PF (deve bater com gerador de clientes)")
dbutils.widgets.text("meses_historico",  "24",    "Meses de histórico de score")
dbutils.widgets.text("num_consultas",    "50000", "Qtd de consultas de crédito")
dbutils.widgets.text("drop_if_exists",   "false", "Recriar tabelas?")

NUM_PF        = int(dbutils.widgets.get("num_clientes_pf"))
MESES_HIST    = int(dbutils.widgets.get("meses_historico"))
NUM_CONSULTAS = int(dbutils.widgets.get("num_consultas"))
DROP_IF_EXISTS = dbutils.widgets.get("drop_if_exists").lower() == "true"
DATABASE      = "serasa_credito"

# COMMAND ----------

from faker import Faker
import random
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import Row

fake = Faker("pt_BR")
Faker.seed(42)
random.seed(42)

MODALIDADES  = ["CARTAO_CREDITO","CREDITO_PESSOAL","FINANCIAMENTO_VEICULO","CREDITO_IMOBILIARIO","CHEQUE_ESPECIAL","CONSIGNADO","CDC","MICROCRÉDITO"]
FINALIDADES  = ["CONSUMO","INVESTIMENTO","CAPITAL_GIRO","REFINANCIAMENTO","EMERGENCIA","EDUCACAO","VIAGEM","REFORMA"]
CONSULTORES  = ["BANCO_A","BANCO_B","FINTECH_A","FINTECH_B","VAREJO_A","VAREJO_B","TELCO","SEGURADORA","LOCADORA"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tabela: score_historico (Delta Managed, particionado por ano_mes)

# COMMAND ----------

print(f"Gerando {NUM_PF} × {MESES_HIST} = {NUM_PF * MESES_HIST:,} registros de histórico de score...")

score_rows = []
today = date.today()

for id_cliente in range(1, NUM_PF + 1):
    score_base = random.randint(200, 900)
    for m in range(MESES_HIST, 0, -1):
        ref_date  = today - relativedelta(months=m)
        ano_mes   = int(ref_date.strftime("%Y%m"))
        variacao  = random.randint(-40, 40)
        score_base = max(0, min(1000, score_base + variacao))

        score_rows.append(Row(
            id_cliente          = id_cliente,
            ano_mes             = ano_mes,
            score               = score_base,
            faixa_score         = ("A" if score_base >= 800 else
                                   "B" if score_base >= 600 else
                                   "C" if score_base >= 400 else
                                   "D" if score_base >= 200 else "E"),
            num_consultas_mes   = random.randint(0, 8),
            num_dividas_ativas  = random.randint(0, 5),
            valor_dividas       = round(random.uniform(0, 50000), 2),
            flag_negativado     = random.random() < 0.12,
            flag_falencia       = random.random() < 0.005,
            modelo_score        = random.choice(["MODELO_V1","MODELO_V2","MODELO_V3"]),
            dt_calculo          = ref_date,
        ))

score_df = spark.createDataFrame(score_rows)

if DROP_IF_EXISTS:
    spark.sql(f"DROP TABLE IF EXISTS `{DATABASE}`.`score_historico`")

(score_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{DATABASE}`.`score_historico`"))

print(f"  score_historico: {score_df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tabela: consultas_credito (Delta Managed)

# COMMAND ----------

print(f"Gerando {NUM_CONSULTAS:,} consultas de crédito...")

consulta_rows = []
for i in range(NUM_CONSULTAS):
    dt_consulta = fake.date_between(start_date="-24m", end_date="today")
    consulta_rows.append(Row(
        id_consulta     = i + 1,
        id_cliente      = random.randint(1, NUM_PF),
        consultor       = random.choice(CONSULTORES),
        modalidade      = random.choice(MODALIDADES),
        finalidade      = random.choice(FINALIDADES),
        valor_solicitado= round(random.uniform(500.0, 500_000.0), 2),
        score_momento   = random.randint(0, 1000),
        resultado       = random.choice(["APROVADO","REPROVADO","PENDENTE","CANCELADO"]),
        motivo_reprovacao= random.choice(["","SCORE_BAIXO","RENDA_INSUFICIENTE","NEGATIVADO","DOCUMENTACAO","POLITICA_INTERNA"]),
        taxa_ofertada   = round(random.uniform(0.9, 15.5), 2),
        prazo_meses     = random.choice([6,12,18,24,36,48,60,72,84]),
        dt_consulta     = dt_consulta,
        ano_mes         = int(dt_consulta.strftime("%Y%m")),
        canal           = random.choice(["APP","WEB","PDV","TELEFONE","API"]),
    ))

consultas_df = spark.createDataFrame(consulta_rows)

if DROP_IF_EXISTS:
    spark.sql(f"DROP TABLE IF EXISTS `{DATABASE}`.`consultas_credito`")

(consultas_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{DATABASE}`.`consultas_credito`"))

print(f"  consultas_credito: {consultas_df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tabela: limites_credito (Delta Managed)

# COMMAND ----------

print(f"Gerando {NUM_PF:,} registros de limites de crédito...")

limite_rows = []
for i in range(1, NUM_PF + 1):
    limite_rows.append(Row(
        id_cliente          = i,
        modalidade          = random.choice(MODALIDADES),
        limite_aprovado     = round(random.uniform(500.0, 100_000.0), 2),
        limite_disponivel   = round(random.uniform(0.0, 100_000.0), 2),
        limite_utilizado    = round(random.uniform(0.0, 100_000.0), 2),
        taxa_juros_mensal   = round(random.uniform(0.9, 12.5), 2),
        taxa_juros_anual    = round(random.uniform(11.0, 260.0), 2),
        prazo_max_meses     = random.choice([12,24,36,48,60,72,84]),
        score_aprovacao     = random.randint(0, 1000),
        dt_aprovacao        = fake.date_between(start_date="-3y", end_date="today"),
        dt_vencimento       = fake.date_between(start_date="today", end_date="+2y"),
        status              = random.choice(["ATIVO","SUSPENSO","CANCELADO","VENCIDO"]),
        flag_pre_aprovado   = random.random() < 0.3,
    ))

limites_df = spark.createDataFrame(limite_rows)

if DROP_IF_EXISTS:
    spark.sql(f"DROP TABLE IF EXISTS `{DATABASE}`.`limites_credito`")

(limites_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{DATABASE}`.`limites_credito`"))

print(f"  limites_credito: {limites_df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. View: vw_score_atual

# COMMAND ----------

spark.sql(f"DROP VIEW IF EXISTS `{DATABASE}`.`vw_score_atual`")
spark.sql(f"""
    CREATE VIEW `{DATABASE}`.`vw_score_atual` AS
    SELECT
        s.id_cliente,
        s.score,
        s.faixa_score,
        s.num_consultas_mes,
        s.num_dividas_ativas,
        s.valor_dividas,
        s.flag_negativado,
        s.dt_calculo,
        l.limite_aprovado,
        l.limite_disponivel,
        l.taxa_juros_mensal
    FROM `{DATABASE}`.`score_historico` s
    INNER JOIN (
        SELECT id_cliente, MAX(ano_mes) AS ultimo_mes
        FROM `{DATABASE}`.`score_historico`
        GROUP BY id_cliente
    ) latest ON s.id_cliente = latest.id_cliente AND s.ano_mes = latest.ultimo_mes
    LEFT JOIN `{DATABASE}`.`limites_credito` l
        ON s.id_cliente = l.id_cliente AND l.status = 'ATIVO'
""")

print(f"  vw_score_atual criada")

# COMMAND ----------

print("=" * 50)
print(f"DATABASE: {DATABASE}")
print("=" * 50)
spark.sql(f"SHOW TABLES IN `{DATABASE}`").show()

import json
dbutils.notebook.exit(json.dumps({
    "database":          DATABASE,
    "score_historico":   NUM_PF * MESES_HIST,
    "consultas_credito": NUM_CONSULTAS,
    "limites_credito":   NUM_PF,
}))
