# Databricks notebook source

# MAGIC %md
# MAGIC # 05 - Gerar Dados: serasa_financeiro + serasa_inadimplencia
# MAGIC
# MAGIC Gera dados dos domínios financeiro e de inadimplência em um único notebook
# MAGIC para aproveitar os dados de clientes já em memória.
# MAGIC
# MAGIC **Tabelas geradas:**
# MAGIC
# MAGIC `serasa_financeiro`:
# MAGIC | Tabela | Tipo | Formato | Partição |
# MAGIC |--------|------|---------|----------|
# MAGIC | `transacoes` | MANAGED | Delta | ano_mes |
# MAGIC | `contratos` | EXTERNAL | ORC | status |
# MAGIC
# MAGIC `serasa_inadimplencia`:
# MAGIC | Tabela | Tipo | Formato | Partição |
# MAGIC |--------|------|---------|----------|
# MAGIC | `ocorrencias` | MANAGED | Delta | ano_mes |
# MAGIC | `protestos` | MANAGED | Delta | uf |
# MAGIC | `vw_inadimplentes_ativos` | VIEW | — | — |

# COMMAND ----------

# MAGIC %pip install faker==24.0.0 --quiet

# COMMAND ----------

dbutils.widgets.text("num_clientes_pf",  "10000", "Qtd clientes PF")
dbutils.widgets.text("num_transacoes",   "100000","Qtd de transações financeiras")
dbutils.widgets.text("num_contratos",    "15000", "Qtd de contratos")
dbutils.widgets.text("num_ocorrencias",  "8000",  "Qtd de ocorrências de inadimplência")
dbutils.widgets.text("external_path",    "dbfs:/tmp/serasa_ext/financeiro", "Path EXTERNAL")
dbutils.widgets.text("drop_if_exists",   "false", "Recriar tabelas?")

NUM_PF         = int(dbutils.widgets.get("num_clientes_pf"))
NUM_TRANSACOES = int(dbutils.widgets.get("num_transacoes"))
NUM_CONTRATOS  = int(dbutils.widgets.get("num_contratos"))
NUM_OCORRENCIAS= int(dbutils.widgets.get("num_ocorrencias"))
EXTERNAL_PATH  = dbutils.widgets.get("external_path")
DROP_IF_EXISTS = dbutils.widgets.get("drop_if_exists").lower() == "true"

# COMMAND ----------

from faker import Faker
import random
from datetime import date
from dateutil.relativedelta import relativedelta
from pyspark.sql import Row

fake = Faker("pt_BR")
Faker.seed(42)
random.seed(42)

ESTADOS    = ["SP","RJ","MG","RS","PR","SC","BA","GO","PE","CE","AM","PA","MT","MS","ES"]
MODALIDADES= ["CARTAO_CREDITO","CREDITO_PESSOAL","FINANCIAMENTO_VEICULO","CREDITO_IMOBILIARIO","CONSIGNADO","CDC"]
BANCOS     = ["BANCO_DO_BRASIL","CAIXA","ITAU","BRADESCO","SANTANDER","NUBANK","INTER","C6","SICOOB","SICREDI"]
TIPOS_TRANS= ["COMPRA","SAQUE","TRANSFERENCIA","PIX","PAGAMENTO_BOLETO","ESTORNO","TARIFA","JUROS","IOF"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. serasa_financeiro.transacoes (Delta Managed)

# COMMAND ----------

print(f"Gerando {NUM_TRANSACOES:,} transações financeiras...")

trans_rows = []
today = date.today()

for i in range(NUM_TRANSACOES):
    dt_trans = fake.date_between(start_date="-24m", end_date="today")
    trans_rows.append(Row(
        id_transacao    = i + 1,
        id_cliente      = random.randint(1, NUM_PF),
        tipo            = random.choice(TIPOS_TRANS),
        modalidade      = random.choice(MODALIDADES),
        banco_origem    = random.choice(BANCOS),
        banco_destino   = random.choice(BANCOS + [None]),
        valor           = round(random.uniform(1.0, 50_000.0), 2),
        valor_juros     = round(random.uniform(0.0, 500.0), 2),
        valor_multa     = round(random.uniform(0.0, 200.0), 2),
        parcela_atual   = random.randint(1, 60),
        total_parcelas  = random.randint(1, 60),
        status          = random.choice(["PAGO","PENDENTE","ATRASADO","CANCELADO","ESTORNADO"]),
        canal           = random.choice(["APP","WEB","PDV","ATM","TEF"]),
        dt_transacao    = dt_trans,
        dt_vencimento   = dt_trans + relativedelta(days=random.randint(0, 30)),
        dt_pagamento    = dt_trans if random.random() < 0.8 else None,
        ano_mes         = int(dt_trans.strftime("%Y%m")),
        flag_fraude     = random.random() < 0.005,
    ))

trans_df = spark.createDataFrame(trans_rows)

if DROP_IF_EXISTS:
    spark.sql("DROP TABLE IF EXISTS `serasa_financeiro`.`transacoes`")

(trans_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ano_mes")
    .saveAsTable("`serasa_financeiro`.`transacoes`"))

print(f"  transacoes: {trans_df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. serasa_financeiro.contratos (EXTERNAL ORC)

# COMMAND ----------

print(f"Gerando {NUM_CONTRATOS:,} contratos (EXTERNAL / ORC)...")

contrato_rows = []
for i in range(NUM_CONTRATOS):
    dt_inicio = fake.date_between(start_date="-5y", end_date="today")
    prazo     = random.choice([6,12,18,24,36,48,60,72,84,120])
    contrato_rows.append(Row(
        id_contrato         = i + 1,
        id_cliente          = random.randint(1, NUM_PF),
        numero_contrato     = fake.bothify("CTR-####-????-####").upper(),
        modalidade          = random.choice(MODALIDADES),
        banco               = random.choice(BANCOS),
        valor_contratado    = round(random.uniform(1_000.0, 500_000.0), 2),
        valor_saldo_devedor = round(random.uniform(0.0, 500_000.0), 2),
        taxa_juros_mensal   = round(random.uniform(0.5, 15.0), 2),
        prazo_meses         = prazo,
        parcela_valor       = round(random.uniform(100.0, 10_000.0), 2),
        parcelas_pagas      = random.randint(0, prazo),
        parcelas_em_atraso  = random.randint(0, 6),
        status              = random.choice(["ATIVO","LIQUIDADO","INADIMPLENTE","RENEGOCIADO","CANCELADO"]),
        dt_inicio           = dt_inicio,
        dt_fim_previsto     = dt_inicio + relativedelta(months=prazo),
        dt_liquidacao       = None if random.random() < 0.7 else fake.date_between(start_date=dt_inicio, end_date="today"),
        uf                  = random.choice(ESTADOS),
    ))

contratos_df = spark.createDataFrame(contrato_rows)
ext_contratos_path = f"{EXTERNAL_PATH}/contratos"

(contratos_df.write
    .format("orc")
    .mode("overwrite")
    .partitionBy("status")
    .save(ext_contratos_path))

if DROP_IF_EXISTS:
    spark.sql("DROP TABLE IF EXISTS `serasa_financeiro`.`contratos`")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `serasa_financeiro`.`contratos` (
        id_contrato          BIGINT,
        id_cliente           BIGINT,
        numero_contrato      STRING,
        modalidade           STRING,
        banco                STRING,
        valor_contratado     DOUBLE,
        valor_saldo_devedor  DOUBLE,
        taxa_juros_mensal    DOUBLE,
        prazo_meses          BIGINT,
        parcela_valor        DOUBLE,
        parcelas_pagas       BIGINT,
        parcelas_em_atraso   BIGINT,
        dt_inicio            DATE,
        dt_fim_previsto      DATE,
        dt_liquidacao        DATE,
        uf                   STRING,
        status               STRING
    )
    USING ORC
    PARTITIONED BY (status)
    LOCATION '{ext_contratos_path}'
""")
spark.sql("MSCK REPAIR TABLE `serasa_financeiro`.`contratos`")

print(f"  contratos: {contratos_df.count():,} registros (EXTERNAL ORC)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. serasa_inadimplencia.ocorrencias (Delta Managed)

# COMMAND ----------

print(f"Gerando {NUM_OCORRENCIAS:,} ocorrências de inadimplência...")

ocorr_rows = []
for i in range(NUM_OCORRENCIAS):
    dt_ocorr = fake.date_between(start_date="-5y", end_date="today")
    ocorr_rows.append(Row(
        id_ocorrencia       = i + 1,
        id_cliente          = random.randint(1, NUM_PF),
        tipo_ocorrencia     = random.choice(["ATRASO","NEGATIVACAO","PROTESTO","ACAO_JUDICIAL","FALENCIA","CONCORDATA"]),
        credor              = random.choice(BANCOS + ["LOJA_A","LOJA_B","CONCESSIONARIA","PLANO_SAUDE","ESCOLA"]),
        valor_divida        = round(random.uniform(50.0, 200_000.0), 2),
        valor_atualizado    = round(random.uniform(50.0, 250_000.0), 2),
        dias_atraso         = random.randint(1, 1800),
        status              = random.choice(["ATIVA","LIQUIDADA","PRESCRITA","CONTESTADA","SUSPENSA"]),
        modalidade          = random.choice(MODALIDADES + ["AGUA","LUZ","TELEFONIA","CONDOMINIO","ALUGUEL"]),
        dt_ocorrencia       = dt_ocorr,
        dt_vencimento_orig  = dt_ocorr - relativedelta(days=random.randint(1,1800)),
        dt_liquidacao       = None if random.random() < 0.6 else fake.date_between(start_date=dt_ocorr, end_date="today"),
        contrato_origem     = fake.bothify("CTR-####-????").upper(),
        fase_cobranca       = random.choice(["PREVENTIVA","ADMINISTRATIVA","JURIDICA","NEGATIVACAO","PRESCRICAO"]),
        ano_mes             = int(dt_ocorr.strftime("%Y%m")),
    ))

ocorr_df = spark.createDataFrame(ocorr_rows)

if DROP_IF_EXISTS:
    spark.sql("DROP TABLE IF EXISTS `serasa_inadimplencia`.`ocorrencias`")

(ocorr_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ano_mes")
    .saveAsTable("`serasa_inadimplencia`.`ocorrencias`"))

print(f"  ocorrencias: {ocorr_df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. serasa_inadimplencia.protestos (Delta Managed)

# COMMAND ----------

num_protestos = NUM_OCORRENCIAS // 5
print(f"Gerando {num_protestos:,} protestos...")

protest_rows = []
for i in range(num_protestos):
    dt_prot = fake.date_between(start_date="-5y", end_date="today")
    protest_rows.append(Row(
        id_protesto         = i + 1,
        id_cliente          = random.randint(1, NUM_PF),
        cartorio             = fake.bothify("CARTORIO ?? OFICIO").upper(),
        numero_protesto     = fake.bothify("####/##"),
        uf                  = random.choice(ESTADOS),
        cidade              = fake.city(),
        valor_protestado    = round(random.uniform(100.0, 500_000.0), 2),
        credor              = random.choice(BANCOS + ["LOJA_A","FORNECEDOR_A"]),
        status              = random.choice(["ATIVO","CANCELADO","PAGO","PRESCRITO"]),
        dt_protesto         = dt_prot,
        dt_cancelamento     = None if random.random() < 0.7 else fake.date_between(start_date=dt_prot, end_date="today"),
        tipo_titulo         = random.choice(["NOTA_PROMISSORIA","DUPLICATA","CHEQUE","LETRA_CAMBIO","CCB"]),
    ))

prot_df = spark.createDataFrame(protest_rows)

if DROP_IF_EXISTS:
    spark.sql("DROP TABLE IF EXISTS `serasa_inadimplencia`.`protestos`")

(prot_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("uf")
    .saveAsTable("`serasa_inadimplencia`.`protestos`"))

print(f"  protestos: {prot_df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. View: vw_inadimplentes_ativos

# COMMAND ----------

spark.sql("DROP VIEW IF EXISTS `serasa_inadimplencia`.`vw_inadimplentes_ativos`")
spark.sql("""
    CREATE VIEW `serasa_inadimplencia`.`vw_inadimplentes_ativos` AS
    SELECT
        o.id_cliente,
        COUNT(o.id_ocorrencia)          AS qtd_ocorrencias,
        SUM(o.valor_atualizado)         AS total_divida,
        MAX(o.dias_atraso)              AS max_dias_atraso,
        MIN(o.dt_ocorrencia)            AS primeira_ocorrencia,
        MAX(o.dt_ocorrencia)            AS ultima_ocorrencia,
        COUNT(DISTINCT o.credor)        AS qtd_credores,
        COALESCE(p.qtd_protestos, 0)    AS qtd_protestos,
        COALESCE(p.valor_protestado, 0) AS total_protestado
    FROM `serasa_inadimplencia`.`ocorrencias` o
    LEFT JOIN (
        SELECT
            id_cliente,
            COUNT(*)        AS qtd_protestos,
            SUM(valor_protestado) AS valor_protestado
        FROM `serasa_inadimplencia`.`protestos`
        WHERE status = 'ATIVO'
        GROUP BY id_cliente
    ) p ON o.id_cliente = p.id_cliente
    WHERE o.status = 'ATIVA'
    GROUP BY o.id_cliente, p.qtd_protestos, p.valor_protestado
""")

print("  vw_inadimplentes_ativos criada")

# COMMAND ----------

print("\n=== serasa_financeiro ===")
spark.sql("SHOW TABLES IN `serasa_financeiro`").show()
print("\n=== serasa_inadimplencia ===")
spark.sql("SHOW TABLES IN `serasa_inadimplencia`").show()

import json
dbutils.notebook.exit(json.dumps({
    "transacoes": NUM_TRANSACOES,
    "contratos":  NUM_CONTRATOS,
    "ocorrencias": NUM_OCORRENCIAS,
    "protestos":  num_protestos,
}))
