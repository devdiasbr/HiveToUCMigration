# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gerar Dados: serasa_clientes
# MAGIC
# MAGIC Cria e popula as tabelas do domínio de clientes usando Faker com locale `pt_BR`.
# MAGIC
# MAGIC **Tabelas geradas:**
# MAGIC | Tabela | Tipo | Formato | Linhas |
# MAGIC |--------|------|---------|--------|
# MAGIC | `clientes_pf` | MANAGED | Delta | configurável |
# MAGIC | `clientes_pj` | MANAGED | Delta | configurável |
# MAGIC | `enderecos` | MANAGED | Delta | 1 por cliente |
# MAGIC | `contatos` | EXTERNAL | Parquet | configurável |
# MAGIC | `vw_clientes_ativos` | VIEW | — | — |

# COMMAND ----------

# MAGIC %pip install faker==24.0.0 --quiet

# COMMAND ----------

dbutils.widgets.text("num_clientes_pf", "10000", "Qtd clientes PF")
dbutils.widgets.text("num_clientes_pj", "2000",  "Qtd clientes PJ")
dbutils.widgets.text("external_path",   "dbfs:/tmp/serasa_ext/clientes", "Path para tabelas EXTERNAL")
dbutils.widgets.text("drop_if_exists",  "false", "Recriar tabelas?")

NUM_PF         = int(dbutils.widgets.get("num_clientes_pf"))
NUM_PJ         = int(dbutils.widgets.get("num_clientes_pj"))
EXTERNAL_PATH  = dbutils.widgets.get("external_path")
DROP_IF_EXISTS = dbutils.widgets.get("drop_if_exists").lower() == "true"
DATABASE       = "serasa_clientes"

print(f"Gerando {NUM_PF} clientes PF e {NUM_PJ} PJ no database '{DATABASE}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

from faker import Faker
from faker.providers import bank, automotive
import random
from datetime import date, timedelta
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DateType, FloatType, BooleanType, LongType, TimestampType
)
from pyspark.sql.functions import col, lit, current_timestamp

fake = Faker("pt_BR")
Faker.seed(42)
random.seed(42)

def cpf_fake():
    nums = [random.randint(0, 9) for _ in range(9)]
    d1 = (sum((10 - i) * nums[i] for i in range(9)) * 10) % 11
    d1 = 0 if d1 >= 10 else d1
    nums.append(d1)
    d2 = (sum((11 - i) * nums[i] for i in range(10)) * 10) % 11
    d2 = 0 if d2 >= 10 else d2
    nums.append(d2)
    return f"{''.join(map(str, nums[:3]))}.{''.join(map(str, nums[3:6]))}.{''.join(map(str, nums[6:9]))}-{''.join(map(str, nums[9:]))}"

def cnpj_fake():
    n = [random.randint(0, 9) for _ in range(12)]
    def calc(nums, weights):
        s = sum(a * b for a, b in zip(nums, weights))
        r = s % 11
        return 0 if r < 2 else 11 - r
    d1 = calc(n, [5,4,3,2,9,8,7,6,5,4,3,2])
    n.append(d1)
    d2 = calc(n, [6,5,4,3,2,9,8,7,6,5,4,3,2])
    n.append(d2)
    return f"{''.join(map(str,n[:2]))}.{''.join(map(str,n[2:5]))}.{''.join(map(str,n[5:8]))}/{''.join(map(str,n[8:12]))}-{''.join(map(str,n[12:]))}"

ESTADOS = ["SP","RJ","MG","RS","PR","SC","BA","GO","PE","CE","AM","PA","MT","MS","ES","RN","PB","MA","PI","AL","SE","RO","TO","AC","AP","RR","DF"]
SCORES  = ["A","B","C","D","E"]
STATUS  = ["ATIVO", "INATIVO", "SUSPENSO", "BLOQUEADO"]
PORTES  = ["MEI", "ME", "EPP", "MEDIO", "GRANDE"]
SETORES = ["TECNOLOGIA","VAREJO","INDUSTRIA","SERVICOS","SAUDE","EDUCACAO","FINANCEIRO","AGRONEGOCIO","CONSTRUCAO","TRANSPORTE"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tabela: clientes_pf (Delta Managed)

# COMMAND ----------

print(f"Gerando {NUM_PF} registros de clientes_pf...")

pf_rows = []
for i in range(NUM_PF):
    dt_nasc = fake.date_of_birth(minimum_age=18, maximum_age=85)
    pf_rows.append(Row(
        id_cliente       = i + 1,
        cpf              = cpf_fake(),
        nome             = fake.name(),
        nome_mae         = fake.name_female(),
        data_nascimento  = dt_nasc,
        genero           = random.choice(["M", "F", "N"]),
        nacionalidade    = "Brasileira",
        estado_civil     = random.choice(["SOLTEIRO","CASADO","DIVORCIADO","VIUVO","UNIAO_ESTAVEL"]),
        escolaridade     = random.choice(["FUNDAMENTAL","MEDIO","SUPERIOR","POS_GRADUACAO","MESTRADO","DOUTORADO"]),
        renda_mensal     = round(random.uniform(1320.0, 35000.0), 2),
        score_serasa     = random.randint(0, 1000),
        faixa_score      = random.choice(SCORES),
        status           = random.choice(STATUS),
        uf_nascimento    = random.choice(ESTADOS),
        dt_cadastro      = fake.date_between(start_date="-10y", end_date="today"),
        dt_atualizacao   = fake.date_between(start_date="-1y",  end_date="today"),
        flag_pep         = random.random() < 0.02,   # 2% PEP
        flag_obito       = random.random() < 0.005,  # 0.5% óbito
        canal_cadastro   = random.choice(["WEB","APP","PARCEIRO","PRESENCIAL","API"]),
        origem           = random.choice(["ORGANICO","INDICACAO","CAMPANHA","COBRANCA"]),
    ))

pf_df = spark.createDataFrame(pf_rows)

if DROP_IF_EXISTS:
    spark.sql(f"DROP TABLE IF EXISTS `{DATABASE}`.`clientes_pf`")

(pf_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("faixa_score", "status")
    .saveAsTable(f"`{DATABASE}`.`clientes_pf`"))

count = spark.table(f"`{DATABASE}`.`clientes_pf`").count()
print(f"  clientes_pf: {count:,} registros")
display(spark.table(f"`{DATABASE}`.`clientes_pf`").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tabela: clientes_pj (Delta Managed)

# COMMAND ----------

print(f"Gerando {NUM_PJ} registros de clientes_pj...")

pj_rows = []
for i in range(NUM_PJ):
    pj_rows.append(Row(
        id_empresa       = i + 1,
        cnpj             = cnpj_fake(),
        razao_social     = fake.company(),
        nome_fantasia    = fake.company(),
        porte            = random.choice(PORTES),
        setor            = random.choice(SETORES),
        cnae_principal   = str(random.randint(1000, 9999)) + "-" + str(random.randint(1, 9)) + "/" + str(random.randint(10, 99)),
        faturamento_anual= round(random.uniform(81000.0, 300_000_000.0), 2),
        num_funcionarios = random.randint(1, 5000),
        score_serasa     = random.randint(0, 1000),
        faixa_score      = random.choice(SCORES),
        status           = random.choice(STATUS),
        uf_sede          = random.choice(ESTADOS),
        dt_abertura      = fake.date_between(start_date="-30y", end_date="-1y"),
        dt_cadastro      = fake.date_between(start_date="-10y", end_date="today"),
        dt_atualizacao   = fake.date_between(start_date="-1y",  end_date="today"),
        capital_social   = round(random.uniform(1000.0, 10_000_000.0), 2),
        optante_simples  = random.random() < 0.4,
        flag_mei         = random.random() < 0.2,
        situacao_receita = random.choice(["ATIVA","INAPTA","SUSPENSA","BAIXADA","NULA"]),
        canal_cadastro   = random.choice(["WEB","APP","PARCEIRO","API"]),
    ))

pj_df = spark.createDataFrame(pj_rows)

if DROP_IF_EXISTS:
    spark.sql(f"DROP TABLE IF EXISTS `{DATABASE}`.`clientes_pj`")

(pj_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("porte", "setor")
    .saveAsTable(f"`{DATABASE}`.`clientes_pj`"))

count = spark.table(f"`{DATABASE}`.`clientes_pj`").count()
print(f"  clientes_pj: {count:,} registros")
display(spark.table(f"`{DATABASE}`.`clientes_pj`").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tabela: enderecos (Delta Managed)

# COMMAND ----------

print("Gerando endereços (1 por cliente PF e PJ)...")

end_rows = []
seq_id = 1
for i in range(1, NUM_PF + 1):
    end_rows.append(Row(
        id_endereco     = seq_id,
        id_cliente      = i,
        tipo_cliente    = "PF",
        tipo_endereco   = random.choice(["RESIDENCIAL","COMERCIAL","COBRANCA","ENTREGA"]),
        cep             = fake.postcode(),
        logradouro      = fake.street_name(),
        numero          = str(random.randint(1, 9999)),
        complemento     = random.choice(["", "Apto " + str(random.randint(1,300)), "Casa", "Sala " + str(random.randint(1,50))]),
        bairro          = fake.bairro(),
        cidade          = fake.city(),
        uf              = random.choice(ESTADOS),
        pais            = "Brasil",
        latitude        = float(fake.latitude()),
        longitude       = float(fake.longitude()),
        flag_principal  = True,
        dt_cadastro     = fake.date_between(start_date="-5y", end_date="today"),
    ))
    seq_id += 1

for i in range(1, NUM_PJ + 1):
    end_rows.append(Row(
        id_endereco     = seq_id,
        id_cliente      = i,
        tipo_cliente    = "PJ",
        tipo_endereco   = random.choice(["SEDE","FILIAL","COBRANCA","ENTREGA"]),
        cep             = fake.postcode(),
        logradouro      = fake.street_name(),
        numero          = str(random.randint(1, 9999)),
        complemento     = random.choice(["", "Sala " + str(random.randint(1,50)), "Andar " + str(random.randint(1,30))]),
        bairro          = fake.bairro(),
        cidade          = fake.city(),
        uf              = random.choice(ESTADOS),
        pais            = "Brasil",
        latitude        = float(fake.latitude()),
        longitude       = float(fake.longitude()),
        flag_principal  = True,
        dt_cadastro     = fake.date_between(start_date="-5y", end_date="today"),
    ))
    seq_id += 1

end_df = spark.createDataFrame(end_rows)

if DROP_IF_EXISTS:
    spark.sql(f"DROP TABLE IF EXISTS `{DATABASE}`.`enderecos`")

(end_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("uf")
    .saveAsTable(f"`{DATABASE}`.`enderecos`"))

print(f"  enderecos: {end_df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tabela: contatos (EXTERNAL Parquet)

# COMMAND ----------

# DBTITLE 1,Cell 13
print("Gerando contatos (EXTERNAL / Parquet)...")

contato_rows = []
seq_id = 1
for i in range(1, NUM_PF + 1):
    n = random.randint(1, 3)
    for _ in range(n):
        contato_rows.append(Row(
            id_contato   = seq_id,
            id_cliente   = i,
            tipo_cliente = "PF",
            tipo_contato = random.choice(["CELULAR","RESIDENCIAL","COMERCIAL","EMAIL","WHATSAPP"]),
            valor        = fake.phone_number() if random.random() < 0.7 else fake.email(),
            flag_principal = seq_id % n == 0,
            flag_ativo   = random.random() < 0.85,
            dt_cadastro  = fake.date_between(start_date="-5y", end_date="today"),
        ))
        seq_id += 1

contatos_df = spark.createDataFrame(contato_rows)

ext_contatos_path = f"{EXTERNAL_PATH}/contatos"
(contatos_df.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("tipo_contato")
    .save(ext_contatos_path))

if DROP_IF_EXISTS:
    spark.sql(f"DROP TABLE IF EXISTS `{DATABASE}`.`contatos`")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{DATABASE}`.`contatos` (
        id_contato     BIGINT,
        id_cliente     BIGINT,
        tipo_cliente   STRING,
        valor          STRING,
        flag_principal BOOLEAN,
        flag_ativo     BOOLEAN,
        dt_cadastro    DATE,
        tipo_contato   STRING
    )
    USING PARQUET
    PARTITIONED BY (tipo_contato)
    LOCATION '{ext_contatos_path}'
""")
spark.sql(f"MSCK REPAIR TABLE `{DATABASE}`.`contatos`")

print(f"  contatos: {contatos_df.count():,} registros (EXTERNAL Parquet em {ext_contatos_path})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. View: vw_clientes_ativos

# COMMAND ----------

spark.sql(f"DROP VIEW IF EXISTS `{DATABASE}`.`vw_clientes_ativos`")
spark.sql(f"""
    CREATE VIEW `{DATABASE}`.`vw_clientes_ativos` AS
    SELECT
        'PF'                AS tipo_cliente,
        id_cliente,
        cpf                 AS documento,
        nome                AS razao_nome,
        score_serasa,
        faixa_score,
        uf_nascimento       AS uf,
        renda_mensal        AS capacidade_financeira,
        dt_cadastro
    FROM `{DATABASE}`.`clientes_pf`
    WHERE status = 'ATIVO'

    UNION ALL

    SELECT
        'PJ'                AS tipo_cliente,
        id_empresa          AS id_cliente,
        cnpj                AS documento,
        razao_social        AS razao_nome,
        score_serasa,
        faixa_score,
        uf_sede             AS uf,
        faturamento_anual   AS capacidade_financeira,
        dt_cadastro
    FROM `{DATABASE}`.`clientes_pj`
    WHERE status = 'ATIVO'
""")

count = spark.sql(f"SELECT COUNT(*) as cnt FROM `{DATABASE}`.`vw_clientes_ativos`").collect()[0]["cnt"]
print(f"  vw_clientes_ativos: {count:,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

print("=" * 50)
print(f"DATABASE: {DATABASE}")
print("=" * 50)
spark.sql(f"SHOW TABLES IN `{DATABASE}`").show()

import json
dbutils.notebook.exit(json.dumps({
    "database":     DATABASE,
    "clientes_pf":  NUM_PF,
    "clientes_pj":  NUM_PJ,
    "enderecos":    NUM_PF + NUM_PJ,
}))
