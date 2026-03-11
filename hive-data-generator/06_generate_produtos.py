# Databricks notebook source

# MAGIC %md
# MAGIC # 06 - Gerar Dados: serasa_produtos
# MAGIC
# MAGIC Gera dados do domínio de produtos e serviços contratados.
# MAGIC Inclui uma tabela CSV (External) para testar migração de formatos legados.
# MAGIC
# MAGIC **Tabelas geradas:**
# MAGIC | Tabela | Tipo | Formato | Notas |
# MAGIC |--------|------|---------|-------|
# MAGIC | `catalogo_produtos` | MANAGED | Delta | Tabela de referência |
# MAGIC | `assinaturas` | MANAGED | Delta | Assinaturas ativas/canceladas |
# MAGIC | `uso_produto` | MANAGED | Delta | Eventos de uso mensal |
# MAGIC | `precos_historico` | EXTERNAL | CSV | Histórico de preços (legado) |

# COMMAND ----------

# MAGIC %pip install faker==24.0.0 --quiet

# COMMAND ----------

dbutils.widgets.text("num_clientes_pf", "10000", "Qtd clientes PF")
dbutils.widgets.text("external_path",   "dbfs:/tmp/serasa_ext/produtos", "Path EXTERNAL")
dbutils.widgets.text("drop_if_exists",  "false", "Recriar tabelas?")

NUM_PF         = int(dbutils.widgets.get("num_clientes_pf"))
EXTERNAL_PATH  = dbutils.widgets.get("external_path")
DROP_IF_EXISTS = dbutils.widgets.get("drop_if_exists").lower() == "true"
DATABASE       = "serasa_produtos"

# COMMAND ----------

from faker import Faker
import random
from datetime import date
from dateutil.relativedelta import relativedelta
from pyspark.sql import Row

fake = Faker("pt_BR")
Faker.seed(42)
random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. catalogo_produtos (Delta Managed — tabela de referência pequena)

# COMMAND ----------

PRODUTOS = [
    Row(id_produto=1,  codigo="SCPF001", nome="Serasa Score PF",          categoria="SCORE",       preco_mensal=9.90,   plataforma="B2C", ativo=True),
    Row(id_produto=2,  codigo="SCPJ001", nome="Serasa Score PJ",          categoria="SCORE",       preco_mensal=29.90,  plataforma="B2B", ativo=True),
    Row(id_produto=3,  codigo="MON001",  nome="Serasa Monitora PF",       categoria="MONITORAMENTO",preco_mensal=14.90, plataforma="B2C", ativo=True),
    Row(id_produto=4,  codigo="MON002",  nome="Serasa Monitora Empresa",  categoria="MONITORAMENTO",preco_mensal=49.90, plataforma="B2B", ativo=True),
    Row(id_produto=5,  codigo="REL001",  nome="Relatório Completo PF",    categoria="RELATORIO",   preco_mensal=0.0,    plataforma="B2C", ativo=True),
    Row(id_produto=6,  codigo="REL002",  nome="Relatório Completo PJ",    categoria="RELATORIO",   preco_mensal=0.0,    plataforma="B2B", ativo=True),
    Row(id_produto=7,  codigo="NEG001",  nome="Negativação Digital",      categoria="COBRANCA",    preco_mensal=199.90, plataforma="B2B", ativo=True),
    Row(id_produto=8,  codigo="BOL001",  nome="Serasa Boleto",            categoria="COBRANCA",    preco_mensal=49.90,  plataforma="B2B", ativo=True),
    Row(id_produto=9,  codigo="CAD001",  nome="Cadastro Positivo",        categoria="CADASTRO",    preco_mensal=0.0,    plataforma="B2C", ativo=True),
    Row(id_produto=10, codigo="API001",  nome="API Consulta Score",       categoria="API",         preco_mensal=299.0,  plataforma="B2B", ativo=True),
    Row(id_produto=11, codigo="API002",  nome="API Dados Cadastrais",     categoria="API",         preco_mensal=499.0,  plataforma="B2B", ativo=True),
    Row(id_produto=12, codigo="ANT001",  nome="Antifraude Basico",        categoria="ANTIFRAUDE",  preco_mensal=149.90, plataforma="B2B", ativo=True),
    Row(id_produto=13, codigo="ANT002",  nome="Antifraude Premium",       categoria="ANTIFRAUDE",  preco_mensal=499.90, plataforma="B2B", ativo=True),
    Row(id_produto=14, codigo="OLD001",  nome="Score Legado v1",          categoria="SCORE",       preco_mensal=4.90,   plataforma="B2C", ativo=False),
    Row(id_produto=15, codigo="OLD002",  nome="Consulta Basica Legado",   categoria="RELATORIO",   preco_mensal=0.0,    plataforma="B2C", ativo=False),
]

catalog_df = spark.createDataFrame(PRODUTOS)

if DROP_IF_EXISTS:
    spark.sql(f"DROP TABLE IF EXISTS `{DATABASE}`.`catalogo_produtos`")

(catalog_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{DATABASE}`.`catalogo_produtos`"))

print(f"  catalogo_produtos: {len(PRODUTOS)} produtos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. assinaturas (Delta Managed)

# COMMAND ----------

num_assinaturas = int(NUM_PF * 1.5)  # média de 1.5 produtos por cliente
print(f"Gerando {num_assinaturas:,} assinaturas...")

produto_ids_ativos = [p.id_produto for p in PRODUTOS if p.ativo]
assin_rows = []

for i in range(num_assinaturas):
    dt_inicio = fake.date_between(start_date="-4y", end_date="today")
    prod_id   = random.choice(produto_ids_ativos)
    status    = random.choice(["ATIVA","CANCELADA","SUSPENSA","TRIAL"])
    assin_rows.append(Row(
        id_assinatura   = i + 1,
        id_cliente      = random.randint(1, NUM_PF),
        id_produto      = prod_id,
        plano           = random.choice(["MENSAL","TRIMESTRAL","SEMESTRAL","ANUAL"]),
        status          = status,
        valor_pago      = round(random.uniform(0.0, 600.0), 2),
        desconto_pct    = round(random.uniform(0.0, 40.0), 2),
        dt_inicio       = dt_inicio,
        dt_fim          = None if status == "ATIVA" else fake.date_between(start_date=dt_inicio, end_date="today"),
        dt_proximo_cobr = dt_inicio + relativedelta(months=random.randint(1, 3)) if status == "ATIVA" else None,
        canal_venda     = random.choice(["APP","WEB","PARCEIRO","TELEVENDAS","PDV"]),
        cupom           = random.choice(["", "PROMO10", "DESCONTO20", "FIDELIDADE", "PARCEIRO15"]),
        flag_trial      = status == "TRIAL",
        motivo_cancel   = ("" if status != "CANCELADA" else
                           random.choice(["PRECO","CONCORRENCIA","NAO_USA","INSATISFACAO","FINANCEIRO","PORTABILIDADE"])),
    ))

assin_df = spark.createDataFrame(assin_rows)

if DROP_IF_EXISTS:
    spark.sql(f"DROP TABLE IF EXISTS `{DATABASE}`.`assinaturas`")

(assin_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("status")
    .saveAsTable(f"`{DATABASE}`.`assinaturas`"))

print(f"  assinaturas: {assin_df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. uso_produto (Delta Managed)

# COMMAND ----------

num_uso = NUM_PF * 3
print(f"Gerando {num_uso:,} registros de uso...")

from datetime import date
today = date.today()

uso_rows = []
for i in range(num_uso):
    dt_uso = fake.date_between(start_date="-12m", end_date="today")
    uso_rows.append(Row(
        id_uso          = i + 1,
        id_cliente      = random.randint(1, NUM_PF),
        id_produto      = random.choice(produto_ids_ativos),
        tipo_evento     = random.choice(["LOGIN","CONSULTA","DOWNLOAD","COMPARTILHAMENTO","ALERTA","CONTESTACAO"]),
        dispositivo     = random.choice(["ANDROID","IOS","WEB_DESKTOP","WEB_MOBILE","API"]),
        duracao_seg     = random.randint(1, 3600),
        dt_evento       = dt_uso,
        ano_mes         = int(dt_uso.strftime("%Y%m")),
        sucesso         = random.random() < 0.95,
        codigo_erro     = ("" if random.random() < 0.95 else
                           random.choice(["ERR_001","ERR_002","TIMEOUT","UNAUTHORIZED"])),
    ))

uso_df = spark.createDataFrame(uso_rows)

if DROP_IF_EXISTS:
    spark.sql(f"DROP TABLE IF EXISTS `{DATABASE}`.`uso_produto`")

(uso_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ano_mes")
    .saveAsTable(f"`{DATABASE}`.`uso_produto`"))

print(f"  uso_produto: {uso_df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. precos_historico (EXTERNAL CSV — formato legado)

# COMMAND ----------

print("Gerando histórico de preços (EXTERNAL CSV)...")

preco_rows = []
for prod in PRODUTOS:
    preco_base = prod.preco_mensal
    for m in range(48):  # 4 anos de histórico
        dt_ref = date.today() - relativedelta(months=m)
        variacao = random.uniform(-0.05, 0.10)
        preco_hist = round(max(0, preco_base * (1 + variacao)), 2)
        preco_rows.append(Row(
            id_produto      = prod.id_produto,
            codigo_produto  = prod.codigo,
            ano_mes         = int(dt_ref.strftime("%Y%m")),
            preco_mensal    = preco_hist,
            preco_anual     = round(preco_hist * 10, 2),  # 2 meses grátis no anual
            moeda           = "BRL",
            motivo_alteracao= random.choice(["REAJUSTE_ANUAL","PROMOCAO","CORRECAO","LANCAMENTO",""]),
        ))

precos_df = spark.createDataFrame(preco_rows)
ext_precos_path = f"{EXTERNAL_PATH}/precos_historico"

(precos_df.write
    .format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .mode("overwrite")
    .save(ext_precos_path))

if DROP_IF_EXISTS:
    spark.sql(f"DROP TABLE IF EXISTS `{DATABASE}`.`precos_historico`")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{DATABASE}`.`precos_historico`
    USING CSV
    OPTIONS (
        header = 'true',
        delimiter = ';',
        inferSchema = 'true'
    )
    LOCATION '{ext_precos_path}'
""")

print(f"  precos_historico: {precos_df.count():,} registros (EXTERNAL CSV)")

# COMMAND ----------

print("=" * 50)
print(f"DATABASE: {DATABASE}")
print("=" * 50)
spark.sql(f"SHOW TABLES IN `{DATABASE}`").show()

import json
dbutils.notebook.exit(json.dumps({
    "database":          DATABASE,
    "catalogo_produtos": len(PRODUTOS),
    "assinaturas":       num_assinaturas,
    "uso_produto":       num_uso,
    "precos_historico":  len(preco_rows),
}))
