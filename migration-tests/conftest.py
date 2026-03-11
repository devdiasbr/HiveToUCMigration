"""
conftest.py — Fixtures globais do pytest para testes de migração Hive → UC.

Configura uma SparkSession local com Delta Lake e um "Hive Metastore" em memória
para que os testes rodem sem depender de um cluster Databricks real.
"""

import pytest
import tempfile
import os
import shutil
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


# ─────────────────────────────────────────────────────────────────────────────
# SparkSession compartilhada para toda a sessão de testes
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """SparkSession local com Delta Lake e Hive Metastore em memória."""
    warehouse = str(tmp_path_factory.mktemp("warehouse"))
    derby_home = str(tmp_path_factory.mktemp("derby"))

    os.environ["DERBY_HOME"] = derby_home

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("migration-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("javax.jdo.option.ConnectionURL",
                f"jdbc:derby:memory:metastore_db;create=true")
        # Desativa UI para testes
        .config("spark.ui.enabled", "false")
    )

    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")

    yield spark_session
    spark_session.stop()


# ─────────────────────────────────────────────────────────────────────────────
# Banco de dados de teste
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def hive_test_db(spark):
    """Cria o database de teste no Hive e retorna seu nome."""
    db = "test_hive_db"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
    yield db
    spark.sql(f"DROP DATABASE IF EXISTS `{db}` CASCADE")


@pytest.fixture(scope="session")
def audit_db(spark):
    """Database de auditoria onde os scripts salvam inventário e logs."""
    db = "migration_audit"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
    yield db
    spark.sql(f"DROP DATABASE IF EXISTS `{db}` CASCADE")


# ─────────────────────────────────────────────────────────────────────────────
# Dados de referência reutilizáveis
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def sample_clientes_df(spark):
    """DataFrame de clientes PF para testes."""
    from pyspark.sql import Row
    rows = [
        Row(id_cliente=1, cpf="123.456.789-00", nome="Ana Silva",   score_serasa=750, faixa_score="A", status="ATIVO",   uf="SP", renda_mensal=5000.0),
        Row(id_cliente=2, cpf="987.654.321-00", nome="Bruno Costa", score_serasa=400, faixa_score="C", status="ATIVO",   uf="RJ", renda_mensal=2500.0),
        Row(id_cliente=3, cpf="111.222.333-44", nome="Carla Melo",  score_serasa=150, faixa_score="E", status="INATIVO", uf="MG", renda_mensal=1500.0),
        Row(id_cliente=4, cpf="555.666.777-88", nome="Diego Reis",  score_serasa=900, faixa_score="A", status="ATIVO",   uf="SP", renda_mensal=12000.0),
        Row(id_cliente=5, cpf="999.888.777-66", nome="Eva Lima",    score_serasa=600, faixa_score="B", status="ATIVO",   uf="PR", renda_mensal=4000.0),
    ]
    return spark.createDataFrame(rows)


@pytest.fixture(scope="session")
def sample_schema_mapping(spark):
    """DataFrame de mapeamento Hive DB → UC Schema."""
    from pyspark.sql import Row
    rows = [
        Row(hive_database="test_hive_db",    uc_catalog="test_catalog", uc_schema="test_hive_db",    name_changed=False, status="CREATED"),
        Row(hive_database="serasa_clientes",  uc_catalog="test_catalog", uc_schema="serasa_clientes", name_changed=False, status="CREATED"),
    ]
    return spark.createDataFrame(rows)
