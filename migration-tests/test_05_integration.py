"""
test_05_integration.py — Testes de integração do pipeline completo.

Simula localmente o fluxo end-to-end:
  1. Cria tabelas no "Hive" (banco local de origem)
  2. Executa a lógica de migração (CTAS / DEEP CLONE simulado)
  3. Valida schema e contagem no "UC" (banco local de destino)
  4. Simula rollback e verifica integridade

Estes testes não dependem de um cluster Databricks real.
"""

import pytest
import json
from pyspark.sql import Row
from pyspark.sql import functions as F


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures de integração
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def integration_src_db(spark):
    db = "integration_hive_src"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
    yield db
    spark.sql(f"DROP DATABASE IF EXISTS `{db}` CASCADE")


@pytest.fixture(scope="module")
def integration_tgt_db(spark):
    db = "integration_uc_tgt"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
    yield db
    spark.sql(f"DROP DATABASE IF EXISTS `{db}` CASCADE")


@pytest.fixture(scope="module")
def populated_hive(spark, integration_src_db):
    """Popula o banco Hive de origem com tabelas de tipos variados."""
    db = integration_src_db

    # 1. Delta Managed
    spark.createDataFrame([
        Row(id=i, nome=f"cliente_{i}", score=i * 10, uf=("SP" if i % 2 == 0 else "RJ"))
        for i in range(1, 201)
    ]).write.format("delta").mode("overwrite") \
      .partitionBy("uf").saveAsTable(f"`{db}`.`clientes`")

    # 2. Delta Managed — tabela pequena de referência
    spark.createDataFrame([
        Row(id=1, codigo="A", descricao="Excelente"),
        Row(id=2, codigo="B", descricao="Bom"),
        Row(id=3, codigo="C", descricao="Regular"),
    ]).write.format("delta").mode("overwrite").saveAsTable(f"`{db}`.`faixas_score`")

    # 3. View
    spark.sql(f"DROP VIEW IF EXISTS `{db}`.`vw_clientes_sp`")
    spark.sql(f"""
        CREATE VIEW `{db}`.`vw_clientes_sp` AS
        SELECT c.id, c.nome, c.score, f.descricao AS faixa
        FROM `{db}`.`clientes` c
        JOIN `{db}`.`faixas_score` f ON f.codigo = (
            CASE
                WHEN c.score >= 800 THEN 'A'
                WHEN c.score >= 500 THEN 'B'
                ELSE 'C'
            END
        )
        WHERE c.uf = 'SP'
    """)

    yield db


# ─────────────────────────────────────────────────────────────────────────────
# Testes de integração
# ─────────────────────────────────────────────────────────────────────────────

class TestEndToEndMigration:

    def test_source_tables_created(self, spark, populated_hive):
        tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN `{populated_hive}`").collect()]
        assert "clientes"    in tables
        assert "faixas_score" in tables
        assert "vw_clientes_sp" in tables

    def test_migrate_delta_managed_table(self, spark, populated_hive, integration_tgt_db):
        """Migra tabela Delta via CTAS e valida contagem e schema."""
        src = f"`{populated_hive}`.`clientes`"
        tgt = f"`{integration_tgt_db}`.`clientes`"

        spark.sql(f"DROP TABLE IF EXISTS {tgt}")
        spark.sql(f"CREATE TABLE {tgt} USING DELTA AS SELECT * FROM {src}")

        src_count = spark.table(src).count()
        tgt_count = spark.table(tgt).count()

        assert src_count == tgt_count == 200

    def test_migrate_reference_table(self, spark, populated_hive, integration_tgt_db):
        src = f"`{populated_hive}`.`faixas_score`"
        tgt = f"`{integration_tgt_db}`.`faixas_score`"

        spark.sql(f"DROP TABLE IF EXISTS {tgt}")
        spark.sql(f"CREATE TABLE {tgt} USING DELTA AS SELECT * FROM {src}")

        assert spark.table(tgt).count() == 3

    def test_migrate_view_with_adapted_ddl(self, spark, populated_hive, integration_tgt_db):
        """Recria a view no banco de destino com os namespaces atualizados."""
        spark.sql(f"DROP VIEW IF EXISTS `{integration_tgt_db}`.`vw_clientes_sp`")
        spark.sql(f"""
            CREATE VIEW `{integration_tgt_db}`.`vw_clientes_sp` AS
            SELECT c.id, c.nome, c.score, f.descricao AS faixa
            FROM `{integration_tgt_db}`.`clientes` c
            JOIN `{integration_tgt_db}`.`faixas_score` f ON f.codigo = (
                CASE
                    WHEN c.score >= 800 THEN 'A'
                    WHEN c.score >= 500 THEN 'B'
                    ELSE 'C'
                END
            )
            WHERE c.uf = 'SP'
        """)

        count = spark.sql(f"SELECT COUNT(*) as cnt FROM `{integration_tgt_db}`.`vw_clientes_sp`").collect()[0]["cnt"]
        assert count > 0

    def test_schema_matches_after_migration(self, spark, populated_hive, integration_tgt_db):
        """Verifica que o schema da tabela migrada bate com a original."""
        src_schema = {f.name: str(f.dataType) for f in spark.table(f"`{populated_hive}`.`clientes`").schema.fields}
        tgt_schema = {f.name: str(f.dataType) for f in spark.table(f"`{integration_tgt_db}`.`clientes`").schema.fields}
        assert src_schema == tgt_schema

    def test_checksum_matches_after_migration(self, spark, populated_hive, integration_tgt_db):
        """Verifica integridade dos dados numéricos após migração."""
        src_sum = spark.table(f"`{populated_hive}`.`clientes`").agg(F.sum("score")).collect()[0][0]
        tgt_sum = spark.table(f"`{integration_tgt_db}`.`clientes`").agg(F.sum("score")).collect()[0][0]
        assert src_sum == tgt_sum

    def test_partitioning_preserved(self, spark, populated_hive, integration_tgt_db):
        """Verifica que os dados por partição batem."""
        src_sp = spark.table(f"`{populated_hive}`.`clientes`").filter(F.col("uf") == "SP").count()
        tgt_sp = spark.table(f"`{integration_tgt_db}`.`clientes`").filter(F.col("uf") == "SP").count()
        assert src_sp == tgt_sp

    def test_rollback_preserves_source(self, spark, populated_hive, integration_tgt_db):
        """Após dropar a tabela destino (rollback), a origem continua acessível."""
        spark.sql(f"DROP TABLE IF EXISTS `{integration_tgt_db}`.`clientes_rollback_test`")
        spark.sql(f"""
            CREATE TABLE `{integration_tgt_db}`.`clientes_rollback_test`
            USING DELTA AS SELECT * FROM `{populated_hive}`.`clientes`
        """)

        # Executa rollback
        spark.sql(f"DROP TABLE IF EXISTS `{integration_tgt_db}`.`clientes_rollback_test`")

        # Origem deve permanecer intacta
        count = spark.table(f"`{populated_hive}`.`clientes`").count()
        assert count == 200


# ─────────────────────────────────────────────────────────────────────────────
# Teste de pipeline com log de auditoria
# ─────────────────────────────────────────────────────────────────────────────

class TestAuditLog:

    def test_migration_log_written_correctly(self, spark, audit_db, populated_hive, integration_tgt_db):
        """Verifica que o log de migração é escrito e consultável."""
        log_rows = []
        tables_to_migrate = ["clientes", "faixas_score"]

        for tbl in tables_to_migrate:
            src = f"`{populated_hive}`.`{tbl}`"
            tgt = f"`{integration_tgt_db}`.`{tbl}_audit_test`"

            spark.sql(f"DROP TABLE IF EXISTS {tgt}")
            spark.sql(f"CREATE TABLE {tgt} USING DELTA AS SELECT * FROM {src}")

            src_count = spark.table(src).count()
            tgt_count = spark.table(tgt).count()

            log_rows.append(Row(
                hive_table   = f"{populated_hive}.{tbl}",
                uc_table     = f"{integration_tgt_db}.{tbl}_audit_test",
                status       = "SUCCESS" if src_count == tgt_count else "COUNT_MISMATCH",
                rows_migrated = tgt_count,
            ))

        log_df = spark.createDataFrame(log_rows)
        log_df.write.format("delta").mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(f"`{audit_db}`.`integration_test_log`")

        loaded = spark.table(f"`{audit_db}`.`integration_test_log`")
        success = loaded.filter(F.col("status") == "SUCCESS").count()
        assert success == len(tables_to_migrate)
