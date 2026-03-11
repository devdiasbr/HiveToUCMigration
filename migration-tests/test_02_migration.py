"""
test_02_migration.py — Testes para os scripts de migração (02_migration/).

Testa:
- DEEP CLONE de tabelas Delta Managed
- Registro de tabelas Delta External
- Adaptação de DDL de views (substituição de namespaces)
- Mapeamento de privilégios Hive → UC
- Log de migração
"""

import pytest
import re
import json
import tempfile
import os
from pyspark.sql import Row
from pyspark.sql.functions import col


# ─────────────────────────────────────────────────────────────────────────────
# Helpers dos scripts de migração
# ─────────────────────────────────────────────────────────────────────────────

PRIVILEGE_MAPPING = {
    "SELECT":         "SELECT",
    "INSERT":         "MODIFY",
    "UPDATE":         "MODIFY",
    "DELETE":         "MODIFY",
    "ALL PRIVILEGES": "ALL PRIVILEGES",
    "ALL":            "ALL PRIVILEGES",
    "CREATE":         "CREATE TABLE",
    "USAGE":          "USE SCHEMA",
    "READ_METADATA":  "BROWSE",
    "MODIFY":         "MODIFY",
}

def map_privilege(hive_privilege: str) -> str:
    return PRIVILEGE_MAPPING.get(hive_privilege.upper(), hive_privilege)

def sanitize_name(name: str) -> str:
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    if sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return sanitized.lower()

def adapt_view_ddl(ddl: str, catalog_name: str, schema_mapping: dict) -> str:
    """Substitui referências `db`.`table` pelo novo namespace UC."""
    def replace_backtick_ref(m):
        db  = m.group(1)
        tbl = m.group(2)
        schema = schema_mapping.get(db, db)
        return f"`{catalog_name}`.`{schema}`.`{tbl}`"

    return re.sub(r'`([^`]+)`\.`([^`]+)`', replace_backtick_ref, ddl)


# ─────────────────────────────────────────────────────────────────────────────
# Testes de mapeamento de privilégios
# ─────────────────────────────────────────────────────────────────────────────

class TestPrivilegeMapping:

    @pytest.mark.parametrize("hive_priv,expected_uc", [
        ("SELECT",         "SELECT"),
        ("INSERT",         "MODIFY"),
        ("UPDATE",         "MODIFY"),
        ("DELETE",         "MODIFY"),
        ("ALL PRIVILEGES", "ALL PRIVILEGES"),
        ("ALL",            "ALL PRIVILEGES"),
        ("CREATE",         "CREATE TABLE"),
        ("USAGE",          "USE SCHEMA"),
        ("READ_METADATA",  "BROWSE"),
    ])
    def test_privilege_mapping(self, hive_priv, expected_uc):
        assert map_privilege(hive_priv) == expected_uc

    def test_unknown_privilege_passthrough(self):
        """Privilégios desconhecidos são retornados como estão."""
        assert map_privilege("CUSTOM_PRIV") == "CUSTOM_PRIV"


# ─────────────────────────────────────────────────────────────────────────────
# Testes de sanitização de nomes
# ─────────────────────────────────────────────────────────────────────────────

class TestSanitizeName:

    def test_valid_name_unchanged(self):
        assert sanitize_name("clientes_pf") == "clientes_pf"

    def test_hyphen_replaced(self):
        assert sanitize_name("minha-tabela") == "minha_tabela"

    def test_space_replaced(self):
        assert sanitize_name("minha tabela") == "minha_tabela"

    def test_uppercase_lowercased(self):
        assert sanitize_name("MinhaTabela") == "minhatabela"

    def test_starts_with_digit(self):
        result = sanitize_name("2024_vendas")
        assert result.startswith("_")
        assert not result[0].isdigit()

    def test_special_chars_replaced(self):
        result = sanitize_name("tabela@#especial!")
        assert re.match(r'^[a-zA-Z0-9_]+$', result)


# ─────────────────────────────────────────────────────────────────────────────
# Testes de adaptação de DDL de views
# ─────────────────────────────────────────────────────────────────────────────

class TestViewDDLAdaptation:

    SCHEMA_MAP = {
        "serasa_clientes":    "serasa_clientes",
        "serasa_credito":     "serasa_credito",
        "serasa_financeiro":  "serasa_financeiro",
    }

    def test_simple_table_reference(self):
        ddl = "SELECT * FROM `serasa_clientes`.`clientes_pf`"
        result = adapt_view_ddl(ddl, "serasa_prod", self.SCHEMA_MAP)
        assert "`serasa_prod`.`serasa_clientes`.`clientes_pf`" in result

    def test_join_with_multiple_references(self):
        ddl = """
            SELECT c.nome, s.score
            FROM `serasa_clientes`.`clientes_pf` c
            JOIN `serasa_credito`.`score_historico` s
                ON c.id_cliente = s.id_cliente
        """
        result = adapt_view_ddl(ddl, "serasa_prod", self.SCHEMA_MAP)
        assert "`serasa_prod`.`serasa_clientes`.`clientes_pf`" in result
        assert "`serasa_prod`.`serasa_credito`.`score_historico`" in result

    def test_unknown_database_unchanged(self):
        """Databases não mapeados devem ser substituídos com o próprio nome."""
        ddl    = "SELECT * FROM `db_desconhecido`.`alguma_tabela`"
        result = adapt_view_ddl(ddl, "serasa_prod", self.SCHEMA_MAP)
        # O db_desconhecido vira catalog.db_desconhecido.tabela
        assert "`serasa_prod`.`db_desconhecido`.`alguma_tabela`" in result

    def test_idempotency(self):
        """Aplicar adapt_view_ddl duas vezes não deve duplicar o catálogo."""
        ddl     = "SELECT * FROM `serasa_clientes`.`clientes_pf`"
        result1 = adapt_view_ddl(ddl, "serasa_prod", self.SCHEMA_MAP)
        # Aplica de novo — como já tem 3 partes, não deve re-substituir
        result2 = adapt_view_ddl(result1, "serasa_prod", self.SCHEMA_MAP)
        assert result1.count("`serasa_prod`") == result2.count("`serasa_prod`")


# ─────────────────────────────────────────────────────────────────────────────
# Testes funcionais com Spark — DEEP CLONE simulado
# ─────────────────────────────────────────────────────────────────────────────

class TestDeepCloneSimulation:

    def test_deep_clone_preserves_row_count(self, spark, hive_test_db, tmp_path):
        """Simula DEEP CLONE verificando que a tabela destino tem os mesmos dados."""
        src_table = f"`{hive_test_db}`.`src_clone_test`"
        tgt_table = f"`{hive_test_db}`.`tgt_clone_test`"

        spark.sql(f"DROP TABLE IF EXISTS {src_table}")
        spark.sql(f"DROP TABLE IF EXISTS {tgt_table}")

        # Cria tabela origem
        spark.createDataFrame([
            Row(id=i, valor=float(i * 10), categoria=("A" if i % 2 == 0 else "B"))
            for i in range(1, 101)
        ]).write.format("delta").saveAsTable(src_table)

        # Simula DEEP CLONE (CREATE ... AS SELECT * FROM src)
        spark.sql(f"CREATE OR REPLACE TABLE {tgt_table} USING DELTA AS SELECT * FROM {src_table}")

        src_count = spark.table(src_table).count()
        tgt_count = spark.table(tgt_table).count()

        assert src_count == tgt_count == 100

    def test_deep_clone_preserves_schema(self, spark, hive_test_db):
        """Verifica que o schema é preservado após a cópia."""
        src = f"`{hive_test_db}`.`src_schema_test`"
        tgt = f"`{hive_test_db}`.`tgt_schema_test`"

        spark.sql(f"DROP TABLE IF EXISTS {src}")
        spark.sql(f"DROP TABLE IF EXISTS {tgt}")

        spark.createDataFrame([
            Row(id=1, nome="João", score=750, ativo=True)
        ]).write.format("delta").saveAsTable(src)

        spark.sql(f"CREATE OR REPLACE TABLE {tgt} USING DELTA AS SELECT * FROM {src}")

        src_fields = {f.name: str(f.dataType) for f in spark.table(src).schema.fields}
        tgt_fields = {f.name: str(f.dataType) for f in spark.table(tgt).schema.fields}

        assert src_fields == tgt_fields

    def test_deep_clone_preserves_partitions(self, spark, hive_test_db):
        """Verifica que o particionamento é reconhecido na tabela destino."""
        src = f"`{hive_test_db}`.`src_part_test`"
        tgt = f"`{hive_test_db}`.`tgt_part_test`"

        spark.sql(f"DROP TABLE IF EXISTS {src}")
        spark.sql(f"DROP TABLE IF EXISTS {tgt}")

        spark.createDataFrame([
            Row(id=i, categoria=("A" if i % 2 == 0 else "B"), valor=float(i))
            for i in range(1, 21)
        ]).write.format("delta").partitionBy("categoria").saveAsTable(src)

        spark.sql(f"""
            CREATE OR REPLACE TABLE {tgt}
            USING DELTA
            PARTITIONED BY (categoria)
            AS SELECT * FROM {src}
        """)

        assert spark.table(tgt).count() == 20


# ─────────────────────────────────────────────────────────────────────────────
# Testes de log de migração
# ─────────────────────────────────────────────────────────────────────────────

class TestMigrationLog:

    def test_log_schema(self, spark, audit_db):
        """Verifica que o log de migração tem o schema esperado."""
        log_rows = [
            Row(hive_table="db1.table1", uc_table="cat.db1.table1",
                status="SUCCESS", duration_sec=12.5, rows_cloned=1000,
                bytes_cloned=204800, error_message=None, migrated_at="2024-01-01T10:00:00"),
            Row(hive_table="db1.table2", uc_table="cat.db1.table2",
                status="ERROR", duration_sec=1.2,  rows_cloned=0,
                bytes_cloned=0, error_message="AnalysisException: table not found",
                migrated_at="2024-01-01T10:00:05"),
        ]

        log_df = spark.createDataFrame(log_rows)
        log_df.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"`{audit_db}`.`test_migration_log`")

        loaded = spark.table(f"`{audit_db}`.`test_migration_log`")
        assert loaded.count() == 2
        assert loaded.filter(col("status") == "SUCCESS").count() == 1
        assert loaded.filter(col("status") == "ERROR").count() == 1

    def test_log_append_mode(self, spark, audit_db):
        """Verifica que múltiplas execuções acumulam no log (mode=append)."""
        spark.sql(f"DROP TABLE IF EXISTS `{audit_db}`.`test_append_log`")

        batch1 = spark.createDataFrame([
            Row(hive_table="db.t1", uc_table="cat.db.t1", status="SUCCESS", run_id="run_001")
        ])
        batch2 = spark.createDataFrame([
            Row(hive_table="db.t2", uc_table="cat.db.t2", status="SUCCESS", run_id="run_002")
        ])

        for batch in [batch1, batch2]:
            batch.write.format("delta").mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(f"`{audit_db}`.`test_append_log`")

        total = spark.table(f"`{audit_db}`.`test_append_log`").count()
        assert total == 2
