"""
test_01_assessment.py — Testes para os scripts de assessment (00_assessment/).

Testa:
- Lógica de inventário de databases e tabelas
- Regras de verificação de compatibilidade com UC
- Detecção de nomes inválidos
- Detecção de formatos não suportados
"""

import pytest
import re
from pyspark.sql import Row
from pyspark.sql.functions import col


# ─────────────────────────────────────────────────────────────────────────────
# Helpers copiados dos scripts de assessment para teste unitário
# ─────────────────────────────────────────────────────────────────────────────

UC_RESERVED_WORDS = {
    "catalog", "schema", "database", "table", "view", "column",
    "function", "volume", "storage", "credential", "location",
    "share", "recipient", "provider"
}

def check_name_validity(name: str) -> str:
    if not name:
        return "INVALID_EMPTY"
    if not re.match(r'^[a-zA-Z0-9_]+$', name):
        return f"INVALID_CHARS: {name}"
    if len(name) > 255:
        return "INVALID_TOO_LONG"
    return "OK"

def check_reserved_word(name: str) -> str:
    if name and name.lower() in UC_RESERVED_WORDS:
        return f"RESERVED_WORD: {name}"
    return "OK"

def get_migration_complexity(object_type: str, is_delta: bool, data_provider: str) -> str:
    if object_type == "VIEW":
        return "MEDIUM"
    if object_type in ("MANAGED", "EXTERNAL") and is_delta:
        return "LOW"
    if not is_delta:
        return "HIGH"
    return "MEDIUM"

def get_recommended_strategy(object_type: str, is_delta: bool) -> str:
    if object_type == "MANAGED" and is_delta:
        return "DEEP CLONE para UC"
    if object_type == "EXTERNAL" and is_delta:
        return "Sync External Location + CREATE TABLE como External no UC"
    if object_type == "VIEW":
        return "Recriar DDL da view no UC"
    return "CTAS com conversão para Delta no UC"


# ─────────────────────────────────────────────────────────────────────────────
# Testes de validação de nomes
# ─────────────────────────────────────────────────────────────────────────────

class TestNameValidation:

    def test_valid_name(self):
        assert check_name_validity("clientes_pf") == "OK"

    def test_valid_name_with_numbers(self):
        assert check_name_validity("tabela_2024") == "OK"

    def test_empty_name(self):
        assert check_name_validity("") == "INVALID_EMPTY"

    def test_name_with_hyphen(self):
        result = check_name_validity("minha-tabela")
        assert result.startswith("INVALID_CHARS")

    def test_name_with_space(self):
        result = check_name_validity("minha tabela")
        assert result.startswith("INVALID_CHARS")

    def test_name_with_dot(self):
        result = check_name_validity("db.table")
        assert result.startswith("INVALID_CHARS")

    def test_name_too_long(self):
        long_name = "a" * 256
        assert check_name_validity(long_name) == "INVALID_TOO_LONG"

    def test_name_max_length_ok(self):
        name = "a" * 255
        assert check_name_validity(name) == "OK"

    @pytest.mark.parametrize("word", ["catalog", "schema", "TABLE", "View", "COLUMN"])
    def test_reserved_words(self, word):
        result = check_reserved_word(word)
        assert result.startswith("RESERVED_WORD")

    def test_non_reserved_word(self):
        assert check_reserved_word("clientes") == "OK"


# ─────────────────────────────────────────────────────────────────────────────
# Testes de classificação de complexidade de migração
# ─────────────────────────────────────────────────────────────────────────────

class TestMigrationComplexity:

    @pytest.mark.parametrize("obj_type,is_delta,expected", [
        ("MANAGED",  True,  "LOW"),
        ("EXTERNAL", True,  "LOW"),
        ("VIEW",     False, "MEDIUM"),
        ("MANAGED",  False, "HIGH"),
        ("EXTERNAL", False, "HIGH"),
    ])
    def test_complexity_classification(self, obj_type, is_delta, expected):
        result = get_migration_complexity(obj_type, is_delta, "delta" if is_delta else "parquet")
        assert result == expected

    @pytest.mark.parametrize("obj_type,is_delta,expected_keyword", [
        ("MANAGED",  True,  "DEEP CLONE"),
        ("EXTERNAL", True,  "External Location"),
        ("VIEW",     False, "DDL"),
        ("MANAGED",  False, "CTAS"),
    ])
    def test_strategy_recommendation(self, obj_type, is_delta, expected_keyword):
        result = get_recommended_strategy(obj_type, is_delta)
        assert expected_keyword in result


# ─────────────────────────────────────────────────────────────────────────────
# Testes com Spark — inventário de tabelas
# ─────────────────────────────────────────────────────────────────────────────

class TestHiveInventory:

    def test_create_and_detect_managed_delta_table(self, spark, hive_test_db):
        """Cria uma tabela Delta Managed e verifica se seria detectada corretamente."""
        spark.sql(f"DROP TABLE IF EXISTS `{hive_test_db}`.`test_managed`")
        spark.createDataFrame([Row(id=1, nome="teste")]).write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(f"`{hive_test_db}`.`test_managed`")

        # Verifica que a tabela existe
        tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN `{hive_test_db}`").collect()]
        assert "test_managed" in tables

    def test_detect_table_type(self, spark, hive_test_db):
        """Verifica que DESCRIBE EXTENDED retorna o tipo correto."""
        spark.sql(f"DROP TABLE IF EXISTS `{hive_test_db}`.`test_type_check`")
        spark.createDataFrame([Row(id=1, val=100)]).write \
            .format("delta") \
            .saveAsTable(f"`{hive_test_db}`.`test_type_check`")

        detail = {
            r["col_name"]: r["data_type"]
            for r in spark.sql(f"DESCRIBE TABLE EXTENDED `{hive_test_db}`.`test_type_check`").collect()
        }
        assert detail.get("Type") == "MANAGED"
        assert "delta" in detail.get("Provider", "").lower()

    def test_inventory_collects_schema_info(self, spark, hive_test_db):
        """Verifica coleta de schema via DESCRIBE TABLE."""
        spark.sql(f"DROP TABLE IF EXISTS `{hive_test_db}`.`test_schema_info`")
        spark.createDataFrame([Row(id=1, nome="x", valor=9.99)]).write \
            .format("delta") \
            .saveAsTable(f"`{hive_test_db}`.`test_schema_info`")

        schema_rows = spark.sql(
            f"DESCRIBE TABLE `{hive_test_db}`.`test_schema_info`"
        ).collect()

        col_names = {r["col_name"] for r in schema_rows if not r["col_name"].startswith("#")}
        assert "id"    in col_names
        assert "nome"  in col_names
        assert "valor" in col_names

    def test_inventory_dataframe_schema(self, spark, sample_clientes_df):
        """Verifica que o DataFrame de clientes tem as colunas esperadas."""
        expected_cols = {"id_cliente", "cpf", "nome", "score_serasa", "faixa_score", "status"}
        actual_cols   = set(sample_clientes_df.columns)
        assert expected_cols.issubset(actual_cols)

    def test_inventory_row_count(self, spark, sample_clientes_df):
        assert sample_clientes_df.count() == 5
