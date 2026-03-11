"""
test_03_validation.py — Testes para os scripts de validação (03_validation/).

Testa:
- Comparação de schemas (detecta colunas ausentes e tipos errados)
- Comparação de contagem de linhas com tolerância
- Comparação de checksums numéricos
- Detecção de diferenças em partições
"""

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


# ─────────────────────────────────────────────────────────────────────────────
# Helpers dos scripts de validação
# ─────────────────────────────────────────────────────────────────────────────

def compare_counts(hive_count: int, uc_count: int, tolerance_pct: float = 0.0) -> dict:
    diff     = abs(hive_count - uc_count)
    diff_pct = (diff / hive_count * 100) if hive_count > 0 else 0.0
    ok       = diff_pct <= tolerance_pct
    return {
        "hive_count":  hive_count,
        "uc_count":    uc_count,
        "diff":        diff,
        "diff_pct":    round(diff_pct, 6),
        "status":      "OK" if ok else "COUNT_MISMATCH",
    }

def compare_schemas(hive_schema: dict, uc_schema: dict) -> dict:
    hive_cols = set(hive_schema.keys())
    uc_cols   = set(uc_schema.keys())

    missing_in_uc = hive_cols - uc_cols
    extra_in_uc   = uc_cols - hive_cols

    type_mismatches = {
        c: f"hive={hive_schema[c]} / uc={uc_schema[c]}"
        for c in hive_cols & uc_cols
        if hive_schema[c].lower() != uc_schema[c].lower()
    }

    has_issues = bool(missing_in_uc or extra_in_uc or type_mismatches)
    return {
        "status":          "MISMATCH" if has_issues else "OK",
        "missing_in_uc":   missing_in_uc,
        "extra_in_uc":     extra_in_uc,
        "type_mismatches": type_mismatches,
    }

NUMERIC_TYPES = {"int", "integer", "long", "bigint", "double", "float", "decimal"}

def is_numeric_col(data_type: str) -> bool:
    return any(t in data_type.lower() for t in NUMERIC_TYPES)


# ─────────────────────────────────────────────────────────────────────────────
# Testes de comparação de contagens
# ─────────────────────────────────────────────────────────────────────────────

class TestCountComparison:

    def test_equal_counts_ok(self):
        r = compare_counts(1000, 1000)
        assert r["status"] == "OK"
        assert r["diff"] == 0

    def test_different_counts_mismatch(self):
        r = compare_counts(1000, 999)
        assert r["status"] == "COUNT_MISMATCH"
        assert r["diff"] == 1

    def test_tolerance_zero_exact_match_required(self):
        r = compare_counts(1000, 999, tolerance_pct=0.0)
        assert r["status"] == "COUNT_MISMATCH"

    def test_tolerance_1pct_accepts_small_diff(self):
        r = compare_counts(1000, 991, tolerance_pct=1.0)
        assert r["status"] == "OK"

    def test_tolerance_exceeded(self):
        r = compare_counts(1000, 980, tolerance_pct=1.0)
        assert r["status"] == "COUNT_MISMATCH"

    def test_zero_hive_count(self):
        r = compare_counts(0, 0)
        assert r["status"] == "OK"
        assert r["diff_pct"] == 0.0

    def test_diff_pct_calculated_correctly(self):
        r = compare_counts(200, 190)
        assert abs(r["diff_pct"] - 5.0) < 0.001


# ─────────────────────────────────────────────────────────────────────────────
# Testes de comparação de schemas
# ─────────────────────────────────────────────────────────────────────────────

class TestSchemaComparison:

    def test_identical_schemas_ok(self):
        schema = {"id": "int", "nome": "string", "valor": "double"}
        r = compare_schemas(schema, schema.copy())
        assert r["status"] == "OK"
        assert not r["missing_in_uc"]
        assert not r["extra_in_uc"]
        assert not r["type_mismatches"]

    def test_missing_column_in_uc(self):
        hive = {"id": "int", "nome": "string", "cpf": "string"}
        uc   = {"id": "int", "nome": "string"}
        r = compare_schemas(hive, uc)
        assert r["status"] == "MISMATCH"
        assert "cpf" in r["missing_in_uc"]

    def test_extra_column_in_uc(self):
        hive = {"id": "int", "nome": "string"}
        uc   = {"id": "int", "nome": "string", "coluna_nova": "string"}
        r = compare_schemas(hive, uc)
        assert r["status"] == "MISMATCH"
        assert "coluna_nova" in r["extra_in_uc"]

    def test_type_mismatch_detected(self):
        hive = {"id": "int", "valor": "double"}
        uc   = {"id": "int", "valor": "string"}
        r = compare_schemas(hive, uc)
        assert r["status"] == "MISMATCH"
        assert "valor" in r["type_mismatches"]

    def test_case_insensitive_type_comparison(self):
        hive = {"id": "INT"}
        uc   = {"id": "int"}
        r = compare_schemas(hive, uc)
        assert r["status"] == "OK"


# ─────────────────────────────────────────────────────────────────────────────
# Testes de detecção de colunas numéricas
# ─────────────────────────────────────────────────────────────────────────────

class TestNumericColumnDetection:

    @pytest.mark.parametrize("dtype,expected", [
        ("int",     True),
        ("integer", True),
        ("bigint",  True),
        ("double",  True),
        ("float",   True),
        ("decimal", True),
        ("string",  False),
        ("boolean", False),
        ("date",    False),
        ("timestamp", False),
    ])
    def test_is_numeric_col(self, dtype, expected):
        assert is_numeric_col(dtype) == expected


# ─────────────────────────────────────────────────────────────────────────────
# Testes funcionais com Spark — comparação real de tabelas
# ─────────────────────────────────────────────────────────────────────────────

class TestSparkValidation:

    def _create_table(self, spark, db, name, rows):
        spark.sql(f"DROP TABLE IF EXISTS `{db}`.`{name}`")
        spark.createDataFrame(rows).write \
            .format("delta").mode("overwrite") \
            .saveAsTable(f"`{db}`.`{name}`")

    def test_identical_tables_pass_count_check(self, spark, hive_test_db):
        rows = [Row(id=i, valor=float(i)) for i in range(1, 51)]
        self._create_table(spark, hive_test_db, "val_src", rows)
        self._create_table(spark, hive_test_db, "val_tgt", rows)

        src_count = spark.table(f"`{hive_test_db}`.`val_src`").count()
        tgt_count = spark.table(f"`{hive_test_db}`.`val_tgt`").count()
        r = compare_counts(src_count, tgt_count)

        assert r["status"] == "OK"

    def test_tables_with_different_counts_fail(self, spark, hive_test_db):
        src_rows = [Row(id=i, valor=float(i)) for i in range(1, 101)]
        tgt_rows = [Row(id=i, valor=float(i)) for i in range(1, 96)]

        self._create_table(spark, hive_test_db, "val_count_src", src_rows)
        self._create_table(spark, hive_test_db, "val_count_tgt", tgt_rows)

        src_count = spark.table(f"`{hive_test_db}`.`val_count_src`").count()
        tgt_count = spark.table(f"`{hive_test_db}`.`val_count_tgt`").count()
        r = compare_counts(src_count, tgt_count)

        assert r["status"] == "COUNT_MISMATCH"
        assert r["diff"] == 5

    def test_checksum_sum_matches(self, spark, hive_test_db):
        """Verifica que SUM de coluna numérica bate entre origem e destino."""
        rows = [Row(id=i, valor=float(i * 100)) for i in range(1, 11)]

        self._create_table(spark, hive_test_db, "chk_src", rows)
        self._create_table(spark, hive_test_db, "chk_tgt", rows)

        src_sum = spark.table(f"`{hive_test_db}`.`chk_src`").agg(F.sum("valor")).collect()[0][0]
        tgt_sum = spark.table(f"`{hive_test_db}`.`chk_tgt`").agg(F.sum("valor")).collect()[0][0]

        assert src_sum == tgt_sum

    def test_checksum_detects_modification(self, spark, hive_test_db):
        """Verifica que uma modificação nos dados é detectada pelo checksum."""
        src_rows = [Row(id=i, valor=float(i * 100)) for i in range(1, 11)]
        tgt_rows = [Row(id=i, valor=float(i * 100 + (1 if i == 5 else 0))) for i in range(1, 11)]

        self._create_table(spark, hive_test_db, "chk_mod_src", src_rows)
        self._create_table(spark, hive_test_db, "chk_mod_tgt", tgt_rows)

        src_sum = spark.table(f"`{hive_test_db}`.`chk_mod_src`").agg(F.sum("valor")).collect()[0][0]
        tgt_sum = spark.table(f"`{hive_test_db}`.`chk_mod_tgt`").agg(F.sum("valor")).collect()[0][0]

        assert src_sum != tgt_sum

    def test_schema_comparison_via_spark_describe(self, spark, hive_test_db):
        """Usa DESCRIBE TABLE para comparar schemas de duas tabelas."""
        spark.sql(f"DROP TABLE IF EXISTS `{hive_test_db}`.`schema_a`")
        spark.sql(f"DROP TABLE IF EXISTS `{hive_test_db}`.`schema_b`")

        spark.createDataFrame([Row(id=1, nome="x", valor=1.0)]).write \
            .format("delta").saveAsTable(f"`{hive_test_db}`.`schema_a`")
        spark.createDataFrame([Row(id=1, nome="x", valor=1.0)]).write \
            .format("delta").saveAsTable(f"`{hive_test_db}`.`schema_b`")

        def get_schema(db, tbl):
            return {
                r["col_name"]: r["data_type"]
                for r in spark.sql(f"DESCRIBE TABLE `{db}`.`{tbl}`").collect()
                if r["col_name"] and not r["col_name"].startswith("#")
            }

        schema_a = get_schema(hive_test_db, "schema_a")
        schema_b = get_schema(hive_test_db, "schema_b")

        r = compare_schemas(schema_a, schema_b)
        assert r["status"] == "OK"
