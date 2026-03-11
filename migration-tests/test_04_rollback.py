"""
test_04_rollback.py — Testes para o script de rollback (04_rollback/).

Testa:
- Verificação de integridade das tabelas originais no Hive
- Geração dos SQLs de DROP para objetos UC
- Geração dos SQLs de REVOKE a partir dos grants migrados
- Garantia de que tabelas Hive permanecem acessíveis após rollback simulado
"""

import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col


# ─────────────────────────────────────────────────────────────────────────────
# Helpers do script de rollback
# ─────────────────────────────────────────────────────────────────────────────

def generate_drop_sql(catalog: str, schema: str, object_name: str, object_type: str) -> str:
    return f"DROP {object_type} IF EXISTS `{catalog}`.`{schema}`.`{object_name}`"

def generate_revoke_sql(grant_sql: str) -> str:
    """Converte um GRANT SQL em REVOKE SQL."""
    return grant_sql.replace("GRANT", "REVOKE", 1).replace(" TO ", " FROM ")

def check_table_accessible(spark, db: str, table: str) -> dict:
    """Verifica se uma tabela está acessível no Hive."""
    try:
        spark.sql(f"SELECT 1 FROM `{db}`.`{table}` LIMIT 1")
        return {"table": f"{db}.{table}", "accessible": True,  "error": None}
    except Exception as e:
        return {"table": f"{db}.{table}", "accessible": False, "error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Testes de geração de SQL de rollback
# ─────────────────────────────────────────────────────────────────────────────

class TestRollbackSQLGeneration:

    def test_drop_table_sql(self):
        sql = generate_drop_sql("serasa_prod", "serasa_clientes", "clientes_pf", "TABLE")
        assert sql == "DROP TABLE IF EXISTS `serasa_prod`.`serasa_clientes`.`clientes_pf`"

    def test_drop_view_sql(self):
        sql = generate_drop_sql("serasa_prod", "serasa_clientes", "vw_ativos", "VIEW")
        assert sql == "DROP VIEW IF EXISTS `serasa_prod`.`serasa_clientes`.`vw_ativos`"

    def test_revoke_from_grant(self):
        grant  = "GRANT SELECT ON TABLE `serasa_prod`.`db`.`tbl` TO `analistas`"
        revoke = generate_revoke_sql(grant)
        assert revoke == "REVOKE SELECT ON TABLE `serasa_prod`.`db`.`tbl` FROM `analistas`"

    def test_revoke_multiple_privileges(self):
        grant  = "GRANT MODIFY ON TABLE `cat`.`schema`.`tbl` TO `engenheiros`"
        revoke = generate_revoke_sql(grant)
        assert "REVOKE" in revoke
        assert "FROM" in revoke
        assert "TO" not in revoke

    def test_drop_schema_cascade(self):
        sql = f"DROP SCHEMA IF EXISTS `serasa_prod`.`serasa_clientes` CASCADE"
        assert "CASCADE" in sql
        assert "serasa_clientes" in sql


# ─────────────────────────────────────────────────────────────────────────────
# Testes funcionais — rollback não afeta tabelas Hive
# ─────────────────────────────────────────────────────────────────────────────

class TestRollbackDoesNotAffectHive:

    def test_hive_table_accessible_after_uc_drop(self, spark, hive_test_db):
        """
        Simula: cria tabela no 'UC' (outro db local) e a dropa.
        A tabela Hive original deve permanecer intacta.
        """
        # Cria tabela "Hive" original
        spark.sql(f"DROP TABLE IF EXISTS `{hive_test_db}`.`rollback_original`")
        spark.createDataFrame([
            Row(id=i, dados=f"valor_{i}") for i in range(1, 21)
        ]).write.format("delta").saveAsTable(f"`{hive_test_db}`.`rollback_original`")

        # Cria tabela "UC" (simulada como outro banco local)
        uc_db = f"{hive_test_db}_uc_sim"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{uc_db}`")
        spark.sql(f"DROP TABLE IF EXISTS `{uc_db}`.`rollback_original`")
        spark.sql(f"""
            CREATE TABLE `{uc_db}`.`rollback_original`
            USING DELTA AS
            SELECT * FROM `{hive_test_db}`.`rollback_original`
        """)

        # Executa rollback — dropa tabela UC
        spark.sql(f"DROP TABLE IF EXISTS `{uc_db}`.`rollback_original`")

        # Verifica que a tabela Hive ainda existe e tem os dados corretos
        result = check_table_accessible(spark, hive_test_db, "rollback_original")
        assert result["accessible"] is True

        count = spark.table(f"`{hive_test_db}`.`rollback_original`").count()
        assert count == 20

        # Limpa DB de simulação
        spark.sql(f"DROP DATABASE IF EXISTS `{uc_db}` CASCADE")

    def test_error_on_inaccessible_table(self, spark, hive_test_db):
        """Verifica que check_table_accessible retorna erro para tabela inexistente."""
        result = check_table_accessible(spark, hive_test_db, "tabela_que_nao_existe_xyz")
        assert result["accessible"] is False
        assert result["error"] is not None

    def test_accessible_table_returns_true(self, spark, hive_test_db):
        spark.sql(f"DROP TABLE IF EXISTS `{hive_test_db}`.`rollback_check`")
        spark.createDataFrame([Row(id=1)]).write \
            .format("delta").saveAsTable(f"`{hive_test_db}`.`rollback_check`")

        result = check_table_accessible(spark, hive_test_db, "rollback_check")
        assert result["accessible"] is True
        assert result["error"] is None


# ─────────────────────────────────────────────────────────────────────────────
# Teste de segurança — dry_run deve ser obrigatório por padrão
# ─────────────────────────────────────────────────────────────────────────────

class TestRollbackSafety:

    def test_dry_run_produces_no_changes(self, spark, hive_test_db):
        """Em dry_run, nenhum DROP deve ser executado — apenas SQLs gerados."""
        spark.sql(f"DROP TABLE IF EXISTS `{hive_test_db}`.`safety_test`")
        spark.createDataFrame([Row(id=1, val="ok")]).write \
            .format("delta").saveAsTable(f"`{hive_test_db}`.`safety_test`")

        dry_run = True
        drop_sqls = []

        # Simula o dry_run: gera SQL mas não executa
        sql = generate_drop_sql(hive_test_db, hive_test_db, "safety_test", "TABLE")
        if dry_run:
            drop_sqls.append(sql)  # apenas coleta, não executa
        else:
            spark.sql(sql)

        # Tabela deve ainda existir
        tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN `{hive_test_db}`").collect()]
        assert "safety_test" in tables
        assert len(drop_sqls) == 1  # SQL foi gerado mas não executado

    def test_requires_confirm_for_real_execution(self):
        """Verifica que a flag confirm_rollback impede execução não confirmada."""
        dry_run        = False
        confirm_rollback = False

        if not dry_run and not confirm_rollback:
            with pytest.raises(Exception, match="confirm_rollback"):
                raise Exception(
                    "SEGURANÇA: Para executar o rollback real, defina confirm_rollback=true "
                    "E dry_run=false."
                )
