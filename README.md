# HiveToUCMigration

Projeto de migração do **Hive Metastore** para o **Unity Catalog (UC)** no Databricks.

Inclui notebooks de migração, gerador de dados sintéticos para ambiente de testes e suíte de testes automatizados.

---

## Estrutura do Projeto

```
HiveToUCMigration/
│
├── hive-to-uc-migration/      # Pipeline de migração (notebooks Databricks)
├── hive-data-generator/       # Gerador de dados Faker para popular o Hive
└── migration-tests/           # Suíte de testes pytest (roda sem cluster)
```

---

## Módulos

### 1. `hive-to-uc-migration/` — Pipeline de Migração

Notebooks Databricks que cobrem todas as fases da migração, com suporte a **dry_run** e **log de auditoria em Delta**.

```
hive-to-uc-migration/
│
├── 00_ORCHESTRATOR.py                        # Entry point — executa todas as fases
│
├── 00_assessment/
│   ├── 01_hive_inventory.py                  # Cataloga databases, tabelas e views
│   └── 02_compatibility_check.py             # Detecta incompatibilidades e gera plano
│
├── 01_setup/
│   ├── 01_create_catalog_schema.py           # Cria catálogo e schemas no UC
│   └── 02_external_locations.py              # Configura storage credentials e external locations
│
├── 02_migration/
│   ├── 01_migrate_managed_tables.py          # DEEP CLONE de tabelas Delta Managed
│   ├── 02_migrate_external_tables.py         # Registra Delta External + CTAS para não-Delta
│   ├── 03_migrate_views.py                   # Adapta DDL e recria views no UC
│   └── 04_migrate_permissions.py             # Migra GRANTS e ownership
│
├── 03_validation/
│   ├── 01_validate_schema.py                 # Compara schema coluna a coluna
│   ├── 02_validate_data.py                   # Valida contagem e checksum numérico
│   └── 03_validate_permissions.py            # Audita grants no UC
│
└── 04_rollback/
    └── 01_rollback_plan.py                   # Remove objetos UC e restaura acesso ao Hive
```

#### Fluxo de migração

```
Assessment → Setup → Migração → Validação
                                    ↓ (se falhar)
                                 Rollback
```

#### Estratégias por tipo de tabela

| Tipo | Estratégia |
|---|---|
| Delta Managed | `DEEP CLONE` para UC |
| Delta External | Registro via `CREATE TABLE ... LOCATION` |
| Parquet / ORC / CSV | `CTAS` com conversão para Delta |
| Views | Extração de DDL + substituição de namespace |

#### Como usar

1. Execute `00_assessment/01_hive_inventory.py` para gerar o inventário
2. Revise o relatório de compatibilidade em `00_assessment/02_compatibility_check.py`
3. Configure os parâmetros no `00_ORCHESTRATOR.py`
4. Execute com `dry_run=true` primeiro, revise os logs
5. Execute com `dry_run=false` para migração real

> **Todos os notebooks têm `dry_run=true` como padrão — as tabelas originais no Hive nunca são deletadas.**

---

### 2. `hive-data-generator/` — Gerador de Dados

Popula o Hive Metastore com dados sintéticos realistas em **português do Brasil** usando a biblioteca [Faker](https://faker.readthedocs.io/).

```
hive-data-generator/
├── 00_ORCHESTRATOR_GENERATOR.py              # Executa todos os geradores em sequência
├── 01_install_dependencies.py               # %pip install faker
├── 02_create_hive_databases.py              # Cria os 5 databases de domínio
├── 03_generate_clientes.py                  # serasa_clientes
├── 04_generate_credito.py                   # serasa_credito
├── 05_generate_financeiro_inadimplencia.py  # serasa_financeiro + serasa_inadimplencia
└── 06_generate_produtos.py                  # serasa_produtos
```

#### Databases e tabelas geradas

| Database | Tabelas | Formato | Volume |
|---|---|---|---|
| `serasa_clientes` | clientes_pf, clientes_pj, enderecos | Delta (particionado) | ~12k linhas |
| `serasa_clientes` | contatos | External **Parquet** | ~25k linhas |
| `serasa_credito` | score_historico, consultas_credito, limites_credito | Delta (particionado por ano_mes) | ~290k linhas |
| `serasa_financeiro` | transacoes | Delta (particionado por ano_mes) | ~100k linhas |
| `serasa_financeiro` | contratos | External **ORC** | ~15k linhas |
| `serasa_inadimplencia` | ocorrencias, protestos | Delta (particionado) | ~10k linhas |
| `serasa_produtos` | catalogo_produtos, assinaturas, uso_produto | Delta | ~46k linhas |
| `serasa_produtos` | precos_historico | External **CSV** | ~720 linhas |

Os formatos variados (Delta, Parquet, ORC, CSV — Managed e External) cobrem todos os cenários de migração.

#### Como usar

```python
# No cluster Databricks, execute o orquestrador:
# Notebook: hive-data-generator/00_ORCHESTRATOR_GENERATOR.py
# Parâmetros:
#   num_clientes_pf = 10000
#   external_path   = dbfs:/tmp/serasa_ext
#   drop_if_exists  = false
```

---

### 3. `migration-tests/` — Testes Automatizados

Suíte **pytest** com `SparkSession` local — roda sem cluster Databricks.

```
migration-tests/
├── conftest.py               # SparkSession local + Delta Lake + fixtures
├── pytest.ini                # Configuração com cobertura de código
├── test_01_assessment.py     # Validação de nomes, palavras reservadas, complexidade
├── test_02_migration.py      # DEEP CLONE, adaptação de DDL, mapeamento de grants
├── test_03_validation.py     # Contagem com tolerância, checksum, comparação de schema
├── test_04_rollback.py       # Geração de DROP/REVOKE e garantias de dry_run
└── test_05_integration.py    # Pipeline end-to-end completo
```

#### Como executar

```bash
cd migration-tests
pip install -r requirements.txt
pytest
```

Relatório de cobertura gerado em `migration-tests/coverage_report/index.html`.

---

## Pré-requisitos

| Componente | Versão mínima |
|---|---|
| Databricks Runtime | 13.x LTS ou superior |
| Unity Catalog Metastore | configurado no workspace |
| Python (local) | 3.9+ |
| PySpark (local) | 3.4+ |
| delta-spark (local) | 2.4+ |

## Configuração do Workspace Databricks

O workspace está configurado via **Databricks Asset Bundle** (`databricks.yml`):

```yaml
bundle:
  name: Serasa
targets:
  dev:
    workspace:
      host: https://dbc-17dfffb4-0124.cloud.databricks.com
```

---

## Convenção de nomenclatura

```
Hive Metastore          Unity Catalog
─────────────────       ──────────────────────────────
<database>.<tabela>  →  <catalog>.<schema>.<tabela>
serasa_clientes.pf   →  serasa_prod.serasa_clientes.pf
```

---

## Segurança

- Nenhuma tabela Hive é deletada durante a migração
- Todo notebook suporta `dry_run=true` (padrão) para simulação sem efeitos colaterais
- O rollback exige confirmação dupla (`dry_run=false` **e** `confirm_rollback=true`)
- Credenciais nunca são hardcoded — use Databricks Secrets (`secret()`)
