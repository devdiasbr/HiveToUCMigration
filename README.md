# HiveToUCMigration

> Pipeline de migração do **Hive Metastore** para o **Unity Catalog (UC)** no Databricks — com geração de dados sintéticos e suíte de testes automatizados.

![Databricks Runtime](https://img.shields.io/badge/Databricks_Runtime-13.x_LTS+-orange)
![Python](https://img.shields.io/badge/Python-3.9+-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.4+-red)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-2.4+-blue)
![Pytest](https://img.shields.io/badge/Tests-pytest-green)
![dry_run](https://img.shields.io/badge/default-dry__run%3Dtrue-yellow)

---

## Visão Geral

Este projeto fornece um framework completo e seguro para migrar objetos do **Hive Metastore** (databases, tabelas managed, external e views) para o **Unity Catalog**, cobrindo:

- **Assessment** automatizado do ambiente Hive com detecção de incompatibilidades
- **Migração** de todos os tipos de tabela (Delta, Parquet, ORC, CSV — Managed e External)
- **Validação** de schema, contagem de linhas e checksum
- **Rollback** com dupla confirmação obrigatória
- **Audit log** completo em tabelas Delta durante todo o processo
- **Testes unitários e de integração** que rodam localmente sem cluster Databricks

---

## Estrutura do Projeto

```
HiveToUCMigration/
│
├── databricks.yml                 # Databricks Asset Bundle (dev/prod)
│
├── hive-to-uc-migration/          # Pipeline de migração (notebooks Databricks)
│   └── requirements.txt
│
├── hive-data-generator/           # Gerador de dados Faker para popular o Hive
│   └── requirements.txt
│
└── migration-tests/               # Suíte de testes pytest (roda sem cluster)
    ├── requirements.txt
    └── pytest.ini
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

#### Fluxo do Pipeline

```
┌─────────────────────────────────┐
│  FASE 0 — ASSESSMENT            │
│  Inventário + Compatibilidade   │
└──────────────┬──────────────────┘
               ↓
┌─────────────────────────────────┐
│  FASE 1 — SETUP                 │
│  Catálogo + External Locations  │
└──────────────┬──────────────────┘
               ↓
┌─────────────────────────────────┐
│  FASE 2 — MIGRAÇÃO              │
│  Tabelas + Views + Permissões   │
└──────────────┬──────────────────┘
               ↓
┌─────────────────────────────────┐
│  FASE 3 — VALIDAÇÃO             │
│  Schema + Dados + Grants        │
└──────────────┬──────────────────┘
               │ falhou?
               ↓
┌─────────────────────────────────┐
│  FASE 4 — ROLLBACK (opcional)   │
│  Remove objetos UC criados      │
└─────────────────────────────────┘
```

#### Estratégias por tipo de tabela

| Tipo | Estratégia |
|---|---|
| Delta Managed | `DEEP CLONE` para UC |
| Delta External | Registro via `CREATE TABLE ... LOCATION` |
| Parquet / ORC / CSV | `CTAS` com conversão para Delta |
| Views | Extração de DDL + substituição de namespace |

#### Convenção de nomenclatura

```
Hive Metastore          Unity Catalog
─────────────────       ──────────────────────────────
<database>.<tabela>  →  <catalog>.<schema>.<tabela>
serasa_clientes.pf   →  serasa_prod.serasa_clientes.pf
```

#### Como usar

1. Execute `00_assessment/01_hive_inventory.py` para gerar o inventário
2. Revise o relatório de compatibilidade em `00_assessment/02_compatibility_check.py`
3. Configure os parâmetros no `00_ORCHESTRATOR.py`
4. Execute com `dry_run=true` primeiro — revise os logs na tabela de auditoria Delta
5. Execute com `dry_run=false` para migração real

> **Todos os notebooks têm `dry_run=true` como padrão — as tabelas originais no Hive nunca são deletadas.**

---

### 2. `hive-data-generator/` — Gerador de Dados

Popula o Hive Metastore com dados sintéticos realistas em **português do Brasil** usando a biblioteca [Faker](https://faker.readthedocs.io/).

Os 5 databases simulam um ambiente de produção da Serasa com múltiplos formatos de tabela para cobrir todos os cenários de migração.

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

| Database | Tabelas | Formato | Volume aprox. |
|---|---|---|---|
| `serasa_clientes` | clientes_pf, clientes_pj, enderecos | Delta (particionado por `uf`) | ~12k linhas |
| `serasa_clientes` | contatos | External **Parquet** | ~25k linhas |
| `serasa_credito` | score_historico, consultas_credito, limites_credito | Delta (particionado por `ano_mes`) | ~290k linhas |
| `serasa_financeiro` | transacoes | Delta (particionado por `ano_mes`) | ~100k linhas |
| `serasa_financeiro` | contratos | External **ORC** | ~15k linhas |
| `serasa_inadimplencia` | ocorrencias, protestos | Delta (particionado por `uf`) | ~10k linhas |
| `serasa_produtos` | catalogo_produtos, assinaturas, uso_produto | Delta | ~46k linhas |
| `serasa_produtos` | precos_historico | External **CSV** | ~720 linhas |

> A variedade de formatos (Delta Managed, Delta External, Parquet, ORC, CSV) garante cobertura completa dos cenários de migração.

#### Como usar

```python
# No cluster Databricks, execute o orquestrador:
# Notebook: hive-data-generator/00_ORCHESTRATOR_GENERATOR.py
#
# Parâmetros disponíveis:
#   num_clientes_pf = 10000       # volume de clientes PF gerados
#   external_path   = dbfs:/tmp/serasa_ext   # path para tabelas external
#   drop_if_exists  = false       # recriar databases existentes
```

---

### 3. `migration-tests/` — Testes Automatizados

Suíte **pytest** com `SparkSession` local e metastore Derby em memória — **roda sem cluster Databricks**.

```
migration-tests/
├── conftest.py               # SparkSession local + Delta Lake + fixtures compartilhadas
├── pytest.ini                # Configuração de execução e cobertura
├── requirements.txt          # Dependências dos testes
├── test_01_assessment.py     # Validação de nomes, palavras reservadas, complexidade
├── test_02_migration.py      # DEEP CLONE, adaptação de DDL, mapeamento de grants
├── test_03_validation.py     # Contagem com tolerância, checksum, comparação de schema
├── test_04_rollback.py       # Geração de DROP/REVOKE e garantias de dry_run
└── test_05_integration.py    # Pipeline end-to-end completo
```

#### Marcadores disponíveis

| Marcador | Descrição |
|---|---|
| `@pytest.mark.unit` | Testes sem Spark, execução rápida |
| `@pytest.mark.spark` | Testes com SparkSession local |
| `@pytest.mark.integration` | Pipeline end-to-end |
| `@pytest.mark.slow` | Testes lentos (pode pular com `-m "not slow"`) |

#### Como executar

```bash
cd migration-tests
pip install -r requirements.txt

# Todos os testes
pytest

# Apenas testes rápidos (sem Spark)
pytest -m unit

# Apenas integração
pytest -m integration

# Pular testes lentos
pytest -m "not slow"

# Com relatório de cobertura
pytest --cov=. --cov-report=html:coverage_report
```

Relatório HTML de cobertura gerado em `migration-tests/coverage_report/index.html`.

---

## Quick Start

### Pré-requisitos

| Componente | Versão mínima |
|---|---|
| Databricks Runtime | 13.x LTS ou superior |
| Unity Catalog Metastore | configurado no workspace |
| Python (local) | 3.9+ |
| PySpark (local) | 3.4+ |
| delta-spark (local) | 2.4+ |
| databricks-sdk | 0.20.0+ |

### 1. Configurar workspace

```yaml
# databricks.yml
bundle:
  name: Serasa
targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-7405618747518215.15.azuredatabricks.net
```

### 2. Gerar dados de teste no Hive

```
Cluster Databricks → Importar notebooks de hive-data-generator/
→ Executar 00_ORCHESTRATOR_GENERATOR.py
```

### 3. Rodar migração (dry run)

```
Cluster Databricks → Importar notebooks de hive-to-uc-migration/
→ Configurar parâmetros em 00_ORCHESTRATOR.py
→ Executar com dry_run=true
→ Revisar logs na tabela Delta de auditoria
→ Executar com dry_run=false
```

### 4. Rodar testes localmente

```bash
cd migration-tests
pip install -r requirements.txt
pytest -v
```

---

## Segurança

- Nenhuma tabela Hive é deletada durante a migração
- Todo notebook suporta `dry_run=true` (padrão) para simulação sem efeitos colaterais
- O rollback exige confirmação dupla (`dry_run=false` **e** `confirm_rollback=true`)
- Credenciais nunca são hardcoded — use **Databricks Secrets** (`dbutils.secrets.get()`)
- Audit log completo de todas as operações persistido em tabelas Delta

---

## Qualidade de Código

```bash
# Formatação
black .

# Linting
flake8 .

# Ordenação de imports
isort .

# Pre-commit hooks (instalar uma vez)
pre-commit install
```

Dependências de dev listadas em `hive-to-uc-migration/requirements.txt`.
