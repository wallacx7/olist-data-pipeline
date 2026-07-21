# Olist Data Pipeline — Airflow + dbt + BigQuery

Pipeline de dados end-to-end usando o dataset público [Brazilian E-Commerce (Olist)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce),
orquestrado com **Apache Airflow** (local, via Docker) e transformado com **dbt** em um
**Data Warehouse dimensional (Star Schema)** no **BigQuery**.

> Status: 🚧 em desenvolvimento — Fase 1 (setup do ambiente)

## Arquitetura

```
CSVs Olist (raw)
    -> Airflow (sensor + carga)
        -> BigQuery [raw layer]
            -> dbt (staging -> intermediate -> marts)
                -> Star Schema (fato + dimensões, com snapshot SCD2)
```

## Stack

- **Orquestração:** Apache Airflow 2.9 (LocalExecutor, Docker Compose)
- **Transformação:** dbt-bigquery
- **Data Warehouse:** Google BigQuery (GCP)
- **Linguagem:** Python 3.11

## Estrutura do repositório

```
olist-data-pipeline/
├── dags/              # DAGs do Airflow
├── dbt/               # Projeto dbt (staging, intermediate, marts, snapshots)
├── data/raw/           # CSVs do dataset Olist (não versionados)
├── scripts/           # Scripts auxiliares (ex: download do dataset)
├── keys/              # Service account do GCP (não versionado)
├── docker-compose.yml
└── .env.example
```

## Pré-requisitos

- Docker e Docker Compose instalados
- Conta GCP com um projeto criado e faturamento habilitado (BigQuery tem free tier generoso)
- Uma Service Account do GCP com permissão de `BigQuery Data Editor` + `BigQuery Job User`,
  com a chave JSON baixada

## Setup local

1. Clone o repositório e entre na pasta:
   ```bash
   git clone <seu-repo>
   cd olist-data-pipeline
   ```

2. Copie o arquivo de ambiente e preencha com seus dados:
   ```bash
   cp .env.example .env
   ```

3. Coloque a chave da Service Account do GCP em `keys/gcp-key.json`
   (esse caminho já está mapeado no `docker-compose.yml` e ignorado pelo Git).

4. Baixe o dataset Olist do Kaggle e coloque os CSVs em `data/raw/`
   (instruções detalhadas em `scripts/README.md`, a ser criado na Fase 2).

5. Suba o Airflow:
   ```bash
   docker compose up airflow-init
   docker compose up -d
   ```

6. Acesse a UI em [http://localhost:8080](http://localhost:8080)
   (usuário/senha definidos no `.env`).

## Roadmap do projeto

- [x] Fase 1 — Setup do ambiente (Docker Compose + Airflow rodando)
- [x] Fase 2 — Ingestão simples: CSV → BigQuery (raw layer)
- [ ] Fase 3 — dbt: staging models
- [ ] Fase 4 — dbt: marts (Star Schema — fato + dimensões)
- [ ] Fase 5 — Airflow: sensor, retries/SLA, TaskGroups
- [ ] Fase 6 — dbt snapshot (SCD Tipo 2) no status do pedido
- [ ] Fase 7 — Testes customizados dbt + `dbt docs`
- [ ] Fase 8 — Documentação final e diagrama do Star Schema

## Modelagem (planejada)

**Fato:** `fct_pedidos` (grão: item do pedido)

**Dimensões:**
- `dim_clientes`
- `dim_produtos`
- `dim_vendedores`
- `dim_data`
- `dim_geolocalizacao`
- `dim_status_pedido` (snapshot SCD Tipo 2)

---

*Projeto pessoal para estudo de Engenharia de Dados — Airflow, dbt e modelagem dimensional em GCP/BigQuery.*
