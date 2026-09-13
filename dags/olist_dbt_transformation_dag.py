"""
DAG: olist_dbt_transformation

Fase 5 (parte 2), revisada: antes disparava um job do dbt Cloud via API
(DbtCloudRunJobOperator). Migrado para dbt Core rodando dentro do próprio
worker do Airflow, orquestrado pelo astronomer-cosmos — cada model, teste
e snapshot do projeto dbt vira uma task individual no grafo do Airflow,
em vez de uma única caixa-preta "rodou o job".

Motivo da migração: a conta dbt Cloud voltou para o plano Developer
(gratuito) após o fim do trial, e esse plano não tem acesso às APIs do
dbt Cloud — Administrative e Discovery API são exclusivas dos planos
Starter, Enterprise e Enterprise+. O DbtCloudRunJobOperator passou a
falhar (401/403) ao tentar disparar o job. Como este é um projeto de
estudo (não produtivo), dbt Core local resolve sem depender de plano
pago — com o bônus de dar lineage por model dentro do próprio Airflow,
em vez de um retângulo único "trigger dbt Cloud".

Dispara automaticamente quando 'olist_raw_ingestion' concluir a carga
das tabelas raw, via agendamento data-aware (Dataset) — substitui o
disparo manual anterior.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior

# Mesmo Dataset (por URI) referenciado em 'olist_raw_ingestion_dag.py':
# o Airflow casa os dois lados pela URI, não é preciso importar entre DAGs.
OLIST_RAW_DATASET = Dataset("bigquery://olist_raw")

# Cosmos lê o projeto dbt e o profile já no *parse* da DAG (para montar o
# grafo de tasks), não só na execução — por isso esses caminhos precisam
# existir também fora do container (ex: no DagBag import test do CI).
# Default = caminhos reais dentro do container Airflow; sobrescrito via env
# var só em ambientes de teste (ver .github/workflows/ci.yml).
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt_profiles")

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_DIR,
    install_dbt_deps=True,  # `dbt deps` (dbt_utils) antes de rodar
)

profile_config = ProfileConfig(
    profile_name="olist_data_pipeline",
    target_name="default",
    profiles_yml_filepath=os.path.join(DBT_PROFILES_DIR, "profiles.yml"),
)

# ExecutionMode.LOCAL (default): usa o dbt-bigquery já instalado no worker do
# Airflow via _PIP_ADDITIONAL_REQUIREMENTS, sem precisar de venv/docker extra.
execution_config = ExecutionConfig()

# TestBehavior.AFTER_ALL em vez do default (AFTER_EACH): o teste
# `relationships` de fct_pagamentos.order_id -> stg_orders.order_id cruza
# staging -> marts. Com AFTER_EACH, o Cosmos prendeu esse teste ao grupo de
# testes do stg_orders (que roda logo após a staging), antes de fct_pagamentos
# sequer existir — "Not found: Table ...fct_pagamentos". Rodar todos os
# testes só depois de todos os models (AFTER_ALL) elimina essa classe de
# problema de ordenação para qualquer teste que cruze camadas.
render_config = RenderConfig(test_behavior=TestBehavior.AFTER_ALL)


@dag(
    dag_id="olist_dbt_transformation",
    description="Roda staging + marts + tests + snapshot via dbt Core (Cosmos)",
    schedule=[OLIST_RAW_DATASET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["olist", "dbt", "transformation"],
)
def olist_dbt_transformation():

    DbtTaskGroup(
        group_id="dbt_build",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
    )


olist_dbt_transformation()
