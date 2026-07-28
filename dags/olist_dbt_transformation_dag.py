"""
DAG: olist_dbt_transformation

Fase 5 (parte 2) do projeto: dispara o job de transformação do dbt Cloud
(staging -> marts, via `dbt build`) a partir do Airflow, usando a API do
dbt Cloud (DbtCloudRunJobOperator).

Isso demonstra um padrão comum em produção: o Airflow orquestra o *quando*
e a *ordem* das coisas rodarem, mas delega a *execução* da transformação
para uma plataforma gerenciada especializada (dbt Cloud), em vez de rodar
dbt localmente dentro do worker do Airflow.

Depende da task 'load_raw_tables' da DAG 'olist_raw_ingestion' ter sido
concluída antes de rodar (a orquestração entre as duas DAGs, via sensor
ou trigger, fica para uma iteração futura — por ora, disparo manual).
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

DBT_CLOUD_CONN_ID = "dbt_cloud_default"
DBT_CLOUD_JOB_ID = 70506183136068


@dag(
    dag_id="olist_dbt_transformation",
    description="Dispara o job dbt Cloud (staging + marts) via API",
    schedule=None,  # disparo manual por enquanto
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["olist", "dbt", "transformation"],
)
def olist_dbt_transformation():

    run_dbt_job = DbtCloudRunJobOperator(
        task_id="run_staging_and_marts",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=DBT_CLOUD_JOB_ID,
        check_interval=15,  # segundos entre verificações de status
        timeout=60 * 15,    # desiste após 15 minutos
        wait_for_termination=True,  # task só finaliza quando o job do dbt terminar
    )


olist_dbt_transformation()