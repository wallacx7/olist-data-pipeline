"""
DAG: olist_raw_ingestion

Fase 5 do projeto: orquestração amadurecida da ingestão dos CSVs do Olist.

Adições em relação à versão inicial (Fase 2):
- FileSensor: só inicia a carga depois de confirmar que os CSVs existem,
  simulando um cenário real de "esperar o arquivo chegar" (ex: de um SFTP,
  bucket, ou processo upstream) em vez de assumir que já está tudo lá.
- Retries + retry_delay: tolera falhas transitórias (rede, timeout do
  BigQuery) sem precisar de intervenção manual.
- TaskGroup: agrupa as 9 tasks de carga visualmente na UI, deixando o grafo
  mais legível conforme a DAG cresce.

Filosofia ELT mantida: nenhuma transformação de dado acontece aqui.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

from gcp_bigquery_utils import load_csv_to_bigquery_raw

DATA_DIR = "/opt/airflow/data/raw"
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET_RAW = os.environ.get("BIGQUERY_DATASET_RAW", "olist_raw")

# Mapeia: nome do arquivo CSV -> nome da tabela raw no BigQuery
TABLES = {
    "olist_orders_dataset.csv": "orders",
    "olist_customers_dataset.csv": "customers",
    "olist_order_items_dataset.csv": "order_items",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "product_category_translation",
}

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="olist_raw_ingestion",
    description="Carrega os CSVs do dataset Olist (raw) para o BigQuery",
    schedule=None,  # disparo manual por enquanto
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["olist", "raw", "ingestion"],
)
def olist_raw_ingestion():

    # Espera todos os CSVs esperados existirem em data/raw/ antes de seguir.
    # poke_interval curto porque, neste projeto, os arquivos já estão lá
    # (não há um processo upstream real depositando-os aos poucos) — mas a
    # estrutura simula o cenário onde haveria essa espera.
    wait_for_files = FileSensor(
        task_id="wait_for_csv_files",
        filepath=os.path.join(DATA_DIR, list(TABLES.keys())[0]),
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=60 * 5,
        mode="reschedule",  # libera o worker enquanto espera, em vez de travar um slot
    )

    with TaskGroup(group_id="load_raw_tables") as load_raw_tables:

        @task(task_id="load_csv_to_raw")
        def load_csv(csv_filename: str, table_name: str) -> None:
            csv_path = os.path.join(DATA_DIR, csv_filename)
            load_csv_to_bigquery_raw(
                csv_path=csv_path,
                table_name=table_name,
                project_id=PROJECT_ID,
                dataset_id=DATASET_RAW,
            )

        load_csv.expand_kwargs(
            [
                {"csv_filename": csv_filename, "table_name": table_name}
                for csv_filename, table_name in TABLES.items()
            ]
        )

    wait_for_files >> load_raw_tables


olist_raw_ingestion()