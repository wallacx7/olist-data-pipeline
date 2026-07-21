"""
DAG: olist_raw_ingestion

Fase 2 do projeto: ingestão simples e crua dos CSVs do dataset Olist para o
BigQuery (camada raw). Sem sensor, retry customizado ou TaskGroup ainda —
isso é adicionado na Fase 5, junto com o restante do amadurecimento da
orquestração (o objetivo aqui é validar o caminho feliz primeiro).

Filosofia ELT: nenhuma transformação de dado acontece aqui. Cada task
apenas pega um CSV e carrega no BigQuery como está. Limpeza, tipagem e
regras de negócio ficam para o dbt (staging/intermediate/marts).
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.decorators import dag, task

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


@dag(
    dag_id="olist_raw_ingestion",
    description="Carrega os CSVs do dataset Olist (raw) para o BigQuery",
    schedule=None,  # disparo manual por enquanto; agendamento entra depois
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["olist", "raw", "ingestion"],
)
def olist_raw_ingestion():

    @task(task_id="load_csv_to_raw")
    def load_csv(csv_filename: str, table_name: str) -> None:
        csv_path = os.path.join(DATA_DIR, csv_filename)
        load_csv_to_bigquery_raw(
            csv_path=csv_path,
            table_name=table_name,
            project_id=PROJECT_ID,
            dataset_id=DATASET_RAW,
        )

    # Uma task mapeada dinamicamente por arquivo/tabela.
    # Todas rodam em paralelo (não há dependência entre elas nesta fase).
    load_csv.expand_kwargs(
        [
            {"csv_filename": csv_filename, "table_name": table_name}
            for csv_filename, table_name in TABLES.items()
        ]
    )


olist_raw_ingestion()
