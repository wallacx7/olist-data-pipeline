"""
Utilitários de carga para o BigQuery.

Mantém a lógica de "como carregar um CSV cru no BigQuery" separada da DAG,
para que a DAG só orquestre (defina ordem/dependência) e não tenha lógica
de negócio ou implementação misturada nela.

Filosofia ELT: carregamos o CSV como está, sem limpeza/transformação aqui.
Renomear colunas, tratar nulos, ajustar tipos etc. é responsabilidade do dbt
(camada staging), não desta etapa de ingestão.
"""
import logging
import os

from google.cloud import bigquery

logger = logging.getLogger(__name__)


def load_csv_to_bigquery_raw(
    csv_path: str,
    table_name: str,
    project_id: str,
    dataset_id: str,
) -> None:
    """
    Carrega um único CSV para uma tabela no dataset "raw" do BigQuery.

    - autodetect=True: BigQuery infere o schema a partir do CSV (aceitável
      na camada raw; o schema é formalizado depois no dbt/staging).
    - write_disposition=WRITE_TRUNCATE: cada execução da DAG substitui os
      dados da tabela raw inteira (simples e idempotente para este estágio
      do projeto; pode evoluir para incremental mais adiante).
    - encoding="UTF-8": o dataset Olist já vem em UTF-8, mas deixamos
      explícito para evitar bugs silenciosos de acentuação (dataset é BR).
    - allow_quoted_newlines=True + max_bad_records: alguns CSVs do Olist
      (ex: order_reviews) têm campos de texto livre com aspas soltas dentro
      do próprio comentário, o que quebra o parser estrito de CSV do
      BigQuery. Como estamos na camada raw (ELT: carregamos como está,
      tratamos depois no dbt), toleramos um pequeno número de linhas
      malformadas em vez de falhar a carga inteira por causa de um punhado
      de linhas com aspas mal-fechadas.

    Levanta exceção se o arquivo não existir ou se o job do BigQuery falhar,
    para que a task do Airflow marque falha corretamente (e não passe batido).
    """
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(f"CSV não encontrado: {csv_path}")

    client = bigquery.Client(project=project_id)

    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    client.create_dataset(dataset_ref, exists_ok=True)

    table_ref = dataset_ref.table(table_name)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        encoding="UTF-8",
        allow_quoted_newlines=True,
        max_bad_records=200,
    )

    logger.info("Carregando %s -> %s.%s.%s", csv_path, project_id, dataset_id, table_name)

    with open(csv_path, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    load_job.result()  # bloqueia até o job terminar (ou falhar)

    destination_table = client.get_table(table_ref)
    logger.info(
        "Carga concluída: %s linhas em %s.%s.%s",
        destination_table.num_rows, project_id, dataset_id, table_name,
    )

    if load_job.errors:
        logger.warning(
            "%s linha(s) malformada(s) foram descartadas durante a carga de %s "
            "(tolerância configurada via max_bad_records). Detalhes: %s",
            len(load_job.errors), table_name, load_job.errors[:3],
        )