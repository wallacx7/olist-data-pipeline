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

    Ao final, carimba a tabela com uma coluna `_loaded_at` (ver
    `_stamp_loaded_at`), usada pelo `dbt source freshness`.

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

    _stamp_loaded_at(client, project_id, dataset_id, table_name)


def _stamp_loaded_at(
    client: "bigquery.Client",
    project_id: str,
    dataset_id: str,
    table_name: str,
) -> None:
    """
    Marca a linha inteira com o timestamp desta carga em `_loaded_at`.

    O dataset Olist é estático e só tem timestamps de negócio (ex:
    order_purchase_timestamp) — nenhum deles diz "quando essa tabela raw foi
    carregada", que é o que o `dbt source freshness` (models/staging/sources.yml)
    precisa pra funcionar. Em vez de inferir isso de metadata do BigQuery,
    carimbamos explicitamente logo após o load: mais simples e não depende de
    comportamento interno do BigQuery que pode mudar.

    WRITE_TRUNCATE substitui a tabela inteira a cada execução, então um UPDATE
    sem WHERE é seguro aqui (não corre risco de "esquecer" linhas de execuções
    antigas misturadas com novas).

    Best-effort: isso é um DML (UPDATE), que o tier gratuito do BigQuery (sem
    billing habilitado no projeto) rejeita com "DML queries are not allowed
    in the free tier". Carregar o dado é o objetivo real desta função — a
    task de ingestão não deve falhar (e travar o pipeline inteiro) só porque
    o freshness-tracking, um extra, não pôde ser aplicado nesse projeto.
    """
    fq_table = f"`{project_id}.{dataset_id}.{table_name}`"
    try:
        client.query(
            f"""
            ALTER TABLE {fq_table} ADD COLUMN IF NOT EXISTS _loaded_at TIMESTAMP;
            UPDATE {fq_table} SET _loaded_at = CURRENT_TIMESTAMP() WHERE TRUE;
            """
        ).result()
    except Exception:
        logger.warning(
            "Não foi possível carimbar _loaded_at em %s.%s.%s (dbt source "
            "freshness não vai funcionar pra essa tabela) — provável causa: "
            "billing desabilitado no projeto GCP, que bloqueia DML no tier "
            "gratuito. A carga em si foi concluída normalmente.",
            project_id, dataset_id, table_name,
            exc_info=True,
        )