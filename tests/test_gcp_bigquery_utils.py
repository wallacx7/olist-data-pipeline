"""
Testes unitários de scripts/gcp_bigquery_utils.py.

O BigQuery Client é sempre mockado — estes testes não fazem nenhuma chamada de
rede nem precisam de credenciais GCP; validam apenas o comportamento do nosso
código (o que é passado pro client, e como erros/avisos são tratados).
"""
from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest
from google.cloud import bigquery

from gcp_bigquery_utils import load_csv_to_bigquery_raw


def test_raises_if_csv_does_not_exist(tmp_path):
    missing_csv = tmp_path / "does_not_exist.csv"

    with pytest.raises(FileNotFoundError, match="não encontrado"):
        load_csv_to_bigquery_raw(
            csv_path=str(missing_csv),
            table_name="orders",
            project_id="fake-project",
            dataset_id="olist_raw",
        )


@patch("gcp_bigquery_utils.bigquery.Client")
def test_happy_path_configures_load_job_correctly(mock_client_cls, tmp_path):
    csv_path = tmp_path / "olist_orders_dataset.csv"
    csv_path.write_text("order_id,customer_id\n1,10\n", encoding="utf-8")

    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    mock_client.get_table.return_value = MagicMock(num_rows=1)

    mock_load_job = MagicMock()
    mock_load_job.errors = None
    mock_client.load_table_from_file.return_value = mock_load_job

    load_csv_to_bigquery_raw(
        csv_path=str(csv_path),
        table_name="orders",
        project_id="fake-project",
        dataset_id="olist_raw",
    )

    # Client instanciado com o projeto certo.
    mock_client_cls.assert_called_once_with(project="fake-project")

    # Dataset criado (idempotente) antes da carga.
    mock_client.create_dataset.assert_called_once()
    dataset_ref_arg = mock_client.create_dataset.call_args.args[0]
    assert dataset_ref_arg.project == "fake-project"
    assert dataset_ref_arg.dataset_id == "olist_raw"
    assert mock_client.create_dataset.call_args.kwargs["exists_ok"] is True

    # job_config reflete as tolerâncias documentadas (raw layer, ELT).
    mock_client.load_table_from_file.assert_called_once()
    _, kwargs = mock_client.load_table_from_file.call_args
    job_config = kwargs["job_config"]
    assert job_config.source_format == bigquery.SourceFormat.CSV
    assert job_config.autodetect is True
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert job_config.allow_quoted_newlines is True
    assert job_config.max_bad_records == 200

    # Job aguardado e tabela final consultada para log de confirmação.
    mock_load_job.result.assert_called_once()
    mock_client.get_table.assert_called_once()

    # Coluna _loaded_at carimbada após a carga (usada pelo dbt source freshness).
    mock_client.query.assert_called_once()
    stamp_sql = mock_client.query.call_args.args[0]
    assert "fake-project.olist_raw.orders" in stamp_sql
    assert "_loaded_at" in stamp_sql
    mock_client.query.return_value.result.assert_called_once()


@patch("gcp_bigquery_utils.bigquery.Client")
def test_logs_warning_when_load_job_has_errors(mock_client_cls, tmp_path, caplog):
    csv_path = tmp_path / "olist_order_reviews_dataset.csv"
    csv_path.write_text("review_id,comment\n1,\"texto com aspas soltas\n", encoding="utf-8")

    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    mock_client.get_table.return_value = MagicMock(num_rows=99)

    mock_load_job = MagicMock()
    mock_load_job.errors = [{"reason": "invalid"}] * 3
    mock_client.load_table_from_file.return_value = mock_load_job

    with caplog.at_level(logging.WARNING):
        load_csv_to_bigquery_raw(
            csv_path=str(csv_path),
            table_name="order_reviews",
            project_id="fake-project",
            dataset_id="olist_raw",
        )

    assert any("malformada" in record.message for record in caplog.records)
