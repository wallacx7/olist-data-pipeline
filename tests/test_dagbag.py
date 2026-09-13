"""
Teste padrão de "DagBag import" — pega a classe de erro mais comum em DAGs
(erro de sintaxe, import quebrado, ciclo de dependência, DAG duplicada) antes
de chegar no Airflow de verdade. Roda no CI a cada push.

Requer apache-airflow instalado (ver requirements-dev.txt) — não é mockado
porque o próprio parsing das DAGs depende dos decorators/operators reais.
"""
import os

import pytest

airflow = pytest.importorskip("airflow", reason="apache-airflow não instalado")
from airflow.models import DagBag  # noqa: E402

DAGS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dags")

EXPECTED_DAG_IDS = {"olist_raw_ingestion", "olist_dbt_transformation"}


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=DAGS_DIR, include_examples=False)


def test_no_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}, (
        f"DAGs com erro de import: {dagbag.import_errors}"
    )


def test_expected_dags_are_present(dagbag: DagBag) -> None:
    assert EXPECTED_DAG_IDS.issubset(dagbag.dags.keys())


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_dag_has_no_cycles_and_has_tasks(dagbag: DagBag, dag_id: str) -> None:
    dag = dagbag.dags[dag_id]
    assert dag is not None
    assert len(dag.tasks) > 0
    # DagBag já roda o teste de ciclo no processo de parsing (test_cycle),
    # mas revalidamos explicitamente para deixar a intenção clara aqui.
    dag.test_cycle()
