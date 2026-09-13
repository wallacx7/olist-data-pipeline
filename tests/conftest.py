"""
Coloca scripts/ e dags/ no sys.path para os testes importarem os módulos do
projeto (gcp_bigquery_utils, os DAGs) do mesmo jeito que o Airflow faz em
runtime — sem precisar instalar o projeto como pacote.
"""
import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

for subdir in ("scripts", "dags"):
    path = os.path.join(ROOT_DIR, subdir)
    if path not in sys.path:
        sys.path.insert(0, path)
