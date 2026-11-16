"""Freshness and volume checks for the tables this repo produces (dctx check). A stale or suddenly small table fails the task,
so it alerts through the same Airflow alerting as any other failure. Adjust REPO and the schedule."""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO = "/opt/airflow/repo"  # a checkout of this repo on the workers, with .data-context/config.yml (dialect, query_command)

with DAG(
    dag_id="dctx_checks",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},  # a retry would only hide a stale table for another run
    tags=["monitoring"],
) as dag:
    BashOperator(
        task_id="freshness_and_volume",
        bash_command=f"cd {REPO} && uv run --no-config .data-context/dctx.py build >/dev/null && uv run --no-config .data-context/dctx.py check --all",
    )
