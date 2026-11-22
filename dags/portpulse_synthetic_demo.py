"""Manually run the isolated synthetic ETA demo pipeline."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from scripts.synthetic_airflow_tasks import (
    validate_run_config as validate_run_config,
    input_path_for_run as input_path_for_run,
    generate_synthetic_events,
    ingest_synthetic_events,
    build_synthetic_eta_analytics,
)

with DAG(
    dag_id="portpulse_synthetic_demo",
    description="Generate, ingest, and model isolated synthetic ETA events",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "owner": "portpulse",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["portpulse", "synthetic", "dbt", "demo"],
) as dag:
    generate_task = PythonOperator(
        task_id="generate_synthetic_eta",
        python_callable=generate_synthetic_events,
    )
    ingest_task = PythonOperator(
        task_id="ingest_synthetic_eta",
        python_callable=ingest_synthetic_events,
    )
    build_task = PythonOperator(
        task_id="build_synthetic_eta_analytics",
        python_callable=build_synthetic_eta_analytics,
    )
    generate_task >> ingest_task >> build_task
