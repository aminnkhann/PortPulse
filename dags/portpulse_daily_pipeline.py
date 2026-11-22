"""Daily Portnet ingestion and dbt analytics pipeline."""

from __future__ import annotations

import logging
import subprocess
import sys
from datetime import date, timedelta, timezone
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, get_current_context


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOGGER = logging.getLogger(__name__)


def _run_date(context: dict) -> str:
    run_conf = context["dag_run"].conf or {}
    configured_date = run_conf.get("run_date")
    if configured_date is not None:
        if not isinstance(configured_date, str):
            raise ValueError("run_date must use YYYY-MM-DD format")
        try:
            parsed_date = date.fromisoformat(configured_date)
        except ValueError as error:
            raise ValueError("run_date must use YYYY-MM-DD format") from error
        if parsed_date.isoformat() != configured_date:
            raise ValueError("run_date must use YYYY-MM-DD format")
        return parsed_date.isoformat()

    run_time = context.get("logical_date")
    if run_time is None:
        run_time = context["dag_run"].run_after
    if run_time is None or run_time.utcoffset() is None:
        raise ValueError(
            "Airflow run must have a timezone-aware logical date or run_after"
        )
    return run_time.astimezone(timezone.utc).date().isoformat()


def ingest_portnet() -> None:
    run_date = _run_date(get_current_context())
    LOGGER.info("Starting Portnet V1 ingestion for UTC date %s", run_date)
    subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.ingest_portnet",
            "--date",
            run_date,
        ],
        cwd=PROJECT_ROOT,
        check=True,
    )


def build_dbt_analytics() -> None:
    LOGGER.info("Building PortPulse dbt models and running dbt tests")
    subprocess.run(
        ["dbt", "build"],
        cwd=PROJECT_ROOT / "dbt",
        check=True,
    )


with DAG(
    dag_id="portpulse_daily_pipeline",
    description="Ingest Portnet V1 and build PortPulse dbt analytics",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "owner": "portpulse",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["portpulse", "portnet", "dbt"],
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_portnet",
        python_callable=ingest_portnet,
    )
    build_task = PythonOperator(
        task_id="build_dbt_analytics",
        python_callable=build_dbt_analytics,
    )
    ingest_task >> build_task
