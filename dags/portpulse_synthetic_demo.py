"""Manually run the isolated synthetic ETA demo pipeline."""

from __future__ import annotations

import hashlib
import logging
import subprocess
import sys
import tempfile
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, get_current_context


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOGGER = logging.getLogger(__name__)


def validate_run_config(conf: object) -> tuple[int, int]:
    if not isinstance(conf, dict):
        raise ValueError("DAG run configuration must be a JSON object")

    port_calls = conf.get("port_calls", 1000)
    if isinstance(port_calls, bool) or not isinstance(port_calls, int):
        raise ValueError("port_calls must be an integer from 1 to 1000")
    if not 1 <= port_calls <= 1000:
        raise ValueError("port_calls must be an integer from 1 to 1000")

    seed = conf.get("seed", 42)
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValueError("seed must be an integer")
    return port_calls, seed


def input_path_for_run(run_id: str) -> Path:
    run_key = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
    return Path(tempfile.gettempdir()) / "portpulse_synthetic_demo" / f"{run_key}.jsonl"


def generate_synthetic_events() -> None:
    context = get_current_context()
    run = context["dag_run"]
    port_calls, seed = validate_run_config(run.conf or {})
    input_path = input_path_for_run(run.run_id)
    LOGGER.info(
        "Generating %s synthetic port calls with seed %s into %s",
        port_calls,
        seed,
        input_path,
    )
    subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.generate_synthetic_eta",
            "--port-calls",
            str(port_calls),
            "--seed",
            str(seed),
            "--output",
            str(input_path),
        ],
        cwd=PROJECT_ROOT,
        check=True,
    )


def ingest_synthetic_events() -> None:
    run = get_current_context()["dag_run"]
    input_path = input_path_for_run(run.run_id)
    LOGGER.info("Validating and ingesting synthetic ETA events from %s", input_path)
    subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.ingest_synthetic_eta",
            "--input",
            str(input_path),
        ],
        cwd=PROJECT_ROOT,
        check=True,
    )


def build_synthetic_eta_analytics() -> None:
    LOGGER.info("Building dbt models and tests for synthetic ETA analytics")
    subprocess.run(
        ["dbt", "build"],
        cwd=PROJECT_ROOT / "dbt",
        check=True,
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
