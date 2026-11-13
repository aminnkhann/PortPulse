"""Bounded Airflow micro-batches for simulated PortPulse PostgreSQL CDC."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from datetime import timedelta
from pathlib import Path

import pendulum
import psycopg
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOGGER = logging.getLogger(__name__)


def build_streaming_dbt_models() -> None:
    executable = shutil.which("dbt")
    if executable is None:
        raise RuntimeError("dbt executable is not available on the Airflow PATH")
    subprocess.run(
        [
            executable,
            "build",
            "--select",
            "stg_stream_eta_changes",
            "mart_stream_latest_eta",
        ],
        cwd=PROJECT_ROOT / "dbt",
        env=os.environ.copy(),
        check=True,
    )


def verify_streaming_models() -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be set for streaming verification")
    with psycopg.connect(database_url) as connection:
        duplicate = connection.execute(
            """SELECT EXISTS (
                   SELECT 1 FROM analytics.mart_stream_latest_eta
                   GROUP BY scenario_id, port_call_id, port_area_code
                   HAVING count(*) > 1
               )"""
        ).fetchone()[0]
        deleted = connection.execute(
            """SELECT EXISTS (
                   SELECT 1 FROM analytics.mart_stream_latest_eta
                   WHERE cdc_operation = 'd'
               )"""
        ).fetchone()[0]
        if duplicate or deleted:
            raise RuntimeError(
                "Streaming mart verification failed: duplicate current rows or "
                "a deleted row is present"
            )
    LOGGER.info("Streaming mart verification passed")


with DAG(
    dag_id="portpulse_streaming_cdc",
    description="Consume bounded Debezium CDC batches and build simulated ETA marts",
    schedule="* * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "owner": "portpulse",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["portpulse", "kafka", "cdc", "demo"],
) as dag:
    consume = ConsumeFromTopicOperator(
        task_id="consume_cdc_batch",
        kafka_config_id="portpulse_kafka",
        topics=["portpulse_stream.stream_demo.port_eta_state"],
        apply_function_batch="scripts.streaming_cdc.process_kafka_batch",
        commit_cadence="end_of_operator",
        max_messages=100,
        max_batch_size=50,
        poll_timeout=5,
        do_xcom_push=False,
    )
    build = PythonOperator(
        task_id="build_streaming_dbt_models",
        python_callable=build_streaming_dbt_models,
    )
    verify = PythonOperator(
        task_id="verify_streaming_models",
        python_callable=verify_streaming_models,
    )
    consume >> build >> verify
