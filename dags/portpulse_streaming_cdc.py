"""Bounded Airflow micro-batches for simulated PortPulse PostgreSQL CDC."""

from __future__ import annotations

import logging
import subprocess
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOGGER = logging.getLogger(__name__)


def build_streaming_dbt_models() -> None:
    subprocess.run(
        [
            "dbt",
            "build",
            "--select",
            "stg_stream_eta_changes",
            "mart_stream_latest_eta",
        ],
        cwd=PROJECT_ROOT / "dbt",
        check=True,
    )


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
    consume >> build
