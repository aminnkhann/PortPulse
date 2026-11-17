"""One manual Airflow run fetches real and synthetic ETA data and builds analytics."""

from __future__ import annotations

import logging
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, get_current_context

from dags.portpulse_synthetic_demo import (
    generate_synthetic_events,
    ingest_synthetic_events,
    build_synthetic_eta_analytics,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOGGER = logging.getLogger(__name__)


def portnet_dates(conf: dict, now: date) -> list[str]:
    """Recheck a limited window of source dates, oldest first."""
    if not isinstance(conf, dict):
        raise ValueError("DAG configuration must be a JSON object")
    selected = conf.get("run_date", now.isoformat())
    if not isinstance(selected, str):
        raise ValueError("run_date must use YYYY-MM-DD")
    try:
        parsed = date.fromisoformat(selected)
    except ValueError as error:
        raise ValueError("run_date must use YYYY-MM-DD") from error
    if parsed.isoformat() != selected:
        raise ValueError("run_date must use YYYY-MM-DD")

    count = conf.get("lookback_days", 7)
    if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= 30:
        raise ValueError("lookback_days must be an integer from 1 to 30")
    return [
        (parsed - timedelta(days=offset)).isoformat()
        for offset in range(count - 1, -1, -1)
    ]


def fetch_real_portnet() -> None:
    run = get_current_context()["dag_run"]
    dates = portnet_dates(run.conf or {}, datetime.now(timezone.utc).date())
    LOGGER.info("Fetching Portnet dates %s through %s", dates[0], dates[-1])
    for i, day in enumerate(dates):
        if i:
            time.sleep(1)
        subprocess.run(
            [sys.executable, "-m", "scripts.ingest_portnet", "--date", day],
            cwd=PROJECT_ROOT,
            check=True,
        )


with DAG(
    dag_id="portpulse_full_pipeline",
    description="One run: real Portnet + synthetic ETA, then dbt analytics",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={"owner": "portpulse", "retries": 1},
    tags=["portpulse", "real", "synthetic", "dbt"],
) as dag:
    real_task = PythonOperator(
        task_id="fetch_real_portnet",
        python_callable=fetch_real_portnet,
    )
    generate_task = PythonOperator(
        task_id="generate_synthetic_eta",
        python_callable=generate_synthetic_events,
    )
    ingest_task = PythonOperator(
        task_id="ingest_synthetic_eta",
        python_callable=ingest_synthetic_events,
    )
    dbt_task = PythonOperator(
        task_id="build_analytics",
        python_callable=build_synthetic_eta_analytics,
    )

    generate_task >> ingest_task
    [real_task, ingest_task] >> dbt_task
