"""Daily Portnet ingestion and dbt analytics pipeline."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import date, timedelta, timezone
from pathlib import Path

import pendulum
import psycopg
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context
from psycopg import sql


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_TABLES = (
    "portnet_eta_observation",
    "portnet_eta_rejected_event",
    "portnet_eta_quality_issue",
)
DBT_MODELS = (
    "stg_portnet_eta",
    "mart_portnet_latest_eta",
    "mart_pipeline_quality",
)
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

    logical_date = context.get("logical_date")
    if logical_date is None or logical_date.utcoffset() is None:
        raise ValueError("Airflow run must have a timezone-aware logical date")
    return logical_date.astimezone(timezone.utc).date().isoformat()


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
        env=os.environ.copy(),
        check=True,
    )


def build_dbt_analytics() -> None:
    LOGGER.info("Building PortPulse dbt models and running dbt tests")
    subprocess.run(
        [str(Path(sys.executable).with_name("dbt")), "build"],
        cwd=PROJECT_ROOT / "dbt",
        env=os.environ.copy(),
        check=True,
    )


def _relation_exists(
    cursor: psycopg.Cursor,
    schema_name: str,
    relation_name: str,
) -> bool:
    cursor.execute(
        """SELECT EXISTS (
               SELECT 1 FROM information_schema.tables
               WHERE table_schema = %s AND table_name = %s
           )""",
        (schema_name, relation_name),
    )
    return cursor.fetchone()[0]


def _count_relation(
    cursor: psycopg.Cursor,
    schema_name: str,
    relation_name: str,
) -> int:
    cursor.execute(
        sql.SQL("SELECT count(*) FROM {}.{}").format(
            sql.Identifier(schema_name),
            sql.Identifier(relation_name),
        )
    )
    return cursor.fetchone()[0]


def verify_pipeline(
    database_url: str | None = None,
    source_schema: str = "public",
    dbt_schema: str | None = None,
) -> dict[str, int]:
    database_url = database_url or os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("Set DATABASE_URL for pipeline verification")
    dbt_schema = dbt_schema or os.environ.get("DBT_SCHEMA", "analytics")

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            source_counts = {}
            for table_name in SOURCE_TABLES:
                if not _relation_exists(cursor, source_schema, table_name):
                    raise RuntimeError(
                        f"Required PostgreSQL source table is missing: "
                        f"{source_schema}.{table_name}"
                    )
                source_counts[table_name] = _count_relation(
                    cursor, source_schema, table_name
                )

            for model_name in DBT_MODELS:
                if not _relation_exists(cursor, dbt_schema, model_name):
                    raise RuntimeError(
                        f"Required dbt model is missing: {dbt_schema}.{model_name}"
                    )

            staging_rows = _count_relation(
                cursor, dbt_schema, "stg_portnet_eta"
            )
            latest_rows = _count_relation(
                cursor, dbt_schema, "mart_portnet_latest_eta"
            )

            cursor.execute(
                sql.SQL(
                    """SELECT count(*) FILTER (
                               WHERE issue_code = 'EXACT_DUPLICATE'
                           ),
                           count(*) FILTER (
                               WHERE issue_code = 'ID_COLLISION'
                           )
                       FROM {}.{}"""
                ).format(
                    sql.Identifier(source_schema),
                    sql.Identifier("portnet_eta_quality_issue"),
                )
            )
            duplicate_issues, collision_issues = cursor.fetchone()

            cursor.execute(
                sql.SQL(
                    """SELECT accepted_observations,
                              rejected_observations,
                              duplicate_issue_rows,
                              collision_issue_rows,
                              latest_eta_records
                       FROM {}.{}"""
                ).format(
                    sql.Identifier(dbt_schema),
                    sql.Identifier("mart_pipeline_quality"),
                )
            )
            quality_rows = cursor.fetchall()
            if len(quality_rows) != 1:
                raise RuntimeError(
                    "mart_pipeline_quality must contain exactly one metrics row"
                )

            actual_metrics = dict(
                zip(
                    (
                        "accepted_observations",
                        "rejected_observations",
                        "duplicate_issue_rows",
                        "collision_issue_rows",
                        "latest_eta_records",
                    ),
                    quality_rows[0],
                    strict=True,
                )
            )
            expected_metrics = {
                "accepted_observations": source_counts[
                    "portnet_eta_observation"
                ],
                "rejected_observations": source_counts[
                    "portnet_eta_rejected_event"
                ],
                "duplicate_issue_rows": duplicate_issues,
                "collision_issue_rows": collision_issues,
                "latest_eta_records": latest_rows,
            }
            if actual_metrics != expected_metrics:
                raise RuntimeError(
                    "Pipeline-quality metrics do not match source and mart "
                    f"counts: expected {expected_metrics}, got {actual_metrics}"
                )
            if staging_rows != source_counts["portnet_eta_observation"]:
                raise RuntimeError(
                    "stg_portnet_eta row count does not match accepted observations"
                )

            cursor.execute(
                sql.SQL(
                    """SELECT EXISTS (
                           SELECT 1
                           FROM {}.{}
                           GROUP BY port_call_id, port_area_code
                           HAVING count(*) > 1
                       )"""
                ).format(
                    sql.Identifier(dbt_schema),
                    sql.Identifier("mart_portnet_latest_eta"),
                )
            )
            if cursor.fetchone()[0]:
                raise RuntimeError(
                    "mart_portnet_latest_eta has multiple rows for a port call "
                    "and area"
                )

    summary = {
        "source_observations": source_counts["portnet_eta_observation"],
        "staging_rows": staging_rows,
        "rejected_records": source_counts["portnet_eta_rejected_event"],
        "duplicate_issues": duplicate_issues,
        "collision_issues": collision_issues,
        "latest_eta_rows": latest_rows,
    }
    LOGGER.info(
        "Pipeline health OK: observations=%d staging=%d rejected=%d "
        "duplicates=%d collisions=%d latest_eta=%d",
        summary["source_observations"],
        summary["staging_rows"],
        summary["rejected_records"],
        summary["duplicate_issues"],
        summary["collision_issues"],
        summary["latest_eta_rows"],
    )
    return summary


with DAG(
    dag_id="portpulse_daily_pipeline",
    description="Ingest Portnet V1 and build and verify PortPulse dbt analytics",
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
    verify_task = PythonOperator(
        task_id="verify_pipeline",
        python_callable=verify_pipeline,
    )

    ingest_task >> build_task >> verify_task
