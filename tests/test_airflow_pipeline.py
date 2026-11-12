"""Tests for the PortPulse Airflow orchestration and verification task."""

import os
import subprocess
import sys
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import psycopg
from airflow.models.dagbag import DagBag
from psycopg import sql
from psycopg.conninfo import make_conninfo

from dags.portpulse_daily_pipeline import (
    PROJECT_ROOT,
    build_dbt_analytics,
    dag,
    ingest_portnet,
    verify_pipeline,
)


DAG_ID = "portpulse_daily_pipeline"
SOURCE_TABLES = (
    "portnet_eta_observation",
    "portnet_eta_rejected_event",
    "portnet_eta_quality_issue",
)


class AirflowDagTests(unittest.TestCase):
    def test_dag_imports_with_three_sequential_tasks(self):
        dagbag = DagBag(dag_folder=str(PROJECT_ROOT / "dags"), include_examples=False)
        self.assertEqual(dagbag.import_errors, {})
        loaded_dag = dagbag.dags.get(DAG_ID)
        self.assertIsNotNone(loaded_dag)
        self.assertEqual(
            loaded_dag.task_ids,
            ["ingest_portnet", "build_dbt_analytics", "verify_pipeline"],
        )
        self.assertEqual(
            loaded_dag.get_task("ingest_portnet").downstream_task_ids,
            {"build_dbt_analytics"},
        )
        self.assertEqual(
            loaded_dag.get_task("build_dbt_analytics").downstream_task_ids,
            {"verify_pipeline"},
        )
        self.assertEqual(dag.dag_id, DAG_ID)
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertTrue(dag.is_paused_upon_creation)

    @patch("dags.portpulse_daily_pipeline.subprocess.run")
    @patch("dags.portpulse_daily_pipeline.get_current_context")
    def test_ingestion_uses_configured_utc_date(
        self,
        get_context,
        run_command,
    ):
        get_context.return_value = {
            "dag_run": SimpleNamespace(conf={"run_date": "2026-11-12"}),
            "logical_date": datetime(2026, 11, 12, tzinfo=timezone.utc),
        }

        ingest_portnet()

        command = run_command.call_args.args[0]
        self.assertEqual(command[-2:], ["--date", "2026-11-12"])
        self.assertEqual(run_command.call_args.kwargs["cwd"], PROJECT_ROOT)
        self.assertTrue(run_command.call_args.kwargs["check"])

    @patch("dags.portpulse_daily_pipeline.subprocess.run")
    @patch("dags.portpulse_daily_pipeline.get_current_context")
    def test_ingestion_uses_utc_logical_date_when_no_override(
        self,
        get_context,
        run_command,
    ):
        get_context.return_value = {
            "dag_run": SimpleNamespace(conf={}),
            "logical_date": datetime.fromisoformat("2026-11-12T00:30:00+02:00"),
        }

        ingest_portnet()

        self.assertEqual(run_command.call_args.args[0][-1], "2026-11-11")

    @patch("dags.portpulse_daily_pipeline.subprocess.run")
    def test_dbt_failure_propagates(self, run_command):
        run_command.side_effect = subprocess.CalledProcessError(1, "dbt")

        with self.assertRaises(subprocess.CalledProcessError):
            build_dbt_analytics()

        self.assertEqual(
            run_command.call_args.args[0],
            [str(Path(sys.executable).with_name("dbt")), "build"],
        )
        self.assertEqual(run_command.call_args.kwargs["cwd"], PROJECT_ROOT / "dbt")
        self.assertTrue(run_command.call_args.kwargs["check"])


class PipelineVerificationTests(unittest.TestCase):
    def setUp(self):
        self.base_url = os.environ.get("PORTPULSE_TEST_DATABASE_URL")
        if not self.base_url:
            self.skipTest("set PORTPULSE_TEST_DATABASE_URL for PostgreSQL checks")
        self.schema_name = f"portpulse_airflow_{uuid.uuid4().hex}"
        with psycopg.connect(self.base_url) as connection:
            connection.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema_name))
            )
        self.database_url = make_conninfo(self.base_url)
        self._create_objects()

    def tearDown(self):
        if getattr(self, "schema_name", None):
            with psycopg.connect(self.base_url) as connection:
                connection.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(self.schema_name)
                    )
                )

    def _create_objects(self):
        schema = sql.Identifier(self.schema_name)
        with psycopg.connect(self.database_url) as connection:
            for table_name in SOURCE_TABLES[1:]:
                connection.execute(
                    sql.SQL("CREATE TABLE {}.{} (issue_code TEXT)").format(
                        schema,
                        sql.Identifier(table_name),
                    )
                )
            connection.execute(
                sql.SQL(
                    """CREATE TABLE {}.portnet_eta_observation (
                           observation_id INTEGER,
                           port_call_id TEXT,
                           port_area_code TEXT
                       )"""
                ).format(schema)
            )
            connection.execute(
                sql.SQL("CREATE VIEW {}.stg_portnet_eta AS SELECT * FROM {}.{}").format(
                    schema,
                    schema,
                    sql.Identifier("portnet_eta_observation"),
                )
            )
            connection.execute(
                sql.SQL(
                    """CREATE TABLE {}.mart_portnet_latest_eta (
                           port_call_id TEXT,
                           port_area_code TEXT,
                           event_id TEXT,
                           eta_utc TIMESTAMPTZ
                       )"""
                ).format(schema)
            )
            connection.execute(
                sql.SQL(
                    """CREATE TABLE {}.mart_pipeline_quality (
                           metric_id INTEGER,
                           accepted_observations INTEGER,
                           rejected_observations INTEGER,
                           duplicate_issue_rows INTEGER,
                           collision_issue_rows INTEGER,
                           latest_eta_records INTEGER
                       )"""
                ).format(schema)
            )
            connection.execute(
                sql.SQL(
                    """INSERT INTO {}.mart_pipeline_quality
                       VALUES (1, 0, 0, 0, 0, 0)"""
                ).format(schema)
            )

    def test_empty_sources_and_zero_rejections_are_healthy(self):
        summary = verify_pipeline(
            self.database_url,
            source_schema=self.schema_name,
            dbt_schema=self.schema_name,
        )

        self.assertEqual(
            summary,
            {
                "source_observations": 0,
                "staging_rows": 0,
                "rejected_records": 0,
                "duplicate_issues": 0,
                "collision_issues": 0,
                "latest_eta_rows": 0,
            },
        )

    def test_rejects_inconsistent_quality_metrics(self):
        with psycopg.connect(self.database_url) as connection:
            connection.execute(
                sql.SQL(
                    "INSERT INTO {}.portnet_eta_observation "
                    "VALUES (1, 'call', 'VUOS')"
                )
                .format(sql.Identifier(self.schema_name))
            )

        with self.assertRaisesRegex(RuntimeError, "metrics do not match"):
            verify_pipeline(
                self.database_url,
                source_schema=self.schema_name,
                dbt_schema=self.schema_name,
            )

    def test_rejects_multiple_latest_rows_for_same_area(self):
        schema = sql.Identifier(self.schema_name)
        with psycopg.connect(self.database_url) as connection:
            connection.execute(
                sql.SQL(
                    """INSERT INTO {}.portnet_eta_observation
                       VALUES (1, 'call', 'VUOS'), (2, 'call', 'VUOS')"""
                ).format(schema)
            )
            connection.execute(
                sql.SQL(
                    """INSERT INTO {}.mart_portnet_latest_eta
                       VALUES ('call', 'VUOS', 'event-1', now()),
                              ('call', 'VUOS', 'event-2', now())"""
                ).format(schema)
            )
            connection.execute(
                sql.SQL(
                    """UPDATE {}.mart_pipeline_quality
                       SET accepted_observations = 2,
                           latest_eta_records = 2"""
                ).format(schema)
            )

        with self.assertRaisesRegex(RuntimeError, "multiple rows"):
            verify_pipeline(
                self.database_url,
                source_schema=self.schema_name,
                dbt_schema=self.schema_name,
            )
