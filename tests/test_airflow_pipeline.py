"""Tests for PortPulse Airflow orchestration."""

import subprocess
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from airflow.dag_processing.dagbag import DagBag

from dags.portpulse_daily_pipeline import (
    PROJECT_ROOT,
    build_dbt_analytics,
    dag,
    ingest_portnet,
)


DAG_ID = "portpulse_daily_pipeline"


class AirflowDagTests(unittest.TestCase):
    def test_dag_imports_with_two_sequential_tasks(self):
        dagbag = DagBag(dag_folder=str(PROJECT_ROOT / "dags"))
        self.assertEqual(dagbag.import_errors, {})
        loaded_dag = dagbag.dags.get(DAG_ID)
        self.assertIsNotNone(loaded_dag)
        self.assertEqual(
            loaded_dag.task_ids,
            ["ingest_portnet", "build_dbt_analytics"],
        )
        self.assertEqual(
            loaded_dag.get_task("ingest_portnet").downstream_task_ids,
            {"build_dbt_analytics"},
        )
        self.assertEqual(
            loaded_dag.get_task("build_dbt_analytics").downstream_task_ids, set()
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

        self.assertEqual(run_command.call_args.args[0], ["dbt", "build"])
        self.assertEqual(run_command.call_args.kwargs["cwd"], PROJECT_ROOT / "dbt")
        self.assertTrue(run_command.call_args.kwargs["check"])
