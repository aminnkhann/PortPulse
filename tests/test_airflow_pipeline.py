"""Tests for PortPulse Airflow orchestration."""

import subprocess
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from airflow.dag_processing.dagbag import DagBag

from dags.portpulse_daily_pipeline import (
    PROJECT_ROOT,
    _run_dates,
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

    def test_bounded_lookback_defaults_to_seven_days(self):
        context = {
            "dag_run": SimpleNamespace(conf={"run_date": "2026-11-12"}),
            "logical_date": datetime(2026, 11, 12, tzinfo=timezone.utc),
        }
        self.assertEqual(
            _run_dates(context),
            [
                "2026-11-06",
                "2026-11-07",
                "2026-11-08",
                "2026-11-09",
                "2026-11-10",
                "2026-11-11",
                "2026-11-12",
            ],
        )
        context["dag_run"].conf["lookback_days"] = 1
        self.assertEqual(_run_dates(context), ["2026-11-12"])

    def test_rejects_unbounded_or_invalid_lookbacks(self):
        for days in (0, 31, -1, True, 2.5, "7"):
            with self.subTest(days=days), self.assertRaises(ValueError):
                _run_dates({
                    "dag_run": SimpleNamespace(
                        conf={"run_date": "2026-11-12", "lookback_days": days}
                    ),
                    "logical_date": datetime(2026, 11, 12, tzinfo=timezone.utc),
                })

    @patch("dags.portpulse_daily_pipeline.time.sleep")
    @patch("dags.portpulse_daily_pipeline.subprocess.run")
    @patch("dags.portpulse_daily_pipeline.get_current_context")
    def test_ingestion_uses_configured_utc_date(
        self,
        get_context,
        run_command,
        sleep,
    ):
        get_context.return_value = {
            "dag_run": SimpleNamespace(
                conf={"run_date": "2026-11-12", "lookback_days": 1}
            ),
            "logical_date": datetime(2026, 11, 12, tzinfo=timezone.utc),
        }

        ingest_portnet()

        command = run_command.call_args.args[0]
        self.assertEqual(command[-2:], ["--date", "2026-11-12"])
        self.assertEqual(run_command.call_args.kwargs["cwd"], PROJECT_ROOT)
        self.assertTrue(run_command.call_args.kwargs["check"])
        sleep.assert_not_called()

    @patch("dags.portpulse_daily_pipeline.time.sleep")
    @patch("dags.portpulse_daily_pipeline.subprocess.run")
    @patch("dags.portpulse_daily_pipeline.get_current_context")
    def test_ingestion_uses_utc_logical_date_when_no_override(
        self,
        get_context,
        run_command,
        sleep,
    ):
        get_context.return_value = {
            "dag_run": SimpleNamespace(conf={"lookback_days": 1}),
            "logical_date": datetime.fromisoformat("2026-11-12T00:30:00+02:00"),
        }

        ingest_portnet()

        self.assertEqual(run_command.call_args.args[0][-1], "2026-11-11")
        sleep.assert_not_called()

    @patch("dags.portpulse_daily_pipeline.time.sleep")
    @patch("dags.portpulse_daily_pipeline.subprocess.run")
    @patch("dags.portpulse_daily_pipeline.get_current_context")
    def test_multiple_dates_are_ingested_oldest_first(
        self, get_context, run_command, sleep
    ):
        get_context.return_value = {
            "dag_run": SimpleNamespace(
                conf={"run_date": "2026-11-12", "lookback_days": 3}
            ),
            "logical_date": datetime(2026, 11, 12, tzinfo=timezone.utc),
        }
        ingest_portnet()
        self.assertEqual(
            [item.args[0][-1] for item in run_command.call_args_list],
            ["2026-11-10", "2026-11-11", "2026-11-12"],
        )
        self.assertEqual(sleep.call_count, 2)

    @patch("dags.portpulse_daily_pipeline.subprocess.run")
    def test_dbt_failure_propagates(self, run_command):
        run_command.side_effect = subprocess.CalledProcessError(1, "dbt")

        with self.assertRaises(subprocess.CalledProcessError):
            build_dbt_analytics()

        self.assertEqual(run_command.call_args.args[0], ["dbt", "build"])
        self.assertEqual(run_command.call_args.kwargs["cwd"], PROJECT_ROOT / "dbt")
        self.assertTrue(run_command.call_args.kwargs["check"])
