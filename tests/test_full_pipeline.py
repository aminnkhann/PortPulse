"""Check that one manual DAG orchestrates both existing ETA ingestion paths."""

import subprocess
import unittest
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import patch

from dags.portpulse_full_pipeline import dag, fetch_real_portnet, portnet_dates


class FullPipelineTests(unittest.TestCase):
    def test_one_unpaused_manual_pipeline_with_both_inputs_before_dbt(self):
        self.assertEqual(dag.dag_id, "portpulse_full_pipeline")
        self.assertIsNone(dag.schedule)
        self.assertFalse(dag.is_paused_upon_creation)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(
            set(dag.task_ids),
            {
                "fetch_real_portnet",
                "generate_synthetic_eta",
                "ingest_synthetic_eta",
                "build_analytics",
            },
        )
        self.assertEqual(
            dag.get_task("generate_synthetic_eta").downstream_task_ids,
            {"ingest_synthetic_eta"},
        )
        self.assertEqual(
            dag.get_task("fetch_real_portnet").downstream_task_ids,
            {"build_analytics"},
        )
        self.assertEqual(
            dag.get_task("ingest_synthetic_eta").downstream_task_ids,
            {"build_analytics"},
        )

    def test_real_date_window_defaults_to_seven_utc_days(self):
        self.assertEqual(
            portnet_dates({}, date(2026, 11, 17)),
            [
                "2026-11-11",
                "2026-11-12",
                "2026-11-13",
                "2026-11-14",
                "2026-11-15",
                "2026-11-16",
                "2026-11-17",
            ],
        )

    @patch("dags.portpulse_full_pipeline.time.sleep")
    @patch("dags.portpulse_full_pipeline.subprocess.run")
    @patch("dags.portpulse_full_pipeline.get_current_context")
    def test_default_window_uses_utc_dag_run_date_across_midnight(
        self, context, run, sleep
    ):
        context.return_value = {
            "dag_run": SimpleNamespace(
                conf={},
                logical_date=datetime.fromisoformat("2026-11-18T00:30:00+02:00"),
            )
        }
        fetch_real_portnet()
        self.assertEqual(run.call_args_list[0].args[0][-1], "2026-11-11")
        self.assertEqual(run.call_args.args[0][-1], "2026-11-17")

        self.assertEqual(
            portnet_dates(
                {"run_date": "2026-11-17", "lookback_days": 1},
                date(2026, 11, 17),
            ),
            ["2026-11-17"],
        )

    @patch("dags.portpulse_full_pipeline.time.sleep")
    @patch("dags.portpulse_full_pipeline.subprocess.run")
    @patch("dags.portpulse_full_pipeline.get_current_context")
    def test_manual_run_uses_run_after_when_logical_date_is_missing(
        self, context, run, sleep
    ):
        context.return_value = {
            "dag_run": SimpleNamespace(
                conf={"lookback_days": 1},
                logical_date=None,
                run_after=datetime.fromisoformat("2026-11-22T00:30:00+02:00"),
            )
        }

        fetch_real_portnet()

        self.assertEqual(run.call_args.args[0][-1], "2026-11-21")
        sleep.assert_not_called()

    @patch("dags.portpulse_full_pipeline.time.sleep")
    @patch("dags.portpulse_full_pipeline.subprocess.run")
    @patch("dags.portpulse_full_pipeline.get_current_context")
    def test_explicit_run_date_overrides_run_timestamp(
        self, context, run, sleep
    ):
        context.return_value = {
            "dag_run": SimpleNamespace(
                conf={"run_date": "2026-11-20", "lookback_days": 2},
                logical_date=datetime.fromisoformat("2026-11-17T12:00:00+00:00"),
                run_after=datetime.fromisoformat("2026-11-21T12:00:00+00:00"),
            )
        }

        fetch_real_portnet()

        self.assertEqual(
            [call.args[0][-1] for call in run.call_args_list],
            ["2026-11-19", "2026-11-20"],
        )
        sleep.assert_called_once_with(1)

    @patch("dags.portpulse_full_pipeline.subprocess.run")
    @patch("dags.portpulse_full_pipeline.get_current_context")
    def test_rejects_missing_or_naive_run_timestamp(self, context, run):
        for logical_date, run_after in (
            (None, None),
            (
                datetime(2026, 11, 17),
                datetime.fromisoformat("2026-11-17T12:00:00+00:00"),
            ),
        ):
            with self.subTest(logical_date=logical_date, run_after=run_after):
                context.return_value = {
                    "dag_run": SimpleNamespace(
                        conf={}, logical_date=logical_date, run_after=run_after
                    )
                }
                with self.assertRaisesRegex(ValueError, "timezone-aware"):
                    fetch_real_portnet()
        run.assert_not_called()

    def test_rejects_invalid_config(self):
        for conf in (
            {"lookback_days": 0},
            {"lookback_days": 31},
            {"lookback_days": True},
            {"lookback_days": "7"},
            {"run_date": "17-11-2026"},
            {"run_date": None},
        ):
            with self.subTest(conf=conf), self.assertRaises(ValueError):
                portnet_dates(conf, date(2026, 11, 17))

    @patch("dags.portpulse_full_pipeline.time.sleep")
    @patch("dags.portpulse_full_pipeline.subprocess.run")
    @patch("dags.portpulse_full_pipeline.get_current_context")
    def test_real_data_fetch_runs_sequentially(self, context, run, sleep):
        context.return_value = {
            "dag_run": SimpleNamespace(
                conf={"lookback_days": 2},
                logical_date=datetime.fromisoformat("2026-11-17T12:00:00+00:00"),
            )
        }
        fetch_real_portnet()
        self.assertEqual(
            [c.args[0][-1] for c in run.call_args_list],
            ["2026-11-16", "2026-11-17"],
        )
        self.assertTrue(all(c.kwargs["check"] for c in run.call_args_list))
        sleep.assert_called_once_with(1)

    @patch("dags.portpulse_full_pipeline.time.sleep")
    @patch("dags.portpulse_full_pipeline.subprocess.run")
    @patch("dags.portpulse_full_pipeline.get_current_context")
    def test_real_api_error_fails_task(self, context, run, sleep):
        context.return_value = {
            "dag_run": SimpleNamespace(
                conf={"run_date": "2026-11-17", "lookback_days": 1},
                logical_date=datetime.fromisoformat("2026-11-17T12:00:00+00:00"),
            )
        }
        run.side_effect = subprocess.CalledProcessError(1, "ingest_portnet")
        with self.assertRaises(subprocess.CalledProcessError):
            fetch_real_portnet()


if __name__ == "__main__":
    unittest.main()
