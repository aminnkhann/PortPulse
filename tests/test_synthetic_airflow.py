"""Structure and configuration checks for the synthetic ETA Airflow DAG."""

import unittest
import os
from pathlib import Path
from unittest.mock import patch

from dags.portpulse_synthetic_demo import (
    dag,
    input_path_for_run,
    validate_run_config,
)


class SyntheticAirflowDagTests(unittest.TestCase):
    def test_dag_is_paused_manual_and_has_three_sequential_tasks(self):
        self.assertEqual(dag.dag_id, "portpulse_synthetic_demo")
        self.assertEqual(
            dag.task_ids,
            [
                "generate_synthetic_eta",
                "ingest_synthetic_eta",
                "build_synthetic_eta_analytics",
            ],
        )
        self.assertTrue(dag.is_paused_upon_creation)
        self.assertIsNone(dag.schedule)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(
            dag.get_task("generate_synthetic_eta").downstream_task_ids,
            {"ingest_synthetic_eta"},
        )
        self.assertEqual(
            dag.get_task("ingest_synthetic_eta").downstream_task_ids,
            {"build_synthetic_eta_analytics"},
        )

    def test_configuration_defaults_and_overrides(self):
        self.assertEqual(validate_run_config({}), (1000, 42))
        self.assertEqual(validate_run_config({"port_calls": 50, "seed": 7}), (50, 7))

    def test_configuration_rejects_invalid_values(self):
        for conf in (
            None,
            {"port_calls": 0},
            {"port_calls": 1001},
            {"port_calls": True},
            {"port_calls": "50"},
            {"seed": 1.5},
            {"seed": False},
        ):
            with self.subTest(conf=conf), self.assertRaises(ValueError):
                validate_run_config(conf)

    def test_input_path_is_stable_per_run_and_separate_between_runs(self):
        with patch.dict(os.environ, {"AIRFLOW_HOME": "/shared/airflow"}):
            first_path = input_path_for_run("manual__first")
            self.assertEqual(first_path, input_path_for_run("manual__first"))
            self.assertNotEqual(first_path, input_path_for_run("manual__second"))
        self.assertEqual(first_path.parents[1], Path("/shared/airflow"))
        self.assertEqual(first_path.parent.name, "portpulse_synthetic_demo")


if __name__ == "__main__":
    unittest.main()
