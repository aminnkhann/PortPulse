"""Structure checks for the optional streaming Airflow DAG."""

import unittest
from unittest.mock import patch

from dags.portpulse_streaming_cdc import (
    build_streaming_dbt_models,
    dag as streaming_dag,
)


class StreamingDagTests(unittest.TestCase):
    def test_streaming_dag_is_manual_only_and_starts_paused(self):
        self.assertIsNone(streaming_dag.schedule)
        self.assertEqual(streaming_dag.max_active_runs, 1)
        self.assertTrue(streaming_dag.is_paused_upon_creation)

    def test_streaming_graph_uses_bounded_consume_then_dbt(self):
        consumer = streaming_dag.get_task("consume_cdc_batch")
        self.assertEqual(consumer.max_messages, 100)
        self.assertEqual(consumer.max_batch_size, 50)
        self.assertEqual(consumer.poll_timeout, 5)
        self.assertEqual(consumer.commit_cadence, "end_of_operator")
        self.assertEqual(consumer.downstream_task_ids, {"build_streaming_dbt_models"})
        self.assertEqual(
            streaming_dag.get_task("build_streaming_dbt_models").downstream_task_ids,
            set(),
        )

    @patch("dags.portpulse_streaming_cdc.subprocess.run")
    def test_build_runs_partition_guard_data_test(self, run):
        build_streaming_dbt_models()
        self.assertIn("assert_stream_key_single_partition", run.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
