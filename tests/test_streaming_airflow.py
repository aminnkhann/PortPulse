"""Structure checks for the optional streaming Airflow DAG."""

import unittest
from pathlib import Path

from airflow.dag_processing.dagbag import DagBag

from dags.portpulse_daily_pipeline import dag as daily_dag
from dags.portpulse_streaming_cdc import dag as streaming_dag


class StreamingDagTests(unittest.TestCase):
    def test_both_dags_import_and_daily_task_graph_is_preserved(self):
        self.assertEqual(
            daily_dag.task_ids,
            ["ingest_portnet", "build_dbt_analytics"],
        )
        dagbag = DagBag(dag_folder=str(Path(__file__).resolve().parents[1] / "dags"))
        self.assertEqual(dagbag.import_errors, {})
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


if __name__ == "__main__":
    unittest.main()
