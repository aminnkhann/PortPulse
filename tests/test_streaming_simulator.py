"""Fixed-sequence checks for the port traffic simulator."""

import os
import unittest
import uuid

import psycopg

from scripts.simulate_port_traffic import EVENTS, run_scenario


class SimulatorScenarioTests(unittest.TestCase):
    def test_sequence_has_expected_database_operations_and_final_eta(self):
        self.assertEqual(
            [(event[0], event[1]) for event in EVENTS],
            [
                ("INSERT", "SIM-001"),
                ("UPDATE", "SIM-001"),
                ("UPDATE", "SIM-001"),
                ("INSERT", "SIM-002"),
                ("DELETE", "SIM-002"),
            ],
        )
        self.assertEqual(EVENTS[2][3], "2026-11-13T14:50:00Z")
        self.assertIsNone(EVENTS[4][3])
        self.assertEqual(EVENTS[0][4], "2026-11-13T10:00:00Z")

    def test_reusing_scenario_id_fails_without_adding_rows(self):
        database_url = os.environ.get("PORTPULSE_TEST_DATABASE_URL")
        if not database_url:
            self.skipTest("set PORTPULSE_TEST_DATABASE_URL for PostgreSQL checks")
        scenario_id = f"simulator-{uuid.uuid4().hex}"
        try:
            run_scenario(database_url, scenario_id, 0)
            with self.assertRaises(psycopg.errors.UniqueViolation):
                run_scenario(database_url, scenario_id, 0)
            with psycopg.connect(database_url) as connection:
                count = connection.execute(
                    """SELECT count(*) FROM stream_demo.port_eta_state
                       WHERE scenario_id = %s""",
                    (scenario_id,),
                ).fetchone()[0]
            self.assertEqual(count, 1)
        finally:
            with psycopg.connect(database_url) as connection:
                connection.execute(
                    "DELETE FROM stream_demo.port_eta_state WHERE scenario_id = %s",
                    (scenario_id,),
                )


if __name__ == "__main__":
    unittest.main()
