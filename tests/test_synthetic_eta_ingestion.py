"""Synthetic ETA validation and isolated PostgreSQL ingestion checks."""

import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import psycopg

from scripts.event_model import PortCallEvent
from scripts.generate_synthetic_eta import build_dataset
from scripts.ingest_synthetic_eta import ingest_file, read_events, store_events
from tests.postgres import apply_migrations, create_test_schema


def write_events(path, events, malformed_line=False):
    lines = [json.dumps(event, separators=(",", ":")) for event in events]
    if malformed_line:
        lines.append('{"event_id":')
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


class SyntheticEtaInputTests(unittest.TestCase):
    def test_reader_separates_valid_invalid_and_malformed_records(self):
        events, _ = build_dataset(11)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "synthetic.jsonl"
            write_events(path, events, malformed_line=True)
            valid, rejected = read_events(path)

        self.assertTrue(all(PortCallEvent.model_validate(event) for event in valid))
        self.assertEqual(
            [item["reason_code"] for item in rejected].count("INVALID_SCHEMA_VERSION"),
            1,
        )
        self.assertEqual(
            [item["reason_code"] for item in rejected].count("INVALID_JSON"), 1
        )


class SyntheticEtaIngestionDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.database_url = create_test_schema(self, "synthetic_eta")
        with psycopg.connect(self.database_url) as connection:
            self.schema = connection.execute("SELECT current_schema()").fetchone()[0]
            connection.execute(
                "CREATE TABLE portnet_eta_observation (event_id TEXT PRIMARY KEY)"
            )
            connection.execute(
                "INSERT INTO portnet_eta_observation VALUES ('real-observation')"
            )
        apply_migrations(self.database_url, "010_create_synthetic_eta_schema.sql")
        apply_migrations(self.database_url, "010_create_synthetic_eta_schema.sql")

    def test_small_and_full_ingestion_are_isolated_and_idempotent(self):
        small_events, small_summary = build_dataset(50)
        full_events, full_summary = build_dataset(1000)

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "synthetic.jsonl"

            write_events(path, small_events, malformed_line=True)
            small_result = ingest_file(path, self.database_url, self.schema)
            self.assertEqual(small_result["accepted"], small_summary["eta_updates"])
            self.assertEqual(small_result["rejected"], 5)
            self.assertEqual(small_result["duplicates"], 7)
            self.assertEqual(small_result["inserted"], small_summary["eta_updates"])

            write_events(path, full_events, malformed_line=True)
            full_result = ingest_file(path, self.database_url, self.schema)
            self.assertEqual(full_result["accepted"], full_summary["eta_updates"])
            self.assertEqual(
                full_result["rejected"],
                full_summary["scenario_categories"]["invalid_events"] + 1,
            )
            self.assertEqual(
                full_result["duplicates"],
                full_summary["scenario_categories"]["duplicate_deliveries"],
            )
            self.assertEqual(
                full_result["inserted"],
                full_summary["eta_updates"] - small_summary["eta_updates"],
            )

            repeated_result = ingest_file(path, self.database_url, self.schema)

        observation_table = f"{self.schema}.eta_observation"
        rejected_table = f"{self.schema}.eta_rejected_event"
        quality_table = f"{self.schema}.eta_quality_issue"
        with psycopg.connect(self.database_url) as connection:
            observation_count = connection.execute(
                f"SELECT count(*) FROM {observation_table}"
            ).fetchone()[0]
            rejected_count = connection.execute(
                f"SELECT count(*) FROM {rejected_table}"
            ).fetchone()[0]
            duplicate_count = connection.execute(
                f"SELECT count(*) FROM {quality_table} "
                "WHERE issue_code = 'EXACT_DUPLICATE'"
            ).fetchone()[0]
            rejected_source_fields = connection.execute(
                f"SELECT source_id, event_time_utc, received_at_utc "
                f"FROM {rejected_table} WHERE reason_code = 'INVALID_SCHEMA_VERSION'"
            ).fetchall()
            source_ids = {
                row[0]
                for row in connection.execute(
                    f"SELECT DISTINCT source_id FROM {observation_table}"
                ).fetchall()
            }
            portnet_count = connection.execute(
                "SELECT count(*) FROM portnet_eta_observation"
            ).fetchone()[0]

        self.assertEqual(observation_count, full_summary["eta_updates"])
        self.assertEqual(
            rejected_count,
            full_summary["scenario_categories"]["invalid_events"] + 1,
        )
        self.assertEqual(
            duplicate_count,
            full_summary["scenario_categories"]["duplicate_deliveries"],
        )
        self.assertEqual(len(rejected_source_fields), 90)
        self.assertTrue(
            all(
                source_id and event_time is not None and received_at is not None
                for source_id, event_time, received_at in rejected_source_fields
            )
        )
        self.assertEqual(source_ids, {"carrier_sim", "coordinator_sim", "terminal_sim"})
        self.assertEqual(portnet_count, 1)
        self.assertEqual(repeated_result["inserted"], 0)
        self.assertEqual(repeated_result["accepted"], full_summary["eta_updates"])
        self.assertEqual(repeated_result["rejected"], full_result["rejected"])
        self.assertEqual(repeated_result["duplicates"], full_result["duplicates"])

    def test_conflicting_payloads_with_one_event_id_create_quality_issue(self):
        events, _ = build_dataset(1)
        original = events[0]
        conflicting = original.copy()
        changed_eta = datetime.fromisoformat(
            conflicting["eta_utc"].replace("Z", "+00:00")
        ) + timedelta(minutes=30)
        conflicting["eta_utc"] = changed_eta.isoformat().replace("+00:00", "Z")

        first = store_events([original], [], self.database_url, self.schema)
        second = store_events([conflicting], [], self.database_url, self.schema)

        quality_table = f"{self.schema}.eta_quality_issue"
        observation_table = f"{self.schema}.eta_observation"
        with psycopg.connect(self.database_url) as connection:
            collision_count = connection.execute(
                f"SELECT count(*) FROM {quality_table} "
                "WHERE issue_code = 'ID_COLLISION'"
            ).fetchone()[0]
            observation_count = connection.execute(
                f"SELECT count(*) FROM {observation_table}"
            ).fetchone()[0]

        self.assertEqual(first["inserted"], 1)
        self.assertEqual(second["collisions"], 1)
        self.assertEqual(second["inserted"], 1)
        self.assertEqual(collision_count, 1)
        self.assertEqual(observation_count, 2)

    def test_distinct_seeds_load_without_identity_collisions(self):
        first_events, first_summary = build_dataset(50, seed=42)
        second_events, second_summary = build_dataset(50, seed=7)
        first_valid = [
            event for event in first_events if event["schema_version"] == "v1"
        ]
        second_valid = [
            event for event in second_events if event["schema_version"] == "v1"
        ]

        first = store_events(first_valid, [], self.database_url, self.schema)
        second = store_events(second_valid, [], self.database_url, self.schema)
        repeated = store_events(first_valid, [], self.database_url, self.schema)

        observation_table = f"{self.schema}.eta_observation"
        quality_table = f"{self.schema}.eta_quality_issue"
        with psycopg.connect(self.database_url) as connection:
            observation_count = connection.execute(
                f"SELECT count(*) FROM {observation_table}"
            ).fetchone()[0]
            collision_count = connection.execute(
                f"SELECT count(*) FROM {quality_table} "
                "WHERE issue_code = 'ID_COLLISION'"
            ).fetchone()[0]

        self.assertEqual(first["inserted"], first_summary["eta_updates"])
        self.assertEqual(second["inserted"], second_summary["eta_updates"])
        self.assertEqual(repeated["inserted"], 0)
        self.assertEqual(
            observation_count,
            first_summary["eta_updates"] + second_summary["eta_updates"],
        )
        self.assertEqual(collision_count, 0)


if __name__ == "__main__":
    unittest.main()
