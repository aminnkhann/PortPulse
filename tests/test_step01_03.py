"""Focused contract, fixture, and ingestion checks for Steps 01–03."""

import json
import os
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import psycopg
from pydantic import ValidationError
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb

from scripts import ingest_fixture
from scripts.event_model import PortCallEvent, build_json_schema


ROOT = Path(__file__).resolve().parents[1]


class FixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.valid, cls.rejected = ingest_fixture.read_fixture()

    def test_fixture_counts_and_pydantic_validation(self):
        self.assertEqual((len(self.valid), len(self.rejected)), (30, 1))
        self.assertTrue(
            all(PortCallEvent.model_validate(event) for event in self.valid)
        )
        self.assertEqual(self.rejected[0]["reason_code"], "INVALID_SCHEMA_VERSION")
        self.assertEqual(len(self.valid) + len(self.rejected), 31)

    def test_json_schema_is_generated_from_the_pydantic_model(self):
        contract = json.loads(
            (ROOT / "contracts/port_call_event_v1.schema.json").read_text()
        )
        self.assertEqual(contract, build_json_schema())

    def test_generated_fixture_matches_the_frozen_fixture(self):
        from scripts import generate_fixture

        frozen = [
            json.loads(line)
            for line in (ROOT / "fixtures/port_call_v1.jsonl").read_text().splitlines()
        ]
        self.assertEqual(generate_fixture.build_events(), frozen)

    def test_invalid_required_field_is_reported(self):
        event = self.valid[0].copy()
        del event["port_call_id"]
        with self.assertRaises(ValidationError) as raised:
            PortCallEvent.model_validate(event)
        reason, field = ingest_fixture.validation_reason(raised.exception)
        self.assertEqual((reason, field), ("MISSING_REQUIRED_FIELD", "port_call_id"))

    def test_canonical_hash_ignores_simulated_receipt_time(self):
        event = self.valid[0]
        changed_receipt = {**event, "received_at_utc": "2026-01-15T10:59:00Z"}
        self.assertEqual(
            ingest_fixture.canonical_hash(event),
            ingest_fixture.canonical_hash(changed_receipt),
        )

    def test_duplicates_and_id_collisions_are_grouped(self):
        unique, duplicates, collisions = ingest_fixture.group_events(self.valid)
        self.assertEqual((len(unique), len(duplicates)), (28, 2))
        self.assertEqual(list(collisions), ["COLLISION-01"])
        self.assertEqual(len(collisions["COLLISION-01"]), 2)

    def test_golden_output_includes_all_over_threshold_delivery_delays(self):
        expected = json.loads((ROOT / "fixtures/expected_output.json").read_text())
        golden_delays = {
            issue["event_id"]: issue["delay_minutes"]
            for issue in expected["issues"]
            if issue["code"] == "DELAYED_DELIVERY"
        }
        fixture_delays = {}
        for event in self.valid:
            event_time = datetime.fromisoformat(
                event["event_time_utc"].replace("Z", "+00:00")
            )
            received_at = datetime.fromisoformat(
                event["received_at_utc"].replace("Z", "+00:00")
            )
            delay = int((received_at - event_time).total_seconds() / 60)
            if delay > 30:
                fixture_delays[event["event_id"]] = delay
        self.assertEqual(golden_delays, fixture_delays)


class DatabaseIntegrationTests(unittest.TestCase):
    """Use an isolated schema in the explicitly supplied test database."""

    def setUp(self):
        self.base_url = os.environ.get("PORTPULSE_TEST_DATABASE_URL")
        if not self.base_url:
            self.skipTest("set PORTPULSE_TEST_DATABASE_URL to run PostgreSQL checks")

        self.schema_name = f"portpulse_test_{uuid.uuid4().hex}"
        with psycopg.connect(self.base_url) as connection:
            connection.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema_name))
            )
        self.database_url = make_conninfo(
            self.base_url, options=f"-c search_path={self.schema_name}"
        )

    def tearDown(self):
        if getattr(self, "schema_name", None):
            with psycopg.connect(self.base_url) as connection:
                connection.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(self.schema_name)
                    )
                )

    def apply_migrations(self, *names):
        with psycopg.connect(self.database_url) as connection:
            for name in names:
                migration = ROOT / "migrations" / name
                connection.execute(migration.read_text(), prepare=False)

    def test_migration_backfills_existing_rows_and_sets_default(self):
        self.apply_migrations(
            "001_create_core_tables.sql", "002_rejected_event_details.sql"
        )
        valid, _ = ingest_fixture.read_fixture()
        event = valid[0]

        with psycopg.connect(self.database_url) as connection:
            connection.execute(
                "INSERT INTO port_call (port_call_id) VALUES (%s)",
                (event["port_call_id"],),
            )
            connection.execute(
                """
                INSERT INTO raw_event
                    (event_id, port_call_id, source_id, event_time_utc,
                     received_at_utc, eta_utc, schema_version,
                     canonical_payload_sha256, raw_payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    event["event_id"],
                    event["port_call_id"],
                    event["source_id"],
                    ingest_fixture.timestamp(event, "event_time_utc"),
                    ingest_fixture.timestamp(event, "received_at_utc"),
                    ingest_fixture.timestamp(event, "eta_utc"),
                    event["schema_version"],
                    ingest_fixture.canonical_hash(event),
                    Jsonb(event),
                ),
            )

        before = datetime.now(timezone.utc)
        self.apply_migrations("003_raw_event_ingested_at.sql")
        self.apply_migrations("003_raw_event_ingested_at.sql")
        after = datetime.now(timezone.utc)

        with psycopg.connect(self.database_url) as connection:
            row = connection.execute(
                "SELECT received_at_utc, ingested_at_utc FROM raw_event"
            ).fetchone()
            self.assertIsNotNone(row[1])
            self.assertNotEqual(row[0], row[1])
            self.assertGreaterEqual(row[1], before)
            self.assertLessEqual(row[1], after)

    def test_ingestion_is_idempotent_and_sets_actual_time(self):
        self.apply_migrations(
            "001_create_core_tables.sql",
            "002_rejected_event_details.sql",
            "003_raw_event_ingested_at.sql",
        )
        valid, rejected = ingest_fixture.read_fixture()
        before = datetime.now(timezone.utc)
        with patch.dict(os.environ, {"DATABASE_URL": self.database_url}):
            first = ingest_fixture.store_fixture(valid, rejected)
            second = ingest_fixture.store_fixture(valid, rejected)
        after = datetime.now(timezone.utc)

        with psycopg.connect(self.database_url) as connection:
            raw_count = connection.execute("SELECT count(*) FROM raw_event").fetchone()[
                0
            ]
            rejected_count = connection.execute(
                "SELECT count(*) FROM rejected_event"
            ).fetchone()[0]
            port_call_count = connection.execute(
                "SELECT count(*) FROM port_call"
            ).fetchone()[0]
            issues = dict(
                connection.execute(
                    "SELECT issue_code, count(*) FROM quality_issue GROUP BY issue_code"
                ).fetchall()
            )
            timestamps = connection.execute(
                "SELECT received_at_utc, ingested_at_utc FROM raw_event"
            ).fetchall()

        self.assertEqual(first, (28, 2, 1))
        self.assertEqual(second, first)
        self.assertEqual((raw_count, rejected_count, port_call_count), (28, 1, 1))
        self.assertEqual(issues, {"EXACT_DUPLICATE": 2, "ID_COLLISION": 1})
        self.assertEqual(len(timestamps), 28)
        for received, ingested in timestamps:
            self.assertNotEqual(received, ingested)
            self.assertGreaterEqual(ingested, before)
            self.assertLessEqual(ingested, after)


if __name__ == "__main__":
    unittest.main()
