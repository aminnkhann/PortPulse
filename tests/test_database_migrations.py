"""Regression checks for schema upgrades and preserved database history."""

import unittest
from datetime import datetime, timezone

import psycopg
from psycopg.types.json import Jsonb

from scripts import ingest_fixture
from tests.postgres import apply_migrations, create_test_schema


class DatabaseMigrationTests(unittest.TestCase):
    def setUp(self):
        self.database_url = create_test_schema(self, "portpulse_migration")

    def test_migration_backfills_existing_rows_and_is_safe_to_replay(self):
        apply_migrations(
            self.database_url,
            "001_create_core_tables.sql",
            "002_rejected_event_details.sql",
        )
        event = ingest_fixture.read_fixture()[0][0]

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
        apply_migrations(self.database_url, "003_raw_event_ingested_at.sql")
        apply_migrations(self.database_url, "003_raw_event_ingested_at.sql")
        after = datetime.now(timezone.utc)

        with psycopg.connect(self.database_url) as connection:
            received_at, ingested_at = connection.execute(
                "SELECT received_at_utc, ingested_at_utc FROM raw_event"
            ).fetchone()
        self.assertIsNotNone(ingested_at)
        self.assertNotEqual(received_at, ingested_at)
        self.assertGreaterEqual(ingested_at, before)
        self.assertLessEqual(ingested_at, after)

    def test_migration_007_upgrades_schema_without_losing_legacy_calls(self):
        apply_migrations(
            self.database_url,
            "001_create_core_tables.sql",
            "002_rejected_event_details.sql",
            "003_raw_event_ingested_at.sql",
            "004_add_digitraffic_source.sql",
            "005_allow_unavailable_state.sql",
            "006_add_portnet_ais_sources.sql",
        )
        with psycopg.connect(self.database_url) as connection:
            connection.execute(
                "INSERT INTO port_call (port_call_id) VALUES ('existing-call')"
            )

        apply_migrations(self.database_url, "007_create_portnet_eta_tables.sql")

        with psycopg.connect(self.database_url) as connection:
            existing_call = connection.execute(
                "SELECT port_call_id FROM port_call"
            ).fetchone()[0]
            observation_table = connection.execute(
                "SELECT to_regclass('portnet_eta_observation')"
            ).fetchone()[0]
        self.assertEqual(existing_call, "existing-call")
        self.assertIsNotNone(observation_table)

    def test_migration_009_preserves_sources_referenced_by_history_or_state(self):
        apply_migrations(self.database_url)
        with psycopg.connect(self.database_url) as connection:
            connection.execute(
                """INSERT INTO source_system (source_id, priority, freshness_minutes)
                   VALUES ('digitraffic', 1, 120),
                          ('portnet', 2, 120), ('ais', 1, 120)"""
            )
            connection.execute(
                "INSERT INTO port_call (port_call_id) VALUES ('legacy-call')"
            )
            connection.execute(
                """INSERT INTO raw_event
                   (event_id, port_call_id, source_id, event_time_utc,
                    received_at_utc, eta_utc, schema_version,
                    canonical_payload_sha256, raw_payload)
                   VALUES ('legacy-event', 'legacy-call', 'digitraffic',
                           '2026-01-15T10:00:00Z', '2026-01-15T10:01:00Z',
                           '2026-01-15T12:00:00Z', 'v1', repeat('0', 64), '{}')"""
            )
            connection.execute(
                """INSERT INTO port_call_state
                   (port_call_id, status, trusted_eta_utc, source_id,
                    winning_event_id, as_of_utc, rules_version)
                   VALUES ('legacy-call', 'TRUSTED', '2026-01-15T12:00:00Z',
                           'ais', 'legacy-event', '2026-01-15T11:00:00Z', '1')"""
            )

        apply_migrations(self.database_url, "009_remove_unused_sources.sql")

        with psycopg.connect(self.database_url) as connection:
            remaining = connection.execute(
                "SELECT source_id FROM source_system WHERE source_id IN "
                "('digitraffic', 'portnet', 'ais') ORDER BY source_id"
            ).fetchall()
        self.assertEqual(remaining, [("ais",), ("digitraffic",)])


if __name__ == "__main__":
    unittest.main()
