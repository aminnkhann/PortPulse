"""Synthetic failure injections; these are not observed Portnet defects."""

import os
import unittest
import uuid
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from scripts import ingest_portnet


RECEIVED_AT = "2026-11-10T08:00:00.000000Z"


class InjectedPortnetReliabilityTests(unittest.TestCase):
    def test_injected_invalid_call_is_quarantined_without_unneeded_fields(self):
        calls = [{"portCallId": 0, "mmsi": 999999999, "agent": "synthetic"}]
        events, rejected, metrics = ingest_portnet.normalize_data(calls, RECEIVED_AT)
        self.assertEqual(events, [])
        self.assertEqual(metrics["rejected_portnet_call_records"], 1)
        self.assertNotIn("agent", rejected[0]["raw_payload"])
        self.assertNotIn("mmsi", rejected[0]["raw_payload"])

    def test_injected_invalid_eta_is_quarantined(self):
        call = {
            "portCallId": 5,
            "portAreaDetails": [
                {
                    "portAreaCode": "VUOS",
                    "eta": "not-a-timestamp",
                    "etaTimestamp": "2026-11-10T07:00:00Z",
                }
            ],
        }
        events, rejected, _ = ingest_portnet.normalize_data([call], RECEIVED_AT)
        self.assertEqual(events, [])
        self.assertEqual(rejected[0]["reason_code"], "INVALID_PORTNET_ETA")

    def test_injected_missing_area_is_quarantined_when_identity_is_ambiguous(self):
        call = {
            "portCallId": 5,
            "portAreaDetails": [
                {"eta": "2026-11-10T12:00:00Z", "etaTimestamp": "2026-11-10T07:00:00Z"},
                {
                    "portAreaCode": "VUOS",
                    "eta": "2026-11-10T13:00:00Z",
                    "etaTimestamp": "2026-11-10T07:30:00Z",
                },
            ],
        }
        events, rejected, _ = ingest_portnet.normalize_data([call], RECEIVED_AT)
        self.assertEqual(len(events), 1)
        self.assertEqual(rejected[0]["reason_code"], "AMBIGUOUS_PORT_AREA")


class InjectedPortnetPostgresReliabilityTests(unittest.TestCase):
    """Injected replay and collision checks use an isolated PostgreSQL schema."""

    def setUp(self):
        self.base_url = os.environ.get("PORTPULSE_TEST_DATABASE_URL")
        if not self.base_url:
            self.skipTest("set PORTPULSE_TEST_DATABASE_URL for PostgreSQL checks")
        self.schema_name = f"portpulse_injected_{uuid.uuid4().hex}"
        with psycopg.connect(self.base_url) as connection:
            connection.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema_name))
            )
        self.database_url = make_conninfo(
            self.base_url, options=f"-c search_path={self.schema_name}"
        )
        root = Path(__file__).resolve().parents[1]
        with psycopg.connect(self.database_url) as connection:
            for migration in sorted((root / "migrations").glob("*.sql")):
                connection.execute(migration.read_text(), prepare=False)

    def tearDown(self):
        if getattr(self, "schema_name", None):
            with psycopg.connect(self.base_url) as connection:
                connection.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(self.schema_name)
                    )
                )

    def _event(self):
        call = {
            "portCallId": 905,
            "portAreaDetails": [
                {
                    "portAreaCode": "VUOS",
                    "eta": "2026-11-10T12:00:00Z",
                    "etaTimestamp": "2026-11-10T07:00:00Z",
                }
            ],
        }
        return ingest_portnet.normalize_data([call], RECEIVED_AT)[0][0]

    def test_injected_replay_stores_one_observation_and_logs_repeat(self):
        event = self._event()
        first = ingest_portnet.store_observations(
            [event],
            [],
            self.database_url,
        )
        replay = {
            **event,
            "received_at_utc": "2026-11-10T08:05:00.000000Z",
        }
        second = ingest_portnet.store_observations(
            [replay],
            [],
            self.database_url,
        )
        self.assertEqual(first, (1, 0, 0, 1))
        self.assertEqual(second, (1, 1, 0, 0))
        with psycopg.connect(self.database_url) as connection:
            counts = connection.execute(
                "SELECT (SELECT count(*) FROM portnet_eta_observation), "
                "(SELECT count(*) FROM portnet_eta_quality_issue "
                "WHERE issue_code = 'EXACT_DUPLICATE')"
            ).fetchone()
        self.assertEqual(counts, (1, 1))

    def test_injected_invalid_eta_is_persisted_in_quarantine(self):
        call = {
            "portCallId": 906,
            "portAreaDetails": [
                {
                    "portAreaCode": "VUOS",
                    "eta": "bad-time",
                    "etaTimestamp": "2026-11-10T07:00:00Z",
                }
            ],
        }
        events, rejected, _ = ingest_portnet.normalize_data([call], RECEIVED_AT)
        ingest_portnet.store_observations(events, rejected, self.database_url)
        with psycopg.connect(self.database_url) as connection:
            row = connection.execute(
                "SELECT reason_code FROM portnet_eta_rejected_event"
            ).fetchone()
        self.assertEqual(row[0], "INVALID_PORTNET_ETA")

    def test_injected_changed_payload_reusing_event_id_is_collision(self):
        event = self._event()
        changed = {
            **event,
            "eta_utc": "2026-11-10T12:30:00.000000Z",
            "source_payload": {
                **event["source_payload"],
                "eta_utc": "2026-11-10T12:30:00.000000Z",
            },
        }
        result = ingest_portnet.store_observations(
            [event, changed],
            [],
            self.database_url,
        )
        self.assertEqual(result, (2, 0, 1, 2))
        with psycopg.connect(self.database_url) as connection:
            counts = connection.execute(
                "SELECT (SELECT count(*) FROM portnet_eta_observation), "
                "(SELECT count(*) FROM portnet_eta_quality_issue "
                "WHERE issue_code = 'ID_COLLISION')"
            ).fetchone()
        self.assertEqual(counts, (2, 1))


if __name__ == "__main__":
    unittest.main()
