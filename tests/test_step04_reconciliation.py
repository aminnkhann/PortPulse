"""Checks for deterministic trusted-state reconciliation."""

import json
import os
import unittest
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import uuid
from unittest.mock import patch

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from scripts import ingest_fixture
from scripts.query_state import query_port_call
from scripts.reconcile import (
    latest_event,
    load_rules,
    reconcile_database,
    reconcile_port_call,
)


ROOT = Path(__file__).resolve().parents[1]


class ReconciliationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.valid, cls.rejected = ingest_fixture.read_fixture()
        cls.rules = load_rules()
        cls.expected = json.loads(
            (ROOT / "fixtures/expected_output.json").read_text(encoding="utf-8")
        )

    def reconcile(self, events=None, rules=None):
        return reconcile_port_call(
            self.valid if events is None else events,
            self.rejected,
            self.rules["sources"],
            self.rules if rules is None else rules,
            "PC-DEMO-001",
        )

    def test_result_matches_the_golden_fixture(self):
        self.assertEqual(self.reconcile(), self.expected)

    def test_input_order_does_not_change_the_result(self):
        self.assertEqual(self.reconcile(self.valid[::-1]), self.expected)

    def test_state_is_unavailable_when_every_source_is_stale(self):
        rules = deepcopy(self.rules)
        rules["as_of"] = rules["as_of"].replace(hour=14)
        result = self.reconcile(rules=rules)
        self.assertEqual(result["trusted_state"]["status"], "UNAVAILABLE_STALE")
        self.assertIsNone(result["trusted_state"]["event_id"])
        self.assertTrue(
            all(state["status"] == "STALE" for state in result["source_states"])
        )

    def test_event_received_after_evaluation_cutoff_is_excluded(self):
        future = {
            **self.valid[0],
            "event_id": "CARRIER-FUTURE-01",
            "event_time_utc": "2026-01-15T11:04:00Z",
            "received_at_utc": "2026-01-15T11:05:00Z",
        }
        result = self.reconcile(self.valid + [future])
        self.assertEqual(result["trusted_state"]["event_id"], "COORD-DELAYED-01")
        self.assertEqual(result["counts"]["events_after_as_of_utc"], 1)

    def test_latest_event_breaks_exact_timestamp_ties_by_smallest_id(self):
        tied = [
            {
                "event_id": event_id,
                "event_time_utc": "2026-01-15T10:00:00Z",
                "received_at_utc": "2026-01-15T10:01:00Z",
            }
            for event_id in ("EVENT-Z", "EVENT-A", "EVENT-M")
        ]
        self.assertEqual(latest_event(tied)["event_id"], "EVENT-A")


class ReconciliationDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.base_url = os.environ.get("PORTPULSE_TEST_DATABASE_URL")
        if not self.base_url:
            self.skipTest("set PORTPULSE_TEST_DATABASE_URL to run PostgreSQL checks")

        self.schema_name = f"portpulse_reconcile_{uuid.uuid4().hex}"
        with psycopg.connect(self.base_url) as connection:
            connection.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema_name))
            )
        self.database_url = make_conninfo(
            self.base_url, options=f"-c search_path={self.schema_name}"
        )
        with psycopg.connect(self.database_url) as connection:
            for migration in sorted((ROOT / "migrations").glob("*.sql")):
                connection.execute(migration.read_text(), prepare=False)
        valid, rejected = ingest_fixture.read_fixture()
        with patch.dict(os.environ, {"DATABASE_URL": self.database_url}):
            ingest_fixture.store_fixture(valid, rejected)

    def tearDown(self):
        if getattr(self, "schema_name", None):
            with psycopg.connect(self.base_url) as connection:
                connection.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(self.schema_name)
                    )
                )

    def test_reconciliation_is_persisted_and_queryable_on_replay(self):
        first = reconcile_database(self.database_url)
        second = reconcile_database(self.database_url)
        report = query_port_call(self.database_url, "PC-DEMO-001")

        self.assertEqual(first, second)
        self.assertEqual(first[0]["trusted_state"]["event_id"], "COORD-DELAYED-01")
        self.assertEqual(report["state"]["winning_event_id"], "COORD-DELAYED-01")
        self.assertEqual(
            report["state"]["trusted_eta_utc"],
            datetime(2026, 1, 15, 15, 40, tzinfo=timezone.utc),
        )
        self.assertEqual(len(report["event_history"]), 28)
        self.assertEqual(len(report["rejected_events"]), 1)
        issue_counts = {}
        for issue in report["quality_issues"]:
            issue_counts[issue["issue_code"]] = (
                issue_counts.get(issue["issue_code"], 0) + 1
            )
        self.assertEqual(
            issue_counts,
            {
                "CONFLICT": 1,
                "DELAYED_DELIVERY": 3,
                "EXACT_DUPLICATE": 2,
                "ID_COLLISION": 1,
                "OUT_OF_ORDER": 2,
                "STALE_SOURCE": 1,
            },
        )

    def test_migration_009_keeps_source_rows_referenced_by_history_or_state(self):
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
            connection.execute(
                (ROOT / "migrations/009_remove_unused_sources.sql").read_text(),
                prepare=False,
            )
            remaining = connection.execute(
                "SELECT source_id FROM source_system WHERE source_id IN "
                "('digitraffic', 'portnet', 'ais') ORDER BY source_id"
            ).fetchall()

        self.assertEqual(remaining, [("ais",), ("digitraffic",)])


if __name__ == "__main__":
    unittest.main()
