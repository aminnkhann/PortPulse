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

from scripts import ingest_digitraffic, ingest_fixture
from scripts.query_state import query_port_call
from scripts.reconcile import load_rules, reconcile_database, reconcile_port_call


ROOT = Path(__file__).resolve().parents[1]


def tests_visit():
    return {
        "visitId": "visit-123",
        "updateTime": "2026-01-15T10:50:00Z",
        "eta": "2026-01-15T14:00:00Z",
        "portOfCall": "FIHEL",
        "status": "Expected to Arrive",
    }


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

    def test_digitraffic_visit_wins_in_its_separate_dataset(self):
        events, rejected = ingest_digitraffic.normalize_visits(
            [tests_visit()], "2026-01-15T10:55:00Z"
        )
        result = reconcile_port_call(
            events, rejected, self.rules["sources"], self.rules,
            "digitraffic:visit-123",
        )
        self.assertEqual(result["trusted_state"]["source_id"], "digitraffic")
        self.assertEqual(
            result["trusted_state"]["event_id"],
            "digitraffic:visit-123:2026-01-15T10:50:00Z",
        )

    def test_digitraffic_collision_excludes_both_revision_variants(self):
        visit = tests_visit()
        changed = {**visit, "eta": "2026-01-15T15:00:00Z"}
        events, rejected = ingest_digitraffic.normalize_visits(
            [visit, changed], "2026-01-15T10:55:00Z"
        )
        result = reconcile_port_call(
            events, rejected, self.rules["sources"], self.rules,
            "digitraffic:visit-123",
        )
        self.assertIsNone(result["trusted_state"]["event_id"])
        self.assertEqual(
            [issue["code"] for issue in result["issues"]], ["ID_COLLISION"]
        )

    def test_arrival_status_prevents_previous_eta_from_remaining_trusted(self):
        visit = tests_visit()
        arrived = {
            **visit,
            "updateTime": "2026-01-15T10:59:00Z",
            "status": "Arrived",
        }
        events, rejected = ingest_digitraffic.normalize_visits(
            [visit, arrived], "2026-01-15T10:59:30Z"
        )
        result = reconcile_port_call(
            events, rejected, self.rules["sources"], self.rules,
            "digitraffic:visit-123",
        )
        self.assertEqual(result["trusted_state"]["status"], "UNAVAILABLE")
        self.assertIsNone(result["trusted_state"]["event_id"])
        self.assertEqual(result["source_states"][0]["status"], "INELIGIBLE")


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
        self.assertEqual(issue_counts, {
            "CONFLICT": 1,
            "DELAYED_DELIVERY": 3,
            "EXACT_DUPLICATE": 2,
            "ID_COLLISION": 1,
            "OUT_OF_ORDER": 2,
            "STALE_SOURCE": 1,
        })

    def test_digitraffic_collision_from_later_poll_has_no_winner(self):
        visit = tests_visit()
        first, _ = ingest_digitraffic.normalize_visits(
            [visit], "2026-01-15T10:55:00Z"
        )
        changed = {**visit, "eta": "2026-01-15T15:00:00Z"}
        second, _ = ingest_digitraffic.normalize_visits(
            [changed], "2026-01-15T10:56:00Z"
        )
        ingest_fixture.store_fixture(
            first, [], record_existing_duplicates=True,
            database_url=self.database_url,
        )
        ingest_fixture.store_fixture(
            second, [], record_existing_duplicates=True,
            database_url=self.database_url,
        )

        result = next(
            item for item in reconcile_database(
                self.database_url, as_of_utc="2026-01-15T11:00:00Z"
            )
            if item["port_call_id"] == "digitraffic:visit-123"
        )
        self.assertIsNone(result["trusted_state"]["event_id"])
        self.assertIn("ID_COLLISION", [issue["code"] for issue in result["issues"]])

    def test_arrived_visit_replaces_previous_trusted_eta_in_database(self):
        visit = tests_visit()
        expected, _ = ingest_digitraffic.normalize_visits(
            [visit], "2026-01-15T10:50:00Z"
        )
        arrived = {
            **visit,
            "updateTime": "2026-01-15T10:59:00Z",
            "status": "Arrived",
        }
        arrival_event, _ = ingest_digitraffic.normalize_visits(
            [arrived], "2026-01-15T10:59:30Z"
        )
        ingest_fixture.store_fixture(
            expected + arrival_event, [], database_url=self.database_url
        )
        result = next(
            item for item in reconcile_database(
                self.database_url, as_of_utc="2026-01-15T11:00:00Z"
            )
            if item["port_call_id"] == "digitraffic:visit-123"
        )
        with psycopg.connect(self.database_url) as connection:
            saved_status = connection.execute(
                "SELECT status FROM port_call_state "
                "WHERE port_call_id = 'digitraffic:visit-123'"
            ).fetchone()[0]
        self.assertEqual(result["trusted_state"]["status"], "UNAVAILABLE")
        self.assertEqual(saved_status, "UNAVAILABLE")


if __name__ == "__main__":
    unittest.main()
