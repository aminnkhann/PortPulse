"""Portnet V1 normalization and persistence checks."""

import json
import os
import unittest
import uuid
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from scripts import ingest_portnet
from scripts.event_model import build_marine_eta_json_schema


ROOT = Path(__file__).resolve().parents[1]
RECEIVED_AT = "2026-11-10T08:00:00.000000Z"


def port_call():
    return {
        "portCallId": 123,
        "mmsi": 230123456,
        "portToVisit": "FIHEL",
        "portAreaDetails": [
            {
                "portAreaCode": "VUOS",
                "eta": "2026-11-10T12:00:00Z",
                "etaTimestamp": "2026-11-10T07:00:00Z",
                "etaSource": "PORT",
            },
            {
                "portAreaCode": "KATA",
                "eta": "2026-11-10T13:00:00Z",
                "etaTimestamp": "2026-11-10T07:30:00Z",
                "etaSource": "PORT",
            },
        ],
    }


class PortnetNormalizationTests(unittest.TestCase):
    def test_portnet_event_is_valid_under_shared_marine_contract(self):
        self.assertEqual(
            json.loads(
                (ROOT / "contracts/marine_eta_event_v1.schema.json").read_text()
            ),
            build_marine_eta_json_schema(),
        )
        events, _, _ = ingest_portnet.normalize_data([port_call()], RECEIVED_AT)
        self.assertEqual({event["source_id"] for event in events}, {"portnet"})

    def test_fetch_requests_only_portnet_v1(self):
        with patch.object(
            ingest_portnet, "fetch_json", return_value={"portCalls": []}
        ) as fetch:
            self.assertEqual(
                ingest_portnet.fetch_port_calls("FIHEL", datetime(2026, 11, 10).date()),
                [],
            )
        self.assertIn(ingest_portnet.PORTNET_URL, fetch.call_args.args[0])
        self.assertIn("locode=FIHEL", fetch.call_args.args[0])

    def test_portnet_observations_are_independent_and_have_separate_times(self):
        events, rejected, metrics = ingest_portnet.normalize_data(
            [port_call()],
            RECEIVED_AT,
        )
        self.assertEqual(rejected, [])
        self.assertEqual(metrics["accepted_portnet_observations"], 2)
        self.assertEqual({event["source_id"] for event in events}, {"portnet"})
        self.assertEqual({event["port_call_id"] for event in events}, {"portnet:123"})
        self.assertEqual(
            {event["event_id"] for event in events},
            {
                "portnet:123:VUOS:2026-11-10T07:00:00.000000Z",
                "portnet:123:KATA:2026-11-10T07:30:00.000000Z",
            },
        )
        self.assertEqual(events[0]["event_time_utc"], "2026-11-10T07:00:00.000000Z")
        self.assertEqual(events[0]["received_at_utc"], RECEIVED_AT)
        self.assertEqual(events[0]["eta_utc"], "2026-11-10T12:00:00.000000Z")

    def test_revisions_in_one_area_have_distinct_event_ids(self):
        call = port_call()
        call["portAreaDetails"] = [
            call["portAreaDetails"][0],
            {
                **call["portAreaDetails"][0],
                "eta": "2026-11-10T12:30:00Z",
                "etaTimestamp": "2026-11-10T07:15:00Z",
            },
        ]
        events, rejected, _ = ingest_portnet.normalize_data([call], RECEIVED_AT)
        self.assertEqual(rejected, [])
        self.assertEqual(len(events), 2)
        self.assertNotEqual(events[0]["event_id"], events[1]["event_id"])

    def test_no_eta_and_completed_calls_have_separate_counts(self):
        calls = [
            {"portCallId": 1, "portAreaDetails": []},
            {
                "portCallId": 2,
                "portAreaDetails": [
                    {
                        "portAreaCode": "VUOS",
                        "eta": "2026-11-10T12:00:00Z",
                        "etaTimestamp": "2026-11-10T07:00:00Z",
                        "ata": "2026-11-10T11:00:00Z",
                    }
                ],
            },
        ]
        events, rejected, metrics = ingest_portnet.normalize_data(calls, RECEIVED_AT)
        self.assertEqual((events, rejected), ([], []))
        self.assertEqual(metrics["portnet_calls_without_eta_skipped"], 1)
        self.assertEqual(metrics["completed_portnet_calls_skipped"], 1)


class PortnetPostgresTests(unittest.TestCase):
    def setUp(self):
        self.base_url = os.environ.get("PORTPULSE_TEST_DATABASE_URL")
        if not self.base_url:
            self.skipTest("set PORTPULSE_TEST_DATABASE_URL for PostgreSQL checks")
        self.schema_name = f"portpulse_portnet_{uuid.uuid4().hex}"
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

    def tearDown(self):
        if getattr(self, "schema_name", None):
            with psycopg.connect(self.base_url) as connection:
                connection.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(self.schema_name)
                    )
                )

    def test_load_preserves_provider_collection_and_database_times(self):
        events, rejected, _ = ingest_portnet.normalize_data([port_call()], RECEIVED_AT)
        result = ingest_portnet.store_observations(events, rejected, self.database_url)
        self.assertEqual(result, (2, 0, 0, 2))
        with psycopg.connect(self.database_url) as connection:
            row = connection.execute(
                "SELECT event_time_utc, received_at_utc, ingested_at_utc "
                "FROM portnet_eta_observation WHERE port_area_code = 'VUOS'"
            ).fetchone()
        self.assertEqual(row[0], datetime.fromisoformat("2026-11-10T07:00:00+00:00"))
        self.assertEqual(row[1], datetime.fromisoformat("2026-11-10T08:00:00+00:00"))
        self.assertIsNotNone(row[2])

    def test_migration_007_upgrades_schema_after_006_without_losing_rows(self):
        upgrade_schema = f"portpulse_upgrade_{uuid.uuid4().hex}"
        try:
            with psycopg.connect(self.base_url) as connection:
                connection.execute(
                    sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(upgrade_schema))
                )
            upgrade_url = make_conninfo(
                self.base_url, options=f"-c search_path={upgrade_schema}"
            )
            with psycopg.connect(upgrade_url) as connection:
                for migration in sorted((ROOT / "migrations").glob("*.sql")):
                    if migration.name < "007_create_portnet_eta_tables.sql":
                        connection.execute(migration.read_text(), prepare=False)
                connection.execute(
                    "INSERT INTO port_call (port_call_id) VALUES ('existing-call')"
                )
                connection.execute(
                    (ROOT / "migrations/007_create_portnet_eta_tables.sql").read_text(),
                    prepare=False,
                )
                existing_call = connection.execute(
                    "SELECT port_call_id FROM port_call"
                ).fetchone()[0]
                self.assertEqual(existing_call, "existing-call")
                self.assertIsNotNone(
                    connection.execute(
                        "SELECT to_regclass('portnet_eta_observation')"
                    ).fetchone()[0]
                )
        finally:
            with psycopg.connect(self.base_url) as connection:
                connection.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(upgrade_schema)
                    )
                )
