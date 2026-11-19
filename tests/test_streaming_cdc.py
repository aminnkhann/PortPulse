"""Validation tests for Debezium messages and idempotent CDC storage."""

import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import psycopg
from psycopg.conninfo import conninfo_to_dict

from scripts.streaming_cdc import decode_debezium_message, store_cdc_messages


def message(value, offset=12, partition=1):
    raw = json.dumps(value).encode() if value is not None else None
    return SimpleNamespace(
        value=lambda: raw,
        topic=lambda: "portpulse_stream.stream_demo.port_eta_state",
        partition=lambda: partition,
        offset=lambda: offset,
        error=lambda: None,
    )


def row(
    eta="2026-11-13T15:00:00Z",
    call_id="SIM-001",
    updated="2026-11-13T10:00:00Z",
):
    return {
        "scenario_id": "unit-test",
        "port_call_id": call_id,
        "port_area_code": "KATA" if call_id == "SIM-002" else "VUOS",
        "eta_utc": eta,
        "updated_at_utc": updated,
    }


def acceptance_history_query():
    guide = Path(__file__).resolve().parents[1] / "docs/final_acceptance_checklist.md"
    match = re.search(
        r'"(SELECT [^"\n]+ FROM stream_demo\.port_eta_cdc_event[^"\n]+)"',
        guide.read_text(),
    )
    if not match:
        raise AssertionError("CDC history query is missing from the acceptance guide")
    return match.group(1)


class DebeziumMessageTests(unittest.TestCase):
    def test_create_update_delete_and_snapshot_operations(self):
        cases = (
            ("c", None, row()),
            ("u", row(), row("2026-11-13T15:30:00Z")),
            ("d", row(), None),
            ("r", None, row()),
        )
        for offset, (operation, before, after) in enumerate(cases):
            with self.subTest(operation=operation):
                result = decode_debezium_message(
                    message({"op": operation, "before": before, "after": after}, offset)
                )
                self.assertEqual(result["operation"], operation)
                self.assertEqual(result["port_call_id"], "SIM-001")
                self.assertEqual(result["offset"], offset)

    def test_tombstone_is_ignored(self):
        self.assertIsNone(decode_debezium_message(message(None)))

    def test_rejects_invalid_json(self):
        invalid = SimpleNamespace(
            value=lambda: b"not-json",
            topic=lambda: "topic",
            partition=lambda: 0,
            offset=lambda: 0,
            error=lambda: None,
        )
        with self.assertRaisesRegex(ValueError, "valid UTF-8 JSON"):
            decode_debezium_message(invalid)

    def test_rejects_missing_row_fields_and_unknown_operations(self):
        with self.assertRaisesRegex(ValueError, "missing required fields"):
            decode_debezium_message(
                message({"op": "c", "after": {"port_call_id": "x"}})
            )
        with self.assertRaisesRegex(ValueError, "unsupported Debezium operation"):
            decode_debezium_message(message({"op": "x", "after": row()}))

    def test_accepts_epoch_millisecond_timestamps(self):
        result = decode_debezium_message(
            message({"op": "c", "after": {**row(), "updated_at_utc": 1794564000000}})
        )
        self.assertEqual(
            result["source_updated_at"], datetime(2026, 11, 13, 10, tzinfo=timezone.utc)
        )

    @patch(
        "scripts.streaming_cdc.psycopg.connect", side_effect=psycopg.OperationalError
    )
    def test_database_failures_propagate(self, connect):
        with self.assertRaises(psycopg.OperationalError):
            store_cdc_messages([message({"op": "c", "after": row()})], "unused")
        connect.assert_called_once_with("unused")


class CdcAcceptanceChecklistTests(unittest.TestCase):
    def test_history_query_uses_the_migration_008_column_name(self):
        root = Path(__file__).resolve().parents[1]
        migration = (
            root / "migrations/008_create_streaming_cdc_tables.sql"
        ).read_text()
        query = acceptance_history_query()
        self.assertRegex(migration, r"(?m)^\s*operation TEXT NOT NULL")
        self.assertIn("SELECT operation, count(*)", query)
        self.assertNotIn("cdc_operation", query)


class CdcPostgresReplayTests(unittest.TestCase):
    def test_acceptance_history_query_executes_against_postgres(self):
        database_url = os.environ.get("PORTPULSE_TEST_DATABASE_URL")
        if not database_url:
            self.skipTest("set PORTPULSE_TEST_DATABASE_URL for PostgreSQL checks")
        query = acceptance_history_query().replace(
            "$SCENARIO_ID", f"schema-check-{uuid.uuid4().hex}"
        )
        with psycopg.connect(database_url) as connection:
            rows = connection.execute(query).fetchall()
        self.assertEqual(rows, [])

    def test_ten_replays_keep_one_history_and_expected_dbt_state(self):
        database_url = os.environ.get("PORTPULSE_TEST_DATABASE_URL")
        if not database_url:
            self.skipTest("set PORTPULSE_TEST_DATABASE_URL for PostgreSQL checks")
        root = Path(__file__).resolve().parents[1]
        unique_id = uuid.uuid4().hex
        topic = f"cdc-test-{unique_id}"
        scenario = f"cdc-replay-{unique_id}"
        schema = f"analytics_cdc_{unique_id[:12]}"

        def scenario_row(call_id, eta, updated):
            return {
                **row(eta, call_id, updated),
                "scenario_id": scenario,
            }

        first = scenario_row("SIM-001", "2026-11-13T15:00:00Z", "2026-11-13T10:00:00Z")
        second = scenario_row("SIM-001", "2026-11-13T15:30:00Z", "2026-11-13T10:02:00Z")
        final = scenario_row("SIM-001", "2026-11-13T14:50:00Z", "2026-11-13T10:04:00Z")
        late = scenario_row("SIM-001", "2026-11-13T18:00:00Z", "2026-11-13T10:03:00Z")
        deleted = scenario_row(
            "SIM-002", "2026-11-13T17:00:00Z", "2026-11-13T10:06:00Z"
        )
        records = [
            message({"op": "c", "before": None, "after": first}, 0, 0),
            message({"op": "u", "before": first, "after": second}, 1, 0),
            message({"op": "u", "before": second, "after": final}, 2, 0),
            message({"op": "u", "before": final, "after": late}, 3, 0),
            message({"op": "c", "before": None, "after": deleted}, 0, 1),
            message({"op": "d", "before": deleted, "after": None}, 1, 1),
        ]
        for record in records:
            record.topic = lambda: topic
        try:
            for _ in range(10):
                store_cdc_messages(records, database_url)
            with psycopg.connect(database_url) as connection:
                count = connection.execute(
                    """SELECT count(*) FROM stream_demo.port_eta_cdc_event
                       WHERE scenario_id = %s""",
                    (scenario,),
                ).fetchone()[0]
            self.assertEqual(count, 6)

            settings = conninfo_to_dict(database_url)
            env = os.environ.copy()
            env.update(
                {
                    "DBT_HOST": settings["host"],
                    "DBT_PORT": settings["port"],
                    "DBT_USER": settings["user"],
                    "DBT_PASSWORD": settings["password"],
                    "DBT_DATABASE": settings["dbname"],
                    "DBT_SCHEMA": schema,
                }
            )
            with tempfile.TemporaryDirectory(prefix="portpulse-cdc-dbt-") as temp_dir:
                temp = Path(temp_dir)
                profile = temp / "profiles"
                profile.mkdir()
                shutil.copy(root / "dbt/profiles.example.yml", profile / "profiles.yml")
                built = subprocess.run(
                    [
                        shutil.which("dbt") or "dbt",
                        "build",
                        "--project-dir",
                        str(root / "dbt"),
                        "--profiles-dir",
                        str(profile),
                        "--select",
                        "stg_stream_eta_changes",
                        "mart_stream_latest_eta",
                        "assert_stream_key_single_partition",
                        "--log-path",
                        str(temp / "logs"),
                        "--target-path",
                        str(temp / "target"),
                    ],
                    env=env,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(built.returncode, 0, built.stdout + built.stderr)
            with psycopg.connect(database_url) as connection:
                latest = connection.execute(
                    f"""SELECT port_call_id, eta_utc
                        FROM {schema}.mart_stream_latest_eta
                        WHERE scenario_id = %s ORDER BY port_call_id""",
                    (scenario,),
                ).fetchall()
            self.assertEqual(
                latest,
                [("SIM-001", datetime(2026, 11, 13, 14, 50, tzinfo=timezone.utc))],
            )
        finally:
            with psycopg.connect(database_url) as connection:
                connection.execute(
                    "DELETE FROM stream_demo.port_eta_cdc_event WHERE scenario_id = %s",
                    (scenario,),
                )
                connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


if __name__ == "__main__":
    unittest.main()
