"""End-to-end Portnet ingestion replay in a disposable database."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import uuid
from contextlib import redirect_stdout
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from scripts import ingest_portnet
from tests.portnet_api_fixtures import generate_portnet_response
from tests.postgres import apply_migrations


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_FIRST_RUN = {
    "portnet_call_records_received": 6,
    "rejected_portnet_call_records": 1,
    "rejected_portnet_eta_details": 1,
    "completed_portnet_calls_skipped": 0,
    "portnet_calls_without_eta_skipped": 1,
    "active_portnet_calls": 4,
    "accepted_portnet_observations": 7,
    "rejected_observations": 2,
    "rejected_by_reason": {
        "INVALID_PORTNET_CALL": 1,
        "INVALID_PORTNET_ETA": 1,
    },
    "duplicate_port_call_records_in_response": 0,
    "unique_normalized_observations_in_response": 6,
    "duplicate_normalized_event_attempts_in_response": 1,
    "unchanged_observations_on_repeat_poll": 0,
    "id_collision_groups": 1,
    "new_observation_rows_inserted": 6,
}
EXPECTED_LATEST = [
    (
        "portnet:1001",
        "VUOS",
        "portnet:1001:VUOS:2026-11-10T08:00:00.000000Z",
        datetime(2026, 11, 10, 13, 0, tzinfo=timezone.utc),
    ),
    (
        "portnet:2002",
        "KATA",
        "portnet:2002:KATA:2026-11-10T07:30:00.000000Z",
        datetime(2026, 11, 10, 13, 30, tzinfo=timezone.utc),
    ),
    (
        "portnet:3003",
        "VUOS",
        "portnet:3003:VUOS:2026-11-10T08:00:00.000000Z",
        datetime(2026, 11, 10, 15, 0, tzinfo=timezone.utc),
    ),
]
EXPECTED_QUALITY = (6, 20, 64, 10, 3)


def replay_portnet_api_response(database_url):
    fixed_now = datetime(2026, 11, 10, 10, 0, tzinfo=timezone.utc)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now if tz else fixed_now.replace(tzinfo=None)

    results = []
    with (
        patch.object(
            ingest_portnet,
            "fetch_json",
            side_effect=lambda _: generate_portnet_response(),
        ),
        patch.object(ingest_portnet, "datetime", FixedDateTime),
        patch.object(
            sys,
            "argv",
            [
                "ingest_portnet",
                "--date",
                "2026-11-10",
                "--database-url",
                database_url,
            ],
        ),
    ):
        for _ in range(10):
            output = StringIO()
            with redirect_stdout(output):
                ingest_portnet.main()
            results.append(json.loads(output.getvalue()))
    return results


def build_portnet_marts(database_url):
    connection_settings = conninfo_to_dict(database_url)
    env = os.environ.copy()
    env.update(
        {
            "DBT_HOST": connection_settings["host"],
            "DBT_PORT": connection_settings["port"],
            "DBT_USER": connection_settings["user"],
            "DBT_PASSWORD": connection_settings["password"],
            "DBT_DATABASE": connection_settings["dbname"],
            "DBT_SCHEMA": "analytics",
        }
    )
    with tempfile.TemporaryDirectory(prefix="portpulse-dbt-run-") as temp_dir:
        temp = Path(temp_dir)
        profiles = temp / "profiles"
        profiles.mkdir()
        shutil.copy(ROOT / "dbt/profiles.example.yml", profiles / "profiles.yml")
        return subprocess.run(
            [
                shutil.which("dbt") or "dbt",
                "build",
                "--project-dir",
                str(ROOT / "dbt"),
                "--profiles-dir",
                str(profiles),
                "--log-path",
                str(temp / "logs"),
                "--target-path",
                str(temp / "target"),
            ],
            env=env,
            capture_output=True,
            text=True,
        )


def read_portnet_marts(database_url):
    with psycopg.connect(database_url) as connection:
        latest = connection.execute(
            "SELECT port_call_id, port_area_code, event_id, eta_utc "
            "FROM analytics.mart_portnet_latest_eta "
            "ORDER BY port_call_id, port_area_code"
        ).fetchall()
        quality = connection.execute(
            "SELECT accepted_observations, rejected_observations, "
            "duplicate_issue_rows, collision_issue_rows, latest_eta_records "
            "FROM analytics.mart_pipeline_quality"
        ).fetchone()
        reasons = connection.execute(
            "SELECT reason_code, count(*) FROM portnet_eta_rejected_event "
            "GROUP BY reason_code ORDER BY reason_code"
        ).fetchall()
        stored_observations = connection.execute(
            "SELECT count(*) FROM portnet_eta_observation"
        ).fetchone()[0]
    return latest, quality, reasons, stored_observations


class PortnetApiReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.base_url = os.environ.get("PORTPULSE_TEST_DATABASE_URL")
        if not self.base_url:
            self.skipTest("set PORTPULSE_TEST_DATABASE_URL for PostgreSQL checks")
        self.database_name = f"portpulse_reliability_{uuid.uuid4().hex}"
        with psycopg.connect(self.base_url, autocommit=True) as connection:
            connection.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.database_name))
            )
        self.database_url = make_conninfo(self.base_url, dbname=self.database_name)
        try:
            apply_migrations(self.database_url)
        except Exception:
            self._drop_database()
            raise

    def tearDown(self):
        if getattr(self, "database_name", None):
            self._drop_database()

    def _drop_database(self):
        with psycopg.connect(self.base_url, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(self.database_name)
                )
            )

    def test_mocked_api_ingestion_replay_and_dbt_marts(self):
        results = replay_portnet_api_response(self.database_url)
        self.assertEqual(
            results[0],
            {
                **EXPECTED_FIRST_RUN,
                "record_origin": "live_api",
                "source": "Portnet V1",
                "date": "2026-11-10",
                "port": "ALL",
            },
        )
        self.assertEqual(len(results), 10)
        for replay in results[1:]:
            self.assertEqual(replay["new_observation_rows_inserted"], 0)
            self.assertEqual(replay["unique_normalized_observations_in_response"], 6)
            self.assertEqual(
                replay["duplicate_normalized_event_attempts_in_response"], 1
            )
            self.assertEqual(replay["unchanged_observations_on_repeat_poll"], 6)
            self.assertEqual(replay["id_collision_groups"], 1)

        built = build_portnet_marts(self.database_url)
        self.assertEqual(built.returncode, 0, built.stdout + built.stderr)
        latest, quality, reasons, stored_observations = read_portnet_marts(
            self.database_url
        )
        self.assertEqual(latest, EXPECTED_LATEST)
        self.assertEqual(quality, EXPECTED_QUALITY)
        self.assertEqual(
            reasons, [("INVALID_PORTNET_CALL", 10), ("INVALID_PORTNET_ETA", 10)]
        )
        self.assertEqual(stored_observations, 6)


if __name__ == "__main__":
    unittest.main()
