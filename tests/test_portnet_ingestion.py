"""Portnet V1 normalization and persistence checks."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from time import monotonic, sleep
import unittest
from datetime import datetime
from unittest.mock import patch

import psycopg
from psycopg.conninfo import make_conninfo

from scripts import ingest_portnet
from scripts.event_storage import PORTNET_INGESTION_LOCK
from tests.postgres import apply_migrations, create_test_schema


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
        self.database_url = create_test_schema(self, "portpulse_portnet")
        apply_migrations(self.database_url)

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

    def _portnet_event(self):
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

    def test_replaying_observation_records_duplicate_without_adding_a_row(self):
        event = self._portnet_event()
        first = ingest_portnet.store_observations([event], [], self.database_url)
        replay = {**event, "received_at_utc": "2026-11-10T08:05:00.000000Z"}
        second = ingest_portnet.store_observations([replay], [], self.database_url)
        third = ingest_portnet.store_observations(
            [{**event, "received_at_utc": "2026-11-10T08:06:00.000000Z"}],
            [],
            self.database_url,
        )
        self.assertEqual(first, (1, 0, 0, 1))
        self.assertEqual(second, (1, 1, 0, 0))
        self.assertEqual(third, (1, 1, 0, 0))
        with psycopg.connect(self.database_url) as connection:
            counts = connection.execute(
                "SELECT (SELECT count(*) FROM portnet_eta_observation), "
                "(SELECT count(*) FROM portnet_eta_quality_issue "
                "WHERE issue_code = 'EXACT_DUPLICATE')"
            ).fetchone()
        self.assertEqual(counts, (1, 1))

    def test_concurrent_replays_keep_one_rejection_and_record_one_duplicate(self):
        event = self._portnet_event()
        invalid_call = {
            "portCallId": 907,
            "portAreaDetails": [
                {
                    "portAreaCode": "VUOS",
                    "eta": "bad-time",
                    "etaTimestamp": "2026-11-10T07:00:00Z",
                }
            ],
        }
        _, rejected, _ = ingest_portnet.normalize_data([invalid_call], RECEIVED_AT)
        start_together = Barrier(3)
        original_lock = ingest_portnet.lock_ingestion

        def simultaneous_lock(cursor, lock_name):
            start_together.wait(timeout=10)
            return original_lock(cursor, lock_name)

        concurrent_url = make_conninfo(
            self.database_url, application_name="portpulse-ingestion-concurrency-test"
        )
        with psycopg.connect(self.database_url) as blocker:
            blocker.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (PORTNET_INGESTION_LOCK,),
            )
            with patch.object(
                ingest_portnet, "lock_ingestion", side_effect=simultaneous_lock
            ):
                with ThreadPoolExecutor(max_workers=2) as pool:
                    futures = [
                        pool.submit(
                            ingest_portnet.store_observations,
                            [event],
                            rejected,
                            concurrent_url,
                        )
                        for _ in range(2)
                    ]
                    start_together.wait(timeout=10)
                    deadline = monotonic() + 5
                    waiting = 0
                    try:
                        while monotonic() < deadline:
                            waiting = blocker.execute(
                                """SELECT count(*) FROM pg_locks l
                                   JOIN pg_stat_activity a ON a.pid = l.pid
                                   WHERE l.locktype = 'advisory' AND NOT l.granted
                                     AND a.application_name = %s""",
                                ("portpulse-ingestion-concurrency-test",),
                            ).fetchone()[0]
                            if waiting == 2:
                                break
                            sleep(0.01)
                        self.assertEqual(waiting, 2)
                    finally:
                        blocker.commit()
                    results = [future.result(timeout=15) for future in futures]

        with psycopg.connect(self.database_url) as connection:
            counts = connection.execute(
                "SELECT (SELECT count(*) FROM portnet_eta_observation "
                "WHERE event_id = %s), "
                "(SELECT count(*) FROM portnet_eta_rejected_event "
                "WHERE port_call_id = 'portnet:907'), "
                "(SELECT count(*) FROM portnet_eta_quality_issue "
                "WHERE issue_code = 'EXACT_DUPLICATE' AND event_id = %s)",
                (event["event_id"], event["event_id"]),
            ).fetchone()
        self.assertEqual(counts, (1, 1, 1))
        self.assertEqual(sum(result[1] for result in results), 1)
        self.assertEqual(sum(result[3] for result in results), 1)

    def test_invalid_eta_is_persisted_in_quarantine(self):
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
        replay = [
            {**item, "received_at_utc": "2026-11-10T08:06:00.000000Z"}
            for item in rejected
        ]
        ingest_portnet.store_observations([], replay, self.database_url)
        with psycopg.connect(self.database_url) as connection:
            reasons = connection.execute(
                "SELECT reason_code, count(*) "
                "FROM portnet_eta_rejected_event GROUP BY reason_code"
            ).fetchall()
        self.assertEqual(reasons, [("INVALID_PORTNET_ETA", 1)])

    def test_changed_payload_with_existing_event_id_is_collision(self):
        event = self._portnet_event()
        changed = {
            **event,
            "eta_utc": "2026-11-10T12:30:00.000000Z",
            "source_payload": {
                **event["source_payload"],
                "eta_utc": "2026-11-10T12:30:00.000000Z",
            },
        }
        result = ingest_portnet.store_observations(
            [event, changed], [], self.database_url
        )
        self.assertEqual(result, (2, 0, 1, 2))
        with psycopg.connect(self.database_url) as connection:
            counts = connection.execute(
                "SELECT (SELECT count(*) FROM portnet_eta_observation), "
                "(SELECT count(*) FROM portnet_eta_quality_issue "
                "WHERE issue_code = 'ID_COLLISION')"
            ).fetchone()
        self.assertEqual(counts, (2, 1))
