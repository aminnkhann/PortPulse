"""Focused checks for matching Portnet and AIS ETA updates."""

import json
import unittest
from datetime import datetime, timezone

from scripts import ingest_marine_etas
from scripts.event_model import MARINE_ETA_SCHEMA, build_marine_eta_json_schema
from scripts.reconcile import load_rules, reconcile_port_call


class MarineEtaIngestionTests(unittest.TestCase):
    RECEIVED_AT = "2026-11-06T11:40:00Z"

    def test_schema_matches_marine_eta_model(self):
        self.assertEqual(
            build_marine_eta_json_schema(),
            json.loads(MARINE_ETA_SCHEMA.read_text(encoding="utf-8")),
        )

    def test_decodes_ais_eta(self):
        eta = ingest_marine_etas.decode_ais_eta(733485, 2026)
        self.assertEqual(eta, datetime(2026, 11, 6, 4, 45, tzinfo=timezone.utc))
        self.assertIsNone(ingest_marine_etas.decode_ais_eta(0, 2026))

    def test_maps_unique_same_vessel_port_and_eta_date(self):
        calls = [{
            "portCallId": 3388488,
            "portCallTimestamp": "2026-11-06T06:00:03.000Z",
            "portToVisit": "FIHEL",
            "mmsi": 230694000,
            "portAreaDetails": [{
                "portAreaCode": "VUOS",
                "eta": "2026-11-06T04:45:00.000Z",
                "etaSource": "Agent",
                "etaTimestamp": "2026-10-30T11:40:07.000Z",
            }],
        }]
        ais = [{
            "mmsi": 230694000,
            "destination": "FIHEL",
            "timestamp": 1793955567191,
            "eta": 733485,
        }]

        events, rejected, counts = ingest_marine_etas.normalize_data(
            calls, ais, self.RECEIVED_AT,
        )

        self.assertEqual(rejected, [])
        self.assertEqual(counts, {
            "matched_ais": 1,
            "unmatched_ais": 0,
            "skipped_portnet": 0,
            "completed_portnet": 0,
            "unmatchable_portnet": 0,
            "missing_portnet_eta_timestamp": 0,
        })
        self.assertEqual({event["source_id"] for event in events}, {"portnet", "ais"})
        self.assertEqual(
            {event["port_call_id"] for event in events}, {"maritime:3388488"},
        )
        etas = {event["source_id"]: event["eta_utc"] for event in events}
        self.assertEqual(etas["portnet"], "2026-11-06T04:45:00.000Z")
        self.assertEqual(etas["ais"], "2026-11-06T04:45:00Z")

    def test_portnet_wins_a_source_conflict_by_configured_priority(self):
        call = {
            "portCallId": 3388488,
            "portCallTimestamp": "2026-11-06T06:00:03.000Z",
            "portToVisit": "FIHEL",
            "mmsi": 230694000,
            "portAreaDetails": [{
                "portAreaCode": "VUOS",
                "eta": "2026-11-06T04:45:00.000Z",
                "etaSource": "Agent",
                "etaTimestamp": "2026-10-30T11:40:07.000Z",
            }],
        }
        ais = [{
            "mmsi": 230694000,
            "destination": "FIHEL",
            "timestamp": 1793955567191,
            "eta": 733549,
        }]
        events, rejected, _ = ingest_marine_etas.normalize_data(
            [call], ais, self.RECEIVED_AT,
        )
        rules = load_rules()
        rules["as_of"] = datetime.fromisoformat(self.RECEIVED_AT.replace("Z", "+00:00"))

        result = reconcile_port_call(
            events, rejected, rules["sources"], rules, "maritime:3388488",
        )

        self.assertEqual(result["trusted_state"]["source_id"], "portnet")
        self.assertEqual(result["trusted_state"]["eta_utc"], "2026-11-06T04:45:00Z")
        self.assertTrue(any(issue["code"] == "CONFLICT" for issue in result["issues"]))

    def test_skips_ambiguous_port_call_match(self):
        call = {
            "portCallId": 1,
            "portCallTimestamp": "2026-11-06T06:00:03.000Z",
            "portToVisit": "FIHEL",
            "mmsi": 230694000,
            "portAreaDetails": [{
                "portAreaCode": "VUOS",
                "eta": "2026-11-06T04:45:00.000Z",
                "etaSource": "Agent",
                "etaTimestamp": "2026-11-06T03:00:00.000Z",
            }],
        }
        second = {**call, "portCallId": 2}
        ais = [{
            "mmsi": 230694000,
            "destination": "FIHEL",
            "timestamp": 1793955567191,
            "eta": 733485,
        }]

        events, rejected, counts = ingest_marine_etas.normalize_data(
            [call, second], ais, self.RECEIVED_AT,
        )

        self.assertEqual(
            len([event for event in events if event["source_id"] == "ais"]), 0,
        )
        self.assertEqual(counts["unmatched_ais"], 1)
        self.assertEqual(rejected, [])

    def test_does_not_compare_eta_after_arrival(self):
        call = {
            "portCallId": 1,
            "portCallTimestamp": "2026-11-06T06:00:03.000Z",
            "portToVisit": "FIHEL",
            "mmsi": 230694000,
            "portAreaDetails": [{
                "portAreaCode": "VUOS",
                "eta": "2026-11-06T04:45:00.000Z",
                "etaSource": "Agent",
                "etaTimestamp": "2026-11-06T03:00:00.000Z",
                "ata": "2026-11-06T05:00:00.000Z",
            }],
        }
        ais = [{
            "mmsi": 230694000,
            "destination": "FIHEL",
            "timestamp": 1793955567191,
            "eta": 733485,
        }]

        events, rejected, counts = ingest_marine_etas.normalize_data(
            [call], ais, self.RECEIVED_AT,
        )

        self.assertEqual(events, [])
        self.assertEqual(rejected, [])
        self.assertEqual(counts["completed_portnet"], 1)

    def test_valid_portnet_record_without_mmsi_is_skipped_not_rejected(self):
        call = {
            "portCallId": 1,
            "mmsi": 0,
            "portToVisit": "FIHEL",
            "portAreaDetails": [{
                "portAreaCode": None,
                "eta": "2026-11-06T04:45:00.000Z",
                "etaSource": "Agent",
                "etaTimestamp": "2026-11-06T03:00:00.000Z",
            }],
        }

        events, rejected, counts = ingest_marine_etas.normalize_data(
            [call], [], self.RECEIVED_AT,
        )

        self.assertEqual(events, [])
        self.assertEqual(rejected, [])
        self.assertEqual(counts["unmatchable_portnet"], 1)

    def test_malformed_portnet_record_is_rejected_with_reason(self):
        events, rejected, _ = ingest_marine_etas.normalize_data(
            [{"mmsi": 230694000, "portToVisit": "FIHEL"}],
            [],
            self.RECEIVED_AT,
        )

        self.assertEqual(events, [])
        self.assertEqual(rejected[0]["reason_code"], "INVALID_PORTNET_EVENT")
        self.assertIn("portCallId", rejected[0]["details"]["message"])


if __name__ == "__main__":
    unittest.main()
