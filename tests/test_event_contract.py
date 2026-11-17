"""Tests for generated event schemas and their validated models."""

import json
import unittest
from pathlib import Path

from scripts import ingest_portnet
from scripts.event_model import build_json_schema, build_marine_eta_json_schema


ROOT = Path(__file__).resolve().parents[1]


class EventContractTests(unittest.TestCase):
    def test_port_call_schema_matches_the_pydantic_model(self):
        contract = json.loads(
            (ROOT / "contracts/port_call_event_v1.schema.json").read_text()
        )
        self.assertEqual(contract, build_json_schema())

    def test_marine_eta_schema_matches_the_pydantic_model(self):
        contract = json.loads(
            (ROOT / "contracts/marine_eta_event_v1.schema.json").read_text()
        )
        self.assertEqual(contract, build_marine_eta_json_schema())

        events, _, _ = ingest_portnet.normalize_data(
            [
                {
                    "portCallId": 123,
                    "portAreaDetails": [
                        {
                            "portAreaCode": "VUOS",
                            "eta": "2026-11-10T12:00:00Z",
                            "etaTimestamp": "2026-11-10T07:00:00Z",
                        }
                    ],
                }
            ],
            "2026-11-10T08:00:00.000000Z",
        )
        self.assertEqual({event["source_id"] for event in events}, {"portnet"})


if __name__ == "__main__":
    unittest.main()
