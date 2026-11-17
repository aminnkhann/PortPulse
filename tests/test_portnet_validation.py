"""Portnet input validation and quarantine behavior."""

import unittest

from scripts import ingest_portnet


RECEIVED_AT = "2026-11-10T08:00:00.000000Z"


class PortnetValidationTests(unittest.TestCase):
    def test_invalid_call_is_quarantined_without_unneeded_fields(self):
        calls = [{"portCallId": 0, "mmsi": 999999999, "agent": "synthetic"}]
        events, rejected, metrics = ingest_portnet.normalize_data(calls, RECEIVED_AT)
        self.assertEqual(events, [])
        self.assertEqual(metrics["rejected_portnet_call_records"], 1)
        self.assertNotIn("agent", rejected[0]["raw_payload"])
        self.assertNotIn("mmsi", rejected[0]["raw_payload"])

    def test_invalid_eta_is_quarantined(self):
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

    def test_missing_area_is_quarantined_when_identity_is_ambiguous(self):
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


if __name__ == "__main__":
    unittest.main()
