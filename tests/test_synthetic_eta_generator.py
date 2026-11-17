"""Checks for the deterministic synthetic ETA dataset."""

import unittest
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from pydantic import ValidationError

from scripts.event_model import PortCallEvent
from scripts.generate_synthetic_eta import START, build_dataset, main
from scripts.ingest_fixture import canonical_hash


class SyntheticEtaGeneratorTests(unittest.TestCase):
    def test_cli_writes_to_the_configured_output_path(self):
        with TemporaryDirectory() as directory:
            output_path = Path(directory) / "run.jsonl"
            with patch(
                "sys.argv",
                [
                    "generate_synthetic_eta",
                    "--port-calls",
                    "1",
                    "--output",
                    str(output_path),
                ],
            ):
                main()

            self.assertTrue(output_path.is_file())
            self.assertEqual(
                len(output_path.read_text(encoding="utf-8").splitlines()),
                len(build_dataset(1)[0]),
            )

    def test_default_dataset_is_reproducible_and_has_expected_counts(self):
        events, summary = build_dataset(seed=42)
        self.assertEqual((events, summary), build_dataset(seed=42))
        self.assertEqual(summary["port_calls"], 50)
        self.assertEqual(summary["eta_updates"], 170)
        self.assertEqual(summary["event_occurrences"], len(events))
        self.assertEqual(summary["event_occurrences"], 181)
        self.assertEqual(
            summary["scenario_categories"],
            {
                "eta_revisions": 120,
                "delayed_events": 6,
                "late_arrivals": 5,
                "duplicate_deliveries": 7,
                "invalid_events": 4,
            },
        )
        self.assertEqual(summary["event_occurrences"], summary["eta_updates"] + 11)

    def test_calls_have_two_to_five_valid_updates_inside_the_30_day_window(self):
        events, summary = build_dataset(50)
        window_end = START + timedelta(days=30)
        updates_by_call = defaultdict(set)

        for event in events:
            for field in ("event_time_utc", "received_at_utc", "eta_utc"):
                self.assertTrue(event[field].endswith("Z"))
                timestamp = datetime.fromisoformat(event[field].replace("Z", "+00:00"))
                self.assertEqual(
                    timestamp.utcoffset(), timezone.utc.utcoffset(timestamp)
                )
                self.assertGreaterEqual(timestamp, START)
                self.assertLess(timestamp, window_end)
            self.assertTrue(event["port_call_id"].startswith("SYNTH-42-PC-"))
            self.assertNotEqual(event["source_id"], "portnet")
            if event["schema_version"] == "v1":
                PortCallEvent.model_validate(event)
                updates_by_call[event["port_call_id"]].add(event["event_id"])
            else:
                with self.assertRaises(ValidationError):
                    PortCallEvent.model_validate(event)

        self.assertEqual(len(updates_by_call), summary["port_calls"])
        for update_ids in updates_by_call.values():
            self.assertGreaterEqual(len(update_ids), 2)
            self.assertLessEqual(len(update_ids), 5)

    def test_duplicate_deliveries_keep_the_same_event_identity(self):
        events, summary = build_dataset()
        by_event_id = defaultdict(list)
        for event in events:
            by_event_id[event["event_id"]].append(event)

        duplicates = [items for items in by_event_id.values() if len(items) > 1]
        self.assertEqual(
            len(duplicates), summary["scenario_categories"]["duplicate_deliveries"]
        )
        for first, duplicate in duplicates:
            self.assertEqual(first["event_id"], duplicate["event_id"])
            self.assertNotEqual(first["received_at_utc"], duplicate["received_at_utc"])
            self.assertEqual(canonical_hash(first), canonical_hash(duplicate))

    def test_configured_size_stays_within_the_requested_range(self):
        events, summary = build_dataset(5, seed=7)
        self.assertEqual(summary["port_calls"], 5)
        self.assertEqual(len({event["port_call_id"] for event in events}), 5)
        large_events, large_summary = build_dataset(1000)
        self.assertEqual(large_summary["port_calls"], 1000)
        self.assertGreaterEqual(large_summary["eta_updates"], 2000)
        self.assertLessEqual(large_summary["eta_updates"], 5000)
        self.assertEqual(large_summary["event_occurrences"], len(large_events))
        with self.assertRaises(ValueError):
            build_dataset(1001)


if __name__ == "__main__":
    unittest.main()
