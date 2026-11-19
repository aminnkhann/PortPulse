"""Regression checks for the real and simulated Metabase dashboards."""

from copy import deepcopy
import unittest
from unittest.mock import patch

from scripts.setup_metabase import DASHBOARDS, setup_cards_and_dashboards


class MetabaseApiStub:
    def __init__(self, state):
        self.state = state

    def __call__(self, path, method="GET", payload=None, session=None):
        if path == "/api/collection":
            return [{"id": 1, "name": "PortPulse"}]
        if path == "/api/collection/1/items":
            return deepcopy(list(self.state["cards"].values()))
        if path.startswith("/api/dashboard"):
            return self.dashboard(path, method, payload)
        if path.startswith("/api/card"):
            return self.card(path, method, payload)
        raise AssertionError(f"Unexpected Metabase request: {method} {path}")

    def dashboard(self, path, method, payload):
        if path == "/api/dashboard" and method == "GET":
            return deepcopy(self.state["dashboards"])
        if path == "/api/dashboard":
            dashboard_id = self.state["next_dashboard_id"]
            self.state["next_dashboard_id"] += 1
            self.state["dashboards"].append(
                {"id": dashboard_id, "name": payload["name"]}
            )
            self.state["dashcards"][dashboard_id] = []
            return {"id": dashboard_id}
        dashboard_id = int(path.rsplit("/", 1)[1])
        if method == "GET":
            return {
                "id": dashboard_id,
                "dashcards": deepcopy(self.state["dashcards"][dashboard_id]),
            }
        self.state["dashcards"][dashboard_id] = deepcopy(payload["dashcards"])
        return {"id": dashboard_id}

    def card(self, path, method, payload):
        if path.endswith("/query"):
            return {"status": "completed"}
        if path == "/api/card":
            card_id = self.state["next_card_id"]
            self.state["next_card_id"] += 1
            self.state["created_cards"] += 1
        else:
            card_id = int(path.rsplit("/", 1)[1])
        self.state["cards"][card_id] = {
            **deepcopy(payload),
            "id": card_id,
            "model": "card",
        }
        return {"id": card_id}


class DashboardDefinitionsTests(unittest.TestCase):
    def test_legacy_dashboard_cards_are_migrated_without_duplicates(self):
        state = {
            "cards": {
                card_id: {"id": card_id, "name": name, "model": "card"}
                for card_id, name in (
                    (10, "Rejected observations"),
                    (11, "Duplicate issues"),
                    (12, "Collision issues"),
                    (13, "Mart refreshed at (UTC)"),
                    (14, "Portnet — Rejected observations"),
                    (99, "My saved question"),
                )
            },
            "dashboards": [
                {"id": 1, "name": "PortPulse — Portnet Overview"},
                {"id": 2, "name": "My dashboard"},
            ],
            "dashcards": {
                1: [
                    {"id": card_id, "card_id": card_id}
                    for card_id in (10, 11, 12, 13, 14, 99, 10)
                ],
                2: [{"id": 90, "card_id": 99}],
            },
            "next_card_id": 100,
            "next_dashboard_id": 3,
            "created_cards": 0,
        }

        custom_dashboard_before = deepcopy(state["dashcards"][2])
        with patch(
            "scripts.setup_metabase.request", side_effect=MetabaseApiStub(state)
        ):
            setup_cards_and_dashboards("session", 1)
            first_card_count = len(state["cards"])
            first_create_count = state["created_cards"]
            setup_cards_and_dashboards("session", 1)

        portnet = state["dashcards"][1]
        referenced_ids = [item["card_id"] for item in portnet]
        current_names = set(DASHBOARDS["PortPulse — Portnet Overview"])
        current_cards = {
            item["name"]: item
            for item in state["cards"].values()
            if item["name"] in current_names
        }
        self.assertEqual(len(current_cards), len(current_names))
        for name, card in current_cards.items():
            self.assertEqual(referenced_ids.count(card["id"]), 1, name)
        self.assertNotIn(14, referenced_ids)
        self.assertIn(99, referenced_ids)
        self.assertEqual(state["cards"][10]["name"], "Unique rejected records")
        self.assertIn(
            "unique_rejected_records",
            state["cards"][10]["dataset_query"]["native"]["query"],
        )
        self.assertEqual(state["dashcards"][2], custom_dashboard_before)
        self.assertEqual(len(state["cards"]), first_card_count)
        self.assertEqual(state["created_cards"], first_create_count)

    def test_dashboards_keep_real_and_simulated_data_separate(self):
        real = DASHBOARDS["PortPulse — Portnet Overview"]
        synthetic = DASHBOARDS["PortPulse — Synthetic ETA Analytics (SIMULATED DATA)"]
        cdc = DASHBOARDS["PortPulse — Kafka CDC Demo (SIMULATED DATA)"]

        self.assertIn("Captured ETA Observations — Daily Trend", real)
        self.assertIn("Portnet identity collision versions", real)
        self.assertIn("Latest recorded Portnet collection (UTC)", real)
        self.assertIn(
            "current_timestamp", real["Latest recorded Portnet collection (UTC)"][1]
        )
        self.assertIn("Synthetic ETA revision direction", synthetic)
        self.assertIn("CDC changes by operation (SIMULATED)", cdc)

        for _, query, _ in real.values():
            self.assertNotIn("synthetic_demo", query)
            self.assertNotIn("mart_synthetic", query)
            self.assertNotIn("stream_demo", query)
            self.assertNotIn("mart_stream", query)
        for _, query, _ in synthetic.values():
            self.assertNotIn("mart_portnet", query)
            self.assertNotIn("stg_portnet", query)
            self.assertNotIn("mart_stream", query)
        for _, query, _ in cdc.values():
            self.assertNotIn("mart_portnet", query)
            self.assertNotIn("stg_portnet", query)
            self.assertNotIn("mart_synthetic", query)

    def test_question_names_and_grid_positions(self):
        names = [name for cards in DASHBOARDS.values() for name in cards]
        self.assertEqual(len(names), len(set(names)))
        allowed_displays = {"scalar", "bar", "row", "line", "table"}
        for cards in DASHBOARDS.values():
            rectangles = []
            for display, query, (row, col, width, height) in cards.values():
                self.assertIn(display, allowed_displays)
                self.assertIn("analytics.", query)
                self.assertGreater(width, 0)
                self.assertGreater(height, 0)
                self.assertGreaterEqual(col, 0)
                self.assertLessEqual(col + width, 24)
                for old_row, old_col, old_width, old_height in rectangles:
                    overlaps = (
                        row < old_row + old_height
                        and old_row < row + height
                        and col < old_col + old_width
                        and old_col < col + width
                    )
                    self.assertFalse(overlaps, "Dashboard cards overlap")
                rectangles.append((row, col, width, height))

    def test_real_trend_uses_provider_timestamp(self):
        real = DASHBOARDS["PortPulse — Portnet Overview"]
        _, query, _ = real["Captured ETA Observations — Daily Trend"]
        self.assertIn("provider_updated_at_utc", query)
        self.assertNotIn("current_timestamp", query)

    def test_eta_revision_chart_uses_estimate_changes_not_actual_arrivals(self):
        synthetic = DASHBOARDS["PortPulse — Synthetic ETA Analytics (SIMULATED DATA)"]
        _, query, _ = synthetic["Synthetic ETA revision direction"]
        self.assertIn("eta_change_minutes", query)
        self.assertIn("previous_eta_utc IS NOT NULL", query)


if __name__ == "__main__":
    unittest.main()
