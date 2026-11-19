"""Regression checks for the real and simulated Metabase dashboards."""

import unittest

from scripts.setup_metabase import DASHBOARDS


class DashboardDefinitionsTests(unittest.TestCase):
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
