import unittest
from unittest.mock import Mock, patch

from scripts.setup_metabase import (
    CARDS,
    CARD_LAYOUT,
    configure_reader,
    find_or_create_collection,
    get_session,
)


class MetabasePermissionTests(unittest.TestCase):
    def test_reader_is_confined_to_analytics_and_future_dbt_tables(self):
        connection = Mock()
        connection.execute.return_value.fetchone.return_value = (1,)
        connection.execute.return_value.fetchall.return_value = []

        configure_reader(connection, "local-test-password")

        statements = [
            call.args[0].as_string()
            if hasattr(call.args[0], "as_string")
            else call.args[0]
            for call in connection.execute.call_args_list
        ]
        combined = "\n".join(str(statement) for statement in statements)
        self.assertIn("GRANT SELECT ON ALL TABLES IN SCHEMA analytics", combined)
        self.assertIn(
            "ALTER DEFAULT PRIVILEGES FOR ROLE portpulse IN SCHEMA analytics", combined
        )
        self.assertIn("REVOKE ALL ON SCHEMA public, stream_demo", combined)
        self.assertIn("NOBYPASSRLS PASSWORD", combined)
        self.assertIn("REVOKE CREATE ON SCHEMA analytics", combined)
        self.assertIn('IN SCHEMA "public" REVOKE ALL ON TABLES', combined)
        self.assertIn('IN SCHEMA "stream_demo" REVOKE ALL ON TABLES', combined)

    @patch("scripts.setup_metabase.request")
    def test_existing_collection_is_reused(self, request):
        request.return_value = [{"name": "PortPulse", "id": 7}]

        result = find_or_create_collection("session")

        self.assertEqual(result["id"], 7)
        request.assert_called_once_with("/api/collection", session="session")

    @patch("scripts.setup_metabase.request")
    def test_existing_admin_session_is_reused(self, request):
        request.side_effect = [
            {"setup-token": None, "has-user-setup": True},
            {"id": "session-id"},
        ]

        self.assertEqual(
            get_session("admin@example.test", "local-password1"), "session-id"
        )
        self.assertEqual(request.call_count, 2)
        self.assertEqual(request.call_args.args[0], "/api/session")

    def test_dashboards_use_only_the_existing_marts(self):
        self.assertTrue(
            all(
                "mart_stream_latest_eta" not in query
                for _, query in list(CARDS.values())[:8]
            )
        )
        self.assertTrue(
            all(
                "mart_stream_latest_eta" in query
                for _, query in list(CARDS.values())[8:]
            )
        )
        self.assertEqual(CARD_LAYOUT["Portnet — Current ETAs"][2], 24)
        self.assertEqual(CARD_LAYOUT["CDC — Current simulated ETAs"][2], 24)


if __name__ == "__main__":
    unittest.main()
