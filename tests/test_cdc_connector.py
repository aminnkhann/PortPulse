"""Tests for Debezium connector registration."""

import json
import os
import unittest
from unittest.mock import patch

from scripts import register_cdc_connector


class ConnectorRegistrationTests(unittest.TestCase):
    @patch("scripts.register_cdc_connector.request_json")
    def test_registers_missing_connector_and_waits_for_task(self, request):
        request.side_effect = [
            [],
            None,
            {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]},
        ]
        with patch.dict(os.environ, {"POSTGRES_PASSWORD": "demo"}):
            register_cdc_connector.register_connector()
        self.assertEqual(request.call_args_list[1].args[:2], ("/connectors", "POST"))

    @patch("scripts.register_cdc_connector.request_json")
    def test_accepts_existing_matching_connector(self, request):
        connector = json.loads(register_cdc_connector.CONNECTOR_FILE.read_text())
        current = {
            key: value
            for key, value in connector["config"].items()
            if key != "database.password"
        }
        request.side_effect = [
            [register_cdc_connector.CONNECTOR_NAME],
            current,
            {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]},
        ]
        register_cdc_connector.register_connector()
        self.assertNotIn(
            "POST",
            [call.args[1] for call in request.call_args_list if len(call.args) > 1],
        )


if __name__ == "__main__":
    unittest.main()
