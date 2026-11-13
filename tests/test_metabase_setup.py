import unittest
from unittest.mock import patch

from scripts.setup_metabase import verify_card_queries


class FailedMetabaseQueryTests(unittest.TestCase):
    @patch(
        "scripts.setup_metabase.request",
        return_value={"status": "failed", "error": "permission denied"},
    )
    def test_failed_saved_question_stops_setup(self, request):
        with self.assertRaisesRegex(RuntimeError, "permission denied"):
            verify_card_queries("session", {"Latest ETA records": 3})
        request.assert_called_once_with("/api/card/3/query", "POST", {}, "session")


if __name__ == "__main__":
    unittest.main()
