"""Bounded, mocked HTTP retry behavior for Portnet requests."""

import json
import unittest
from email.message import Message
from urllib.error import HTTPError
from unittest.mock import patch

from scripts import ingest_portnet


def successful_response():
    class Response:
        headers = Message()

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self):
            return json.dumps({"portCalls": []}).encode()

    return Response()


class PortnetHttpTests(unittest.TestCase):
    @patch("scripts.ingest_portnet.time.sleep")
    @patch("scripts.ingest_portnet.urlopen")
    def test_retries_429_and_respects_retry_after(self, urlopen, sleep):
        headers = Message()
        headers["Retry-After"] = "3"
        urlopen.side_effect = [
            HTTPError("https://example.test", 429, "busy", headers, None),
            successful_response(),
        ]
        self.assertEqual(
            ingest_portnet.fetch_json("https://example.test"), {"portCalls": []}
        )
        sleep.assert_called_once_with(3)

    @patch("scripts.ingest_portnet.time.sleep")
    @patch("scripts.ingest_portnet.urlopen")
    def test_permanent_http_failure_is_not_retried(self, urlopen, sleep):
        urlopen.side_effect = HTTPError(
            "https://example.test", 400, "bad", Message(), None
        )
        with self.assertRaisesRegex(RuntimeError, "400"):
            ingest_portnet.fetch_json("https://example.test")
        urlopen.assert_called_once()
        sleep.assert_not_called()

    @patch("scripts.ingest_portnet.time.sleep")
    @patch("scripts.ingest_portnet.urlopen")
    def test_temporary_failures_stop_at_attempt_limit(self, urlopen, sleep):
        urlopen.side_effect = HTTPError(
            "https://example.test", 503, "busy", Message(), None
        )
        with self.assertRaisesRegex(RuntimeError, "503"):
            ingest_portnet.fetch_json("https://example.test")
        self.assertEqual(urlopen.call_count, ingest_portnet.MAX_FETCH_ATTEMPTS)
        self.assertEqual(sleep.call_count, ingest_portnet.MAX_FETCH_ATTEMPTS - 1)


if __name__ == "__main__":
    unittest.main()
