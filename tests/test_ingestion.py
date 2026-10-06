import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import requests

import main


def response(body, status=200):
    result = requests.Response()
    result.status_code = status
    result.url = "https://example.test/api/indicator"
    result._content = body.encode("utf-8")
    result._content_consumed = True
    return result


class IngestionTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        previous_directory = Path.cwd()
        os.chdir(directory.name)
        self.addCleanup(os.chdir, previous_directory)
        self.output = Path("bronze_layer/sample.json")

    def test_success_preserves_complete_payload_and_creates_directory(self):
        payload = {"@odata.context": "source metadata", "value": [{"NumericValue": None}]}
        with patch("main.requests.get", return_value=response(json.dumps(payload))):
            self.assertTrue(main.fetch_data("sample", "https://example.test/api/indicator"))
        self.assertEqual(json.loads(self.output.read_text()), payload)

    def test_html_http_error_is_reported_before_json_parsing(self):
        self.output.parent.mkdir()
        self.output.write_text("previous snapshot")
        with patch("main.requests.get", return_value=response("<html>Unavailable</html>", 503)):
            with self.assertLogs("main", level="ERROR") as logs:
                self.assertFalse(main.fetch_data("sample", "https://example.test/api/indicator"))
        self.assertIn("HTTP", logs.output[0])
        self.assertIn("503", logs.output[0])
        self.assertEqual(self.output.read_text(), "previous snapshot")

    def test_invalid_json_is_not_saved(self):
        with patch("main.requests.get", return_value=response("not JSON")):
            with self.assertLogs("main", level="ERROR") as logs:
                self.assertFalse(main.fetch_data("sample", "https://example.test/api/indicator"))
        self.assertIn("JSON", logs.output[0])
        self.assertFalse(self.output.exists())

    def test_unexpected_collection_shapes_are_not_saved(self):
        for payload in ([], {}, {"value": None}, {"value": {}}):
            with self.subTest(payload=payload):
                with patch("main.requests.get", return_value=response(json.dumps(payload))):
                    with self.assertLogs("main", level="ERROR"):
                        self.assertFalse(main.fetch_data("sample", "https://example.test/api/indicator"))
                self.assertFalse(self.output.exists())

    def test_transport_failures_are_reported_without_creating_output(self):
        failures = (
            (requests.exceptions.Timeout("read timeout"), "timed out"),
            (requests.exceptions.ConnectionError("connection refused"), "network"),
        )
        for error, category in failures:
            with self.subTest(error=error):
                with patch("main.requests.get", side_effect=error):
                    with self.assertLogs("main", level="ERROR") as logs:
                        self.assertFalse(main.fetch_data("sample", "https://example.test/api/indicator"))
                self.assertIn(category, logs.output[0])
                self.assertIn("sample", logs.output[0])
                self.assertIn("https://example.test/api/indicator", logs.output[0])
                self.assertFalse(self.output.exists())

    def test_filesystem_failure_is_reported(self):
        Path("bronze_layer").write_text("not a directory")
        with patch("main.requests.get", return_value=response('{"value": []}')):
            with self.assertLogs("main", level="ERROR") as logs:
                self.assertFalse(main.fetch_data("sample", "https://example.test/api/indicator"))
        self.assertIn("write", logs.output[0])
        self.assertEqual(Path("bronze_layer").read_text(), "not a directory")

    def test_failed_run_still_saves_later_datasets(self):
        responses = [response("Unavailable", 503)]
        responses.extend(response('{"value": []}') for _ in range(5))
        with patch("main.requests.get", side_effect=responses):
            with self.assertLogs("main", level="ERROR"):
                self.assertEqual(main.main(), 1)
        self.assertFalse(Path("bronze_layer/dimensions.json").exists())
        self.assertEqual(json.loads(Path("bronze_layer/hale_at_birth.json").read_text()), {"value": []})


if __name__ == "__main__":
    unittest.main()
