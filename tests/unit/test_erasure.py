import gzip
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from src.pipeline import process_data_batch


class TestErasureProcessor(unittest.TestCase):
    """Test cases for erasure request processing."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.erasure_date = "2020-01-25"
        self.processing_date = "2020-01-26"

        for hour_dir in ["hour=0", "hour=00"]:
            os.makedirs(
                os.path.join(self.temp_dir.name, f"date={self.erasure_date}", hour_dir),
                exist_ok=True,
            )

        # hour=0 erasure file
        self.erasure_file_0 = os.path.join(
            self.temp_dir.name,
            f"date={self.erasure_date}",
            "hour=0",
            "erasure-requests.json.gz",
        )
        with gzip.open(self.erasure_file_0, "wt") as f:
            f.write(json.dumps({"customer-id": "cust123"}) + "\n")

        # hour=00 erasure file
        self.erasure_file_00 = os.path.join(
            self.temp_dir.name,
            f"date={self.erasure_date}",
            "hour=00",
            "erasure-requests.json.gz",
        )
        with gzip.open(self.erasure_file_00, "wt") as f:
            f.write(json.dumps({"customer-id": "cust456"}) + "\n")

        # Create processing date/hour files for products, customers, transactions
        self.processing_path = os.path.join(
            self.temp_dir.name, f"date={self.processing_date}", "hour=0"
        )
        os.makedirs(self.processing_path, exist_ok=True)

        for fname in ["products.json.gz", "customers.json.gz", "transactions.json.gz"]:
            with gzip.open(os.path.join(self.processing_path, fname), "wt") as f:
                f.write("{}")

    def tearDown(self):
        self.temp_dir.cleanup()

    @patch("src.pipeline.os.path.exists")
    @patch("src.pipeline.apply_erasure_requests")
    @patch("src.pipeline.scan_for_new_data")
    @patch("src.pipeline.load_all_products_for_date")
    @patch("src.pipeline.generate_stats")
    def test_erasure_requests_applied_for_hour_0_first(
        self,
        mock_generate_stats,
        mock_load_products,
        mock_scan,
        mock_apply_erasure,
        mock_exists,
    ):
        """Ensure erasure requests from hour=0 are applied if present."""

        mock_scan.return_value = {
            "products": os.path.join(self.processing_path, "products.json.gz"),
            "customers": os.path.join(self.processing_path, "customers.json.gz"),
            "transactions": os.path.join(self.processing_path, "transactions.json.gz"),
        }

        mock_load_products.return_value = {}
        mock_apply_erasure.return_value = {}

        mock_exists.side_effect = lambda path: path == self.erasure_file_0

        process_data_batch(self.processing_date, 0, base_path=self.temp_dir.name)
        mock_apply_erasure.assert_called_once_with(self.erasure_file_0)

    @patch("src.pipeline.os.path.exists")
    @patch("src.pipeline.apply_erasure_requests")
    @patch("src.pipeline.scan_for_new_data")
    @patch("src.pipeline.load_all_products_for_date")
    @patch("src.pipeline.generate_stats")
    def test_erasure_requests_fallback_to_hour_00(
        self,
        mock_generate_stats,
        mock_load_products,
        mock_scan,
        mock_apply_erasure,
        mock_exists,
    ):
        """Ensure fallback to hour=00 if hour=0 is missing."""

        mock_scan.return_value = {
            "products": os.path.join(self.processing_path, "products.json.gz"),
            "customers": os.path.join(self.processing_path, "customers.json.gz"),
            "transactions": os.path.join(self.processing_path, "transactions.json.gz"),
        }

        mock_load_products.return_value = {}
        mock_apply_erasure.return_value = {}

        mock_exists.side_effect = lambda path: path == self.erasure_file_00

        process_data_batch(self.processing_date, 0, base_path=self.temp_dir.name)
        mock_apply_erasure.assert_called_once_with(self.erasure_file_00)


if __name__ == "__main__":
    unittest.main()
