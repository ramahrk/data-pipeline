import gzip
import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from src.pipeline import process_data_batch


class TestPipeline(unittest.TestCase):
    """Tests for pipeline execution including erasure logic."""

    def setUp(self):
        """Set up temporary directory and mock data structure."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.current_date = "2020-01-23"
        self.previous_date = "2020-01-22"

        # Create erasure file for previous day (used by process_data_batch)
        prev_day_dir = os.path.join(
            self.temp_dir.name, f"date={self.previous_date}", "hour=00"
        )
        os.makedirs(prev_day_dir, exist_ok=True)
        self.erasure_file = os.path.join(prev_day_dir, "erasure-requests.json.gz")
        with gzip.open(self.erasure_file, "wt") as f:
            f.write(json.dumps({"customer-id": "abc123"}) + "\n")

        # Create data files for current day
        current_day_dir = os.path.join(
            self.temp_dir.name, f"date={self.current_date}", "hour=00"
        )
        os.makedirs(current_day_dir, exist_ok=True)
        for fname in ["products.json.gz", "customers.json.gz", "transactions.json.gz"]:
            with gzip.open(os.path.join(current_day_dir, fname), "wt") as f:
                f.write("{}")

    def tearDown(self):
        self.temp_dir.cleanup()

    @patch("src.pipeline.etl_records_processed")
    @patch("src.pipeline.etl_errors")
    @patch("src.pipeline.etl_records_processed_pushgateway")
    @patch("src.pipeline.needs_reference_data", return_value=False)
    @patch("src.pipeline.generate_stats")
    @patch("src.pipeline.apply_erasure_requests")
    @patch("src.pipeline.process_transactions")
    @patch("src.pipeline.process_customers")
    @patch("src.pipeline.process_products")
    def test_process_data_batch(
        self,
        mock_process_products,
        mock_process_customers,
        mock_process_transactions,
        mock_apply_erasure,
        mock_generate_stats,
        mock_needs_reference_data,
        mock_etl_push,
        mock_etl_errors,
        mock_etl_processed,
    ):
        """Test full batch including erasure call."""

        # Set mocks for metric counters
        mock_etl_processed.inc = MagicMock()
        mock_etl_push.inc = MagicMock()
        mock_etl_errors.inc = MagicMock()

        # Run the actual test
        process_data_batch(self.current_date, 0, base_path=self.temp_dir.name)

        # Assertions
        mock_apply_erasure.assert_called_once_with(self.erasure_file)
        mock_process_customers.assert_called_once()
        mock_process_products.assert_called_once()
        mock_process_transactions.assert_called_once()


if __name__ == "__main__":
    unittest.main()
