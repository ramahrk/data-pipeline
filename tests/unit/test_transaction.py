"""
Unit tests for transaction data processing.
"""

import gzip
import json
import os
import tempfile
import unittest
from unittest.mock import patch

from src.processors.transaction import process_transactions
from src.utils.validation import validate_transaction


class TestTransactionProcessor(unittest.TestCase):
    """Test cases for transaction data processing."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.TemporaryDirectory()

        # Valid and invalid transactions (flat structure)
        self.valid_transaction = {
            "transaction_id": "T123",
            "customer_id": "cust123",
            "sku": "PROD123",
            "quantity": 2,
            "total_cost": 39.98,
        }

        self.invalid_transaction = {
            "transaction_id": "T456",
            "customer_id": "nonexistent",
            "sku": "PROD123",
            "quantity": 3,
            "total_cost": 50.00,  # Should be 3 * 19.99 = 59.97
        }

        self.input_path = os.path.join(self.temp_dir.name, "date=2020-01-23", "hour=00")
        os.makedirs(self.input_path, exist_ok=True)
        self.input_file = os.path.join(self.input_path, "transactions.json.gz")
        with gzip.open(self.input_file, "wt") as f:
            f.write(json.dumps(self.valid_transaction) + "\n")
            f.write(json.dumps(self.invalid_transaction) + "\n")

    def tearDown(self):
        """Clean up after tests."""
        self.temp_dir.cleanup()

    @patch("src.utils.validation.find_product_by_sku")
    @patch("src.utils.validation.find_customer_by_id")
    def test_validate_transaction_valid(self, mock_find_customer, mock_find_product):
        """Test valid transaction validation."""
        mock_find_customer.return_value = {"id": "cust123"}
        mock_find_product.return_value = {"sku": "PROD123", "price": 19.99}

        errors = validate_transaction(self.valid_transaction)
        self.assertEqual(len(errors), 0)

    @patch("src.utils.validation.find_product_by_sku")
    @patch("src.utils.validation.find_customer_by_id")
    def test_validate_transaction_invalid(self, mock_find_customer, mock_find_product):
        """Test invalid transaction validation."""
        mock_find_customer.return_value = None
        mock_find_product.return_value = {"sku": "PROD123", "price": 19.99}

        errors = validate_transaction(self.invalid_transaction)
        self.assertTrue(any("Total cost" in e for e in errors))

    @patch("src.processors.transaction.OUTPUT_PATH", new_callable=tempfile.mkdtemp)
    @patch("src.processors.transaction.QUARANTINE_PATH", new_callable=tempfile.mkdtemp)
    @patch(
        "src.processors.transaction.REFERENCE_DATA_PATH", new_callable=tempfile.mkdtemp
    )
    @patch("src.processors.transaction.etl_errors", create=True)
    def test_process_transactions(
        self, _, mock_ref_path, mock_quarantine_path, mock_output_path
    ):
        """Test processing transactions with one valid and one invalid."""
        with patch("src.utils.validation.find_product_by_sku") as mock_product, patch(
            "src.utils.validation.find_customer_by_id"
        ) as mock_customer:

            mock_product.return_value = {"sku": "PROD123", "price": 19.99}
            mock_customer.return_value = {"id": "cust123"}

            stats = process_transactions(self.input_file)

            self.assertEqual(stats["processed"], 2)
            self.assertEqual(stats["valid"], 1)
            self.assertEqual(stats["invalid"], 1)

            # Check valid transaction in output
            output_dir = os.path.join(mock_output_path, "date=2020-01-23", "hour=00")
            output_file = os.path.join(output_dir, "transactions.json.gz")
            self.assertTrue(os.path.exists(output_file))
            with gzip.open(output_file, "rt") as f:
                records = [json.loads(line) for line in f]
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["transaction_id"], "T123")

            # Check invalid transaction in quarantine
            quarantine_dir = os.path.join(
                mock_quarantine_path, "date=2020-01-23", "hour=00"
            )
            quarantine_file = os.path.join(
                quarantine_dir, "transactions_invalid.json.gz"
            )
            self.assertTrue(os.path.exists(quarantine_file))
            with gzip.open(quarantine_file, "rt") as f:
                records = [json.loads(line) for line in f]
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["transaction_id"], "T456")
            self.assertIn("_errors", records[0])


if __name__ == "__main__":
    unittest.main()
