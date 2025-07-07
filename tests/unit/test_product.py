"""
Unit tests for product data processing.
"""

import gzip
import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from src.processors.product import process_products
from src.utils.data_access import find_product_by_sku
from src.utils.validation import validate_product


class TestProductProcessor(unittest.TestCase):
    """Test cases for product data processing."""

    def setUp(self):
        """Set up test environment."""
        # Create a temporary directory for test data
        self.temp_dir = tempfile.TemporaryDirectory()

        # Sample product data
        self.valid_product = {
            "sku": "PROD123",
            "name": "Test Product",
            "description": "A test product",
            "price": 19.99,
            "popularity": 4.5,
            "category": "electronics",  # Added category field
        }

        self.invalid_product = {
            "sku": "PROD456",
            "name": "Invalid Product",
            "description": "An invalid product",
            "price": -5.00,  # Negative price (invalid)
            "popularity": 0,  # Zero popularity (invalid)
        }

        # Create test input file
        self.input_path = os.path.join(self.temp_dir.name, "date=2020-01-23", "hour=00")
        os.makedirs(self.input_path, exist_ok=True)

        self.input_file = os.path.join(self.input_path, "products.json.gz")
        with gzip.open(self.input_file, "wt") as f:
            f.write(json.dumps(self.valid_product) + "\n")
            f.write(json.dumps(self.invalid_product) + "\n")

    def tearDown(self):
        """Clean up after tests."""
        self.temp_dir.cleanup()

    def test_validate_product_valid(self):
        """Test validation of a valid product."""
        errors = validate_product(self.valid_product)
        self.assertEqual(
            len(errors), 0, "Valid product should have no validation errors"
        )

    def test_validate_product_invalid(self):
        """Test validation of an invalid product."""
        errors = validate_product(self.invalid_product)
        self.assertGreater(
            len(errors), 0, "Invalid product should have validation errors"
        )

        # Check specific error messages
        error_messages = " ".join(errors)
        self.assertIn("Price", error_messages, "Should detect negative price")
        self.assertIn("Popularity", error_messages, "Should detect zero popularity")

    @patch("src.processors.product.etl_records_processed")
    @patch("src.processors.product.etl_errors")
    @patch("src.processors.product.etl_records_processed_pushgateway")
    @patch(
        "src.processors.product.OUTPUT_PATH", new_callable=lambda: tempfile.mkdtemp()
    )
    @patch(
        "src.processors.product.QUARANTINE_PATH",
        new_callable=lambda: tempfile.mkdtemp(),
    )
    @patch(
        "src.processors.product.REFERENCE_DATA_PATH",
        new_callable=lambda: tempfile.mkdtemp(),
    )
    def test_process_products(
        self,
        mock_ref_path,
        mock_quarantine_path,
        mock_output_path,
        mock_pushgateway,
        mock_etl_errors,
        mock_etl_records_processed,
    ):
        """Test processing of product data."""

        # Mock the metric increments to avoid errors
        mock_etl_records_processed.inc = MagicMock()
        mock_pushgateway.inc = MagicMock()
        mock_etl_errors.inc = MagicMock()

        # Process the test input file
        stats = process_products(self.input_file)

        # Check processing statistics
        self.assertEqual(stats["processed"], 2, "Should process 2 records")
        self.assertEqual(stats["valid"], 1, "Should find 1 valid record")
        self.assertEqual(stats["invalid"], 1, "Should find 1 invalid record")

        # Check output files
        output_dir = os.path.join(mock_output_path, "date=2020-01-23", "hour=00")
        output_file = os.path.join(output_dir, "products.json.gz")
        self.assertTrue(os.path.exists(output_file), "Output file should exist")

        # Check valid records in output file
        with gzip.open(output_file, "rt") as f:
            records = [json.loads(line) for line in f]
            self.assertEqual(len(records), 1, "Output should contain 1 valid record")
            self.assertEqual(
                records[0]["sku"],
                self.valid_product["sku"],
                "Output should contain the valid product",
            )

        # Check quarantined records
        quarantine_dir = os.path.join(
            mock_quarantine_path, "date=2020-01-23", "hour=00"
        )
        quarantine_file = os.path.join(quarantine_dir, "products_invalid.json.gz")
        self.assertTrue(os.path.exists(quarantine_file), "Quarantine file should exist")

        # Check invalid records in quarantine file
        with gzip.open(quarantine_file, "rt") as f:
            records = [json.loads(line) for line in f]
            self.assertEqual(
                len(records), 1, "Quarantine should contain 1 invalid record"
            )
            self.assertEqual(
                records[0]["sku"],
                self.invalid_product["sku"],
                "Quarantine should contain the invalid product",
            )
            self.assertIn(
                "_errors", records[0], "Quarantined record should have _errors field"
            )

        # Check reference data file
        reference_file = os.path.join(mock_ref_path, "products.json.gz")
        self.assertTrue(os.path.exists(reference_file), "Reference file should exist")

        # Check reference data content
        with gzip.open(reference_file, "rt") as f:
            records = [json.loads(line) for line in f]
            self.assertEqual(
                len(records), 1, "Reference data should contain 1 valid record"
            )
            self.assertEqual(
                records[0]["sku"],
                self.valid_product["sku"],
                "Reference data should contain the valid product",
            )

    def test_find_product_by_sku(self):
        """Test finding a product by SKU."""
        # Create a temporary directory for the test
        temp_dir = tempfile.mkdtemp()

        # Create a reference product file
        sku = "TEST-SKU"
        product_data = {
            "sku": sku,
            "name": "Test Product",
            "description": "A test product",
            "price": 29.99,
            "popularity": 4.8,
        }

        reference_dir = os.path.join(temp_dir, "products")
        os.makedirs(reference_dir, exist_ok=True)

        product_file = os.path.join(reference_dir, f"{sku}.json")
        with open(product_file, "w") as f:
            json.dump(product_data, f)

        # Test finding the product
        found_product = find_product_by_sku(sku, reference_path=temp_dir)
        self.assertIsNotNone(found_product, "Should find the product")
        self.assertEqual(found_product["sku"], sku, "Should return the correct product")

        # Test finding a non-existent product
        not_found = find_product_by_sku("nonexistent", reference_path=temp_dir)
        self.assertIsNone(not_found, "Should return None for non-existent product")


if __name__ == "__main__":
    unittest.main()
