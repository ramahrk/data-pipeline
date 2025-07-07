"""
Unit tests for customer data processing.
"""

import gzip
import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, Mock, patch

from src.processors.customer import process_customers, update_customer
from src.utils.data_access import find_customer_by_id
from src.utils.validation import validate_customer


class TestCustomerProcessor(unittest.TestCase):
    """Test cases for customer data processing."""

    def setUp(self):
        """Set up test environment."""
        # Create a temporary directory for test data
        self.temp_dir = tempfile.TemporaryDirectory()

        # Sample customer data
        self.valid_customer = {
            "id": "cust123",
            "first_name": "John",
            "last_name": "Doe",
            "email": "john.doe@example.com",
        }

        self.invalid_customer = {
            "id": "cust456",
            "first_name": "Jane",
            "last_name": "",  # Empty last name (invalid)
            "email": "invalid-email",  # Invalid email format
        }

        # Create test input file
        self.input_path = os.path.join(self.temp_dir.name, "date=2020-01-23", "hour=00")
        os.makedirs(self.input_path, exist_ok=True)

        self.input_file = os.path.join(self.input_path, "customers.json.gz")
        with gzip.open(self.input_file, "wt") as f:
            f.write(json.dumps(self.valid_customer) + "\n")
            f.write(json.dumps(self.invalid_customer) + "\n")

    def tearDown(self):
        """Clean up after tests."""
        self.temp_dir.cleanup()

    def test_validate_customer_valid(self):
        """Test validation of a valid customer."""
        errors = validate_customer(self.valid_customer)
        self.assertEqual(
            len(errors), 0, "Valid customer should have no validation errors"
        )

    def test_validate_customer_invalid(self):
        """Test validation of an invalid customer."""
        errors = validate_customer(self.invalid_customer)
        self.assertGreater(
            len(errors), 0, "Invalid customer should have validation errors"
        )

        # Check specific error messages
        error_messages = " ".join(errors)
        self.assertIn("last_name", error_messages, "Should detect empty last name")
        self.assertIn("email", error_messages, "Should detect invalid email format")

    @patch("src.processors.customer.etl_records_processed", new_callable=Mock)
    @patch(
        "src.processors.customer.etl_records_processed_pushgateway", new_callable=Mock
    )
    @patch("src.processors.customer.etl_errors", new_callable=Mock)
    @patch(
        "src.processors.customer.REFERENCE_DATA_PATH",
        new_callable=lambda: tempfile.mkdtemp(),
    )
    @patch(
        "src.processors.customer.QUARANTINE_PATH",
        new_callable=lambda: tempfile.mkdtemp(),
    )
    @patch(
        "src.processors.customer.OUTPUT_PATH", new_callable=lambda: tempfile.mkdtemp()
    )
    def test_process_customers(
        self,
        mock_output_path,
        mock_quarantine_path,
        mock_reference_path,
        mock_etl_errors,
        mock_etl_push,
        mock_etl_records_processed,
    ):
        """Test processing of customer data."""

        # Mock the metric increments to avoid errors
        mock_etl_records_processed.inc = MagicMock()
        mock_etl_push.inc = MagicMock()
        mock_etl_errors.inc = MagicMock()

        # Process the test input file
        stats = process_customers(self.input_file)

        # Check processing statistics
        self.assertEqual(stats["processed"], 2, "Should process 2 records")
        self.assertEqual(stats["valid"], 1, "Should find 1 valid record")
        self.assertEqual(stats["invalid"], 1, "Should find 1 invalid record")

        # Check output files
        output_dir = os.path.join(mock_output_path, "date=2020-01-23", "hour=00")
        output_file = os.path.join(output_dir, "customers.json.gz")
        self.assertTrue(os.path.exists(output_file), "Output file should exist")

        # Check valid records in output file
        with gzip.open(output_file, "rt") as f:
            records = [json.loads(line.strip()) for line in f]
            self.assertEqual(len(records), 1, "Output should contain 1 valid record")
            self.assertEqual(
                records[0]["id"],
                self.valid_customer["id"],
                "Output should contain the valid customer",
            )

        # Check quarantined records
        quarantine_dir = os.path.join(
            mock_quarantine_path, "date=2020-01-23", "hour=00"
        )
        quarantine_file = os.path.join(quarantine_dir, "customers_invalid.json.gz")
        self.assertTrue(os.path.exists(quarantine_file), "Quarantine file should exist")

        # Check invalid records in quarantine file
        with gzip.open(quarantine_file, "rt") as f:
            records = [json.loads(line.strip()) for line in f]
            self.assertEqual(
                len(records), 1, "Quarantine should contain 1 invalid record"
            )
            self.assertEqual(
                records[0]["id"],
                self.invalid_customer["id"],
                "Quarantine should contain the invalid customer",
            )
            self.assertIn(
                "_errors", records[0], "Quarantined record should have _errors field"
            )

    def test_find_customer_by_id(self):
        """Test finding a customer by ID."""
        # Create a temporary directory for the test
        temp_dir = tempfile.mkdtemp()

        # Create a reference customer file
        customer_id = "test123"
        customer_data = {
            "id": customer_id,
            "first_name": "Test",
            "last_name": "User",
            "email": "test@example.com",
        }

        reference_dir = os.path.join(temp_dir, "customers")
        os.makedirs(reference_dir, exist_ok=True)

        customer_file = os.path.join(reference_dir, f"{customer_id}.json")
        with open(customer_file, "w") as f:
            json.dump(customer_data, f)

        # Test finding the customer using the temp directory
        found_customer = find_customer_by_id(customer_id, reference_path=temp_dir)
        self.assertIsNotNone(found_customer, "Should find the customer")
        self.assertEqual(
            found_customer["id"], customer_id, "Should return the correct customer"
        )

        # Test finding a non-existent customer
        not_found = find_customer_by_id("nonexistent", reference_path=temp_dir)
        self.assertIsNone(not_found, "Should return None for non-existent customer")

    @patch(
        "src.processors.customer.REFERENCE_DATA_PATH",
        new_callable=lambda: tempfile.mkdtemp(),
    )
    def test_update_customer(self, mock_ref_path):
        """Test updating a customer."""
        # Create a reference customer file
        customer_id = "test456"
        customer_data = {
            "id": customer_id,
            "first_name": "Original",
            "last_name": "Name",
            "email": "original@example.com",
        }

        reference_dir = os.path.join(mock_ref_path, "customers")
        os.makedirs(reference_dir, exist_ok=True)

        customer_file = os.path.join(reference_dir, f"{customer_id}.json")
        with open(customer_file, "w") as f:
            json.dump(customer_data, f)

        # Update the customer
        updated_data = customer_data.copy()
        updated_data["first_name"] = "Updated"
        updated_data["email"] = "updated@example.com"

        result = update_customer(updated_data)
        self.assertTrue(result, "Update should succeed")

        # Verify the update
        with open(customer_file, "r") as f:
            stored_data = json.load(f)

        self.assertEqual(
            stored_data["first_name"], "Updated", "First name should be updated"
        )
        self.assertEqual(
            stored_data["email"], "updated@example.com", "Email should be updated"
        )

        # Test updating a customer without ID
        invalid_update = {"first_name": "Invalid"}
        result = update_customer(invalid_update)
        self.assertFalse(result, "Update without ID should fail")


if __name__ == "__main__":
    unittest.main()
