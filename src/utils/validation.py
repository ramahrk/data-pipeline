"""
Validation utilities for data processing.
"""

import re
from decimal import Decimal

from src.utils.data_access import find_customer_by_id, find_product_by_sku
from src.utils.logging import setup_logger

# Set up logger
logger = setup_logger(__name__)

# Email validation regex pattern
EMAIL_PATTERN = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


def is_valid_email(email):
    """
    Validate email format.

    Args:
        email (str): Email address to validate

    Returns:
        bool: True if valid, False otherwise
    """
    if not email:
        return False

    return bool(EMAIL_PATTERN.match(email))


def validate_customer(customer):
    """
    Validate a customer record against defined constraints.

    Args:
        customer (dict): Customer record to validate

    Returns:
        list: List of validation errors, empty if valid
    """
    errors = []

    # Check required fields
    required_fields = ["id", "first_name", "last_name", "email"]
    for field in required_fields:
        if field not in customer or not customer[field]:
            errors.append(f"Missing required field: {field}")

    # Validate email format
    if "email" in customer and customer["email"]:
        if not is_valid_email(customer["email"]):
            errors.append("Invalid email format")

    return errors


def validate_product(product):
    """
    Validate a product record against defined constraints.

    Args:
        product (dict): Product record to validate

    Returns:
        list: List of validation errors, empty if valid
    """
    errors = []

    # Check all fields are populated
    required_fields = ["sku", "name", "price", "category", "popularity"]
    for field in required_fields:
        if field not in product or product[field] is None:
            errors.append(f"Missing required field: {field}")

    # Validate price is positive
    if "price" in product and product["price"] is not None:
        try:
            price = Decimal(str(product["price"]))
            if price <= 0:
                errors.append("Price must be positive")
        except (ValueError, TypeError, Decimal.InvalidOperation):
            errors.append("Invalid price format")

    # Validate popularity > 0
    if "popularity" in product and product["popularity"] is not None:
        try:
            popularity = float(product["popularity"])
            if popularity <= 0:
                errors.append("Popularity must be greater than 0")
        except (ValueError, TypeError):
            errors.append("Invalid popularity format")

    return errors


def validate_transaction(transaction, products_by_sku=None, customers_by_id=None):
    """
    Validate a transaction record against defined constraints.

    Args:
        transaction (dict): Transaction record to validate

    Returns:
        list: List of validation errors, empty if valid
    """
    errors = []

    # Check required fields
    required_fields = ["transaction_id", "customer_id", "sku", "quantity", "total_cost"]
    for field in required_fields:
        if field not in transaction or transaction[field] is None:
            errors.append(f"Missing required field: {field}")

    # If basic field validation failed, return early
    if errors:
        return errors

    # Validate customer_id exists
    if "customer_id" in transaction and transaction["customer_id"] is not None:
        customer = find_customer_by_id(transaction["customer_id"])
        if not customer:
            logger.warning(f"Customer ID {transaction['customer_id']} not found")

    # Validate product sku exists
    if "sku" in transaction and transaction["sku"] is not None:
        product = (
            products_by_sku.get(transaction["sku"])
            if products_by_sku
            else find_product_by_sku(transaction["sku"])
        )

        if not product:
            # Log a warning if product not found
            # This allows processing transactions even if product data hasn't been loaded yet
            logger.warning(f"Product SKU {transaction['sku']} not found")
        else:
            # Validate total_cost equals price × quantity
            if (
                "total_cost" in transaction
                and transaction["total_cost"] is not None
                and "quantity" in transaction
                and transaction["quantity"] is not None
            ):
                try:
                    expected_cost = Decimal(str(product["price"])) * Decimal(
                        str(transaction["quantity"])
                    )
                    actual_cost = Decimal(str(transaction["total_cost"]))

                    # Allow for small rounding differences
                    if abs(expected_cost - actual_cost) > Decimal("0.01"):
                        errors.append(
                            f"Total cost {actual_cost} does not match expected cost {expected_cost}"
                        )
                except (ValueError, TypeError, Decimal.InvalidOperation):
                    errors.append("Invalid cost or quantity format")

    return errors


def validate_erasure_request(request):
    """
    Validate an erasure request against defined constraints.

    Args:
        request (dict): Erasure request to validate

    Returns:
        list: List of validation errors, empty if valid
    """
    errors = []

    # Check that at least one of customer-id or email is present
    if ("customer-id" not in request or not request["customer-id"]) and (
        "email" not in request or not request["email"]
    ):
        errors.append(
            "Erasure request must include at least one of customer-id or email"
        )

    # Validate email format if present
    if "email" in request and request["email"]:
        if not is_valid_email(request["email"]):
            errors.append("Invalid email format")

    return errors


def validate_record(record_type, record):
    """
    Validate a record based on its type.

    Args:
        record_type (str): Type of record ('customer', 'product', 'transaction', 'erasure')
        record (dict): Record to validate

    Returns:
        list: List of validation errors, empty if valid
    """
    if record_type == "customer":
        return validate_customer(record)
    elif record_type == "product":
        return validate_product(record)
    elif record_type == "transaction":
        return validate_transaction(record)
    elif record_type == "erasure":
        return validate_erasure_request(record)
    else:
        return [f"Unknown record type: {record_type}"]
