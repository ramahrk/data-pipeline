"""
Data access utilities for the ETL pipeline.
"""

import gzip
import json
import os

from src.config import (INPUT_PATH, OUTPUT_PATH, PROMETHEUS_PUSHGATEWAY,
                        REFERENCE_DATA_PATH)
from src.utils.logging import setup_logger

# Set up logger
logger = setup_logger(__name__)


def load_all_products_for_date(date):
    """
    Load and merge product data from all hourly product files for the given date.
    Returns a dict mapping SKU to product info.
    """
    products_by_sku = {}
    base_path = os.path.join(INPUT_PATH, f"date={date}")
    for h in range(24):
        for fmt in [f"hour={h}", f"hour={h:02d}"]:
            product_file = os.path.join(base_path, fmt, "products.json.gz")
            if os.path.exists(product_file):
                try:
                    products_by_sku.update(load_products_by_sku(product_file))
                except Exception as e:
                    logger.warning(
                        f"Failed to load product data from {product_file}: {e}"
                    )
    if not products_by_sku:
        logger.warning(f"No product reference files found for date={date}")
    return products_by_sku


# Helper function to load a specific customer's data
def load_customers_by_id(file_path: str):
    """
    Load customers from a specific processed file into a dictionary.
    Args:
        file_path (str): Full path to a single customers.json.gz
    """
    customers_by_id = {}
    if os.path.exists(file_path):
        with gzip.open(file_path, "rt") as f:
            for line in f:
                customer = json.loads(line.strip())
                if "id" in customer:
                    customers_by_id[customer["id"]] = customer
    return customers_by_id


def load_products_by_sku(product_reference_file):
    products_by_sku = {}
    with gzip.open(product_reference_file, "rt") as f:
        for line in f:
            product = json.loads(line.strip())
            sku = product.get("sku")
            if sku:
                products_by_sku[sku] = product
    return products_by_sku


def find_customer_by_id(customer_id, reference_path=None):
    """
    Find a customer by ID in the reference data.

    Args:
        customer_id (str): Customer ID to find
        reference_path (str, optional): Override the reference data path

    Returns:
        dict: Customer data or None if not found
    """
    if reference_path is None:
        reference_path = REFERENCE_DATA_PATH

    reference_dir = os.path.join(reference_path, "customers")
    if not os.path.exists(reference_dir):
        os.makedirs(reference_dir, exist_ok=True)

    customer_file = os.path.join(reference_dir, f"{customer_id}.json")

    if os.path.exists(customer_file):
        with open(customer_file, "r") as f:
            return json.load(f)

    return None


def find_customers_by_email(email, reference_path=None):
    """
    Find customers by email in the reference data.

    Args:
        email (str): Email to search for
        reference_path (str, optional): Override the reference data path

    Returns:
        list: List of customer records with matching email
    """
    if reference_path is None:
        reference_path = REFERENCE_DATA_PATH

    reference_dir = os.path.join(reference_path, "customers")
    if not os.path.exists(reference_dir):
        os.makedirs(reference_dir, exist_ok=True)

    matching_customers = []

    # This is inefficient for large datasets - in production we'd use a database or index
    if os.path.exists(reference_dir):
        for filename in os.listdir(reference_dir):
            if filename.endswith(".json"):
                customer_file = os.path.join(reference_dir, filename)
                with open(customer_file, "r") as f:
                    try:
                        customer = json.load(f)
                        if customer.get("email") == email:
                            matching_customers.append(customer)
                    except json.JSONDecodeError:
                        logger.warning(
                            f"Invalid JSON in customer file: {customer_file}"
                        )

    return matching_customers


def find_product_by_sku(sku, reference_path=None):
    """
    Find a product by SKU in the reference data.

    Args:
        sku (str): Product SKU to find
        reference_path (str, optional): Override the reference data path

    Returns:
        dict: Product data or None if not found
    """
    if reference_path is None:
        reference_path = REFERENCE_DATA_PATH

    reference_dir = os.path.join(reference_path, "products")
    if not os.path.exists(reference_dir):
        os.makedirs(reference_dir, exist_ok=True)

    product_file = os.path.join(reference_dir, f"{sku}.json")

    if os.path.exists(product_file):
        with open(product_file, "r") as f:
            return json.load(f)

    return None
