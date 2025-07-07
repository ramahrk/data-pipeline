"""
Transaction data processor module with Prometheus Pushgateway and HTTP server support.
"""

import gzip
import json
import os
import time as pytime

from prometheus_client import (CollectorRegistry, Counter, Histogram,
                               push_to_gateway, start_http_server)

from src.config import (OUTPUT_PATH, PROMETHEUS_PUSHGATEWAY, QUARANTINE_PATH,
                        REFERENCE_DATA_PATH)
from src.metrics import (TRANSACTIONS_INVALID_COUNTER,
                         TRANSACTIONS_PROCESSED_COUNTER,
                         TRANSACTIONS_PROCESSING_DURATION,
                         TRANSACTIONS_VALID_COUNTER, etl_errors,
                         etl_records_processed,
                         etl_records_processed_pushgateway, registry)
from src.utils.logging import log_processing_stats, setup_logger
from src.utils.validation import validate_transaction

# Set up logger
logger = setup_logger(__name__)

# Environment variable to disable Prometheus metrics (both HTTP server & Pushgateway)
DISABLE_METRICS_PUSH = os.getenv("DISABLE_METRICS_PUSH", "0") == "1"


# Start Prometheus HTTP server for scraping metrics if not disabled
# if not DISABLE_METRICS_PUSH:
#     try:
#         start_http_server(8002, registry=registry)
#         logger.info("Prometheus metrics HTTP server started on port 8002")
#     except Exception as e:
#         logger.warning(f"Could not start Prometheus metrics HTTP server: {e}")


def normalize_transaction(transaction):
    # (Keep your existing normalization code here, exactly as before)
    if all(
        field in transaction
        for field in ["transaction_id", "customer_id", "sku", "quantity", "total_cost"]
    ):
        return transaction

    normalized = {}

    if "transaction_id" in transaction:
        normalized["transaction_id"] = transaction["transaction_id"]
    elif "id" in transaction:
        normalized["transaction_id"] = transaction["id"]
    else:
        for key, value in transaction.items():
            if isinstance(value, str) and (
                key.endswith("id") or key.endswith("_id") or key == "id"
            ):
                normalized["transaction_id"] = value
                break
        else:
            return None

    if "customer_id" in transaction:
        normalized["customer_id"] = transaction["customer_id"]
    elif (
        "customer" in transaction
        and isinstance(transaction["customer"], dict)
        and "id" in transaction["customer"]
    ):
        normalized["customer_id"] = transaction["customer"]["id"]
    elif "user_id" in transaction:
        normalized["customer_id"] = transaction["user_id"]
    else:
        for key, value in transaction.items():
            if isinstance(value, str) and (
                key.startswith("customer") or key.startswith("user")
            ):
                normalized["customer_id"] = value
                break
        else:
            normalized["customer_id"] = "unknown"

    if (
        "purchases" in transaction
        and "products" in transaction["purchases"]
        and transaction["purchases"]["products"]
    ):
        products = transaction["purchases"]["products"]
        if products and isinstance(products, list) and len(products) > 0:
            product = products[0]
            normalized["sku"] = (
                product.get("sku") or product.get("product_id") or "unknown"
            )
            normalized["quantity"] = product.get("quantity", 1)
            if "total" in product:
                normalized["total_cost"] = product["total"]
            elif "price" in product:
                normalized["total_cost"] = product["price"] * normalized["quantity"]
            else:
                normalized["total_cost"] = 0
    elif "items" in transaction and transaction["items"]:
        items = transaction["items"]
        if items and isinstance(items, list) and len(items) > 0:
            item = items[0]
            normalized["sku"] = item.get("sku") or item.get("product_id") or "unknown"
            normalized["quantity"] = item.get("quantity", 1)
            if "total" in item:
                normalized["total_cost"] = item["total"]
            elif "price" in item:
                normalized["total_cost"] = item["price"] * normalized["quantity"]
            else:
                normalized["total_cost"] = 0
    elif "sku" in transaction:
        normalized["sku"] = transaction["sku"]
        normalized["quantity"] = transaction.get("quantity", 1)
        normalized["total_cost"] = transaction.get(
            "total_cost", transaction.get("price", 0)
        )
    else:
        return None

    return normalized


def push_metrics_to_gateway(stats):
    try:
        push_to_gateway(
            PROMETHEUS_PUSHGATEWAY,
            job="etl_transaction_processing",
            registry=registry,
        )
        logger.info("Metrics pushed to Prometheus Pushgateway")
    except Exception as e:
        logger.error(f"Failed to push metrics to Pushgateway: {e}")


def process_transactions(
    input_file, source_file=None, products_by_sku=None, customers_by_id=None
):
    logger.info(
        f"Processing transactions from {input_file}, source file: {source_file}"
    )
    start_time = pytime.time()

    products_by_sku = products_by_sku or {}
    customers_by_id = customers_by_id or {}

    source_path = source_file or input_file
    normalized_path = source_path.replace("\\", "/")
    path_parts = normalized_path.split("/")

    date_parts = [p for p in path_parts if p.startswith("date=")]
    hour_parts = [p for p in path_parts if p.startswith("hour=")]

    if not date_parts or not hour_parts:
        logger.error(
            f"Input file path missing 'date=' or 'hour=' folder: {source_path}"
        )
        date_part = "date=unknown"
        hour_part = "hour=unknown"
    else:
        date_part = date_parts[0]
        hour_part = hour_parts[0]

    date = date_part.split("=")[1] if "=" in date_part else "unknown"
    hour = hour_part.split("=")[1] if "=" in hour_part else "unknown"

    output_dir = os.path.join(OUTPUT_PATH, date_part, hour_part)
    os.makedirs(output_dir, exist_ok=True)

    quarantine_dir = os.path.join(QUARANTINE_PATH, date_part, hour_part)
    os.makedirs(quarantine_dir, exist_ok=True)

    stats = {"processed": 0, "valid": 0, "invalid": 0, "processing_time": 0}

    valid_transactions = []
    invalid_transactions = []
    all_validation_errors = []

    with TRANSACTIONS_PROCESSING_DURATION.time():
        with gzip.open(input_file, "rt") as f:
            for line_num, line in enumerate(f):
                stats["processed"] += 1
                TRANSACTIONS_PROCESSED_COUNTER.inc()

                try:
                    transaction = json.loads(line.strip())

                    if line_num == 0:
                        logger.info(
                            f"Transaction structure: {json.dumps(transaction, indent=2)}"
                        )
                        logger.info(f"Transaction keys: {list(transaction.keys())}")

                    normalized_transaction = normalize_transaction(transaction)
                    normalized_transaction["_source_file"] = source_path

                    if normalized_transaction:
                        validation_errors = validate_transaction(
                            normalized_transaction,
                            products_by_sku=products_by_sku,
                            customers_by_id=customers_by_id,
                        )

                        if validation_errors:
                            normalized_transaction["_errors"] = validation_errors
                            invalid_transactions.append(normalized_transaction)
                            all_validation_errors.extend(validation_errors)
                            stats["invalid"] += 1
                            TRANSACTIONS_INVALID_COUNTER.inc()
                            logger.warning(
                                f"Invalid transaction record: {normalized_transaction.get('transaction_id', 'unknown')}"
                            )
                        else:
                            valid_transactions.append(normalized_transaction)
                            stats["valid"] += 1
                            TRANSACTIONS_VALID_COUNTER.inc()
                    else:
                        transaction["_errors"] = [
                            "Could not normalize transaction structure"
                        ]
                        invalid_transactions.append(transaction)
                        stats["invalid"] += 1
                        logger.warning(
                            f"Invalid transaction structure: {transaction.get('transaction_id', transaction.get('id', 'unknown'))}"
                        )

                except json.JSONDecodeError:
                    stats["invalid"] += 1
                    logger.error(
                        f"Invalid JSON in transaction record at line {stats['processed']}"
                    )
                except Exception as e:
                    stats["invalid"] += 1
                    logger.error(f"Error processing transaction record: {str(e)}")

    output_file = os.path.join(output_dir, "transactions.json.gz")
    with gzip.open(output_file, "wt") as f:
        for transaction in valid_transactions:
            f.write(json.dumps(transaction) + "\n")

    if invalid_transactions:
        quarantine_file = os.path.join(quarantine_dir, "transactions_invalid.json.gz")
        logger.warning("Invalid transactions found, moving to quarantine.")
        with gzip.open(quarantine_file, "wt") as f:
            for transaction in invalid_transactions:
                f.write(json.dumps(transaction) + "\n")
    if not valid_transactions:
        logger.warning("Valid transactions is empty.")

    if all_validation_errors and stats["invalid"] < 5:
        logger.debug(f"Validation errors (sample): {all_validation_errors[:5]}")

    stats["processing_time"] = pytime.time() - start_time

    if not DISABLE_METRICS_PUSH:
        push_metrics_to_gateway(stats)

    log_processing_stats("transactions", stats)

    stats_file = os.path.join(output_dir, "transactions_stats.json")
    with open(stats_file, "w") as f:
        json.dump(stats, f, indent=2)

    return stats
