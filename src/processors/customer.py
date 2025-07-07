"""
Customer data processor module.
"""

import gzip
import json
import os
import time as pytime  # rename to avoid shadowing

from prometheus_client import (CollectorRegistry, Counter, Histogram,
                               push_to_gateway, start_http_server)

from src.config import (OUTPUT_PATH, PROMETHEUS_PUSHGATEWAY, QUARANTINE_PATH,
                        REFERENCE_DATA_PATH)
from src.metrics import (MESSAGE_PROCESSED_COUNTER,
                         MESSAGE_PROCESSED_INVALID_COUNTER,
                         MESSAGE_PROCESSED_VALID_COUNTER,
                         MESSAGE_PROCESSING_DURATION, etl_errors,
                         etl_records_processed,
                         etl_records_processed_pushgateway, registry)
from src.utils.logging import log_processing_stats, setup_logger
from src.utils.validation import validate_customer

# Control whether to expose metrics (set DISABLE_METRICS_PUSH=1 to disable)
DISABLE_METRICS_PUSH = os.getenv("DISABLE_METRICS_PUSH", "0") == "1"


# Set up logger
logger = setup_logger(__name__)

# # Start Prometheus metrics server if metrics enabled
# if not DISABLE_METRICS_PUSH:
#     try:
#         start_http_server(8002, registry=registry)
#         logger.info("Prometheus metrics server started on port 8002")
#     except Exception as e:
#         logger.warning(f"Could not start Prometheus metrics server: {e}")


def push_metrics_to_gateway():
    """
    Push metrics to Prometheus Pushgateway.
    """
    try:
        push_to_gateway(
            PROMETHEUS_PUSHGATEWAY, job="customer_processor_job", registry=registry
        )
        logger.info("Metrics pushed to Prometheus Pushgateway")
    except Exception as e:
        logger.error(f"Failed to push metrics to Pushgateway: {e}")


def process_customers(input_file, source_file=None):
    """
    Process customer data from input file.

    Args:
        input_file (str): Path to the input file
        source_file (str, optional): Original source file path

    Returns:
        dict: Processing statistics
    """
    logger.info(f"Processing customers from {input_file}, source file: {source_file}")
    start_time = pytime.time()

    # Extract date and hour from source path (fall back to input_file if needed)
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

    # Create output directories
    output_dir = os.path.join(OUTPUT_PATH, date_part, hour_part)
    os.makedirs(output_dir, exist_ok=True)

    quarantine_dir = os.path.join(QUARANTINE_PATH, date_part, hour_part)
    os.makedirs(quarantine_dir, exist_ok=True)

    reference_dir = os.path.join(REFERENCE_DATA_PATH, "customers")
    os.makedirs(reference_dir, exist_ok=True)

    # Initialize statistics
    stats = {
        "processed": 0,
        "valid": 0,
        "invalid": 0,
        "anonymized": 0,
        "processing_time": 0,
    }

    valid_customers = []
    invalid_customers = []

    with MESSAGE_PROCESSING_DURATION.time():
        with gzip.open(input_file, "rt") as f:
            for line in f:
                stats["processed"] += 1
                MESSAGE_PROCESSED_COUNTER.inc()

                try:
                    customer = json.loads(line.strip())
                    validation_errors = validate_customer(customer)

                    if validation_errors:
                        customer["_errors"] = validation_errors
                        invalid_customers.append(customer)
                        stats["invalid"] += 1
                        MESSAGE_PROCESSED_INVALID_COUNTER.inc()
                        logger.warning(
                            f"Invalid customer record: {customer.get('id', 'unknown')}"
                        )
                    else:
                        valid_customers.append(customer)
                        stats["valid"] += 1
                        MESSAGE_PROCESSED_VALID_COUNTER.inc()

                    # Update reference data
                    if "id" in customer:
                        customer_file = os.path.join(
                            reference_dir, f"{customer['id']}.json"
                        )
                        with open(customer_file, "w") as ref_f:
                            json.dump(customer, ref_f)

                except json.JSONDecodeError:
                    stats["invalid"] += 1
                    MESSAGE_PROCESSED_INVALID_COUNTER.inc()
                    logger.error(
                        f"Invalid JSON in customer record at line {stats['processed']}"
                    )
                except Exception as e:
                    stats["invalid"] += 1
                    MESSAGE_PROCESSED_INVALID_COUNTER.inc()
                    logger.error(f"Error processing customer record: {e}")

    # Write valid customers to output
    output_file = os.path.join(output_dir, "customers.json.gz")
    with gzip.open(output_file, "wt") as f:
        for customer in valid_customers:
            f.write(json.dumps(customer) + "\n")

    # Write invalid customers to quarantine
    if invalid_customers:
        quarantine_file = os.path.join(quarantine_dir, "customers_invalid.json.gz")
        with gzip.open(quarantine_file, "wt") as f:
            for customer in invalid_customers:
                f.write(json.dumps(customer) + "\n")

    stats["processing_time"] = pytime.time() - start_time

    log_processing_stats("customers", stats)

    # Save stats to file
    stats_file = os.path.join(output_dir, "customers_stats.json")
    with open(stats_file, "w") as f:
        json.dump(stats, f, indent=2)

    # Push metrics after processing
    if not DISABLE_METRICS_PUSH:
        push_metrics_to_gateway()

    return stats


def update_customer(customer, reference_path=None):
    """
    Update a customer record in the reference data.

    Args:
        customer (dict): Updated customer data
        reference_path (str, optional): Override the reference data path

    Returns:
        bool: True if successful, False otherwise
    """
    if "id" not in customer:
        logger.error("Cannot update customer without ID")
        return False

    if reference_path is None:
        reference_path = REFERENCE_DATA_PATH

    reference_dir = os.path.join(reference_path, "customers")
    os.makedirs(reference_dir, exist_ok=True)

    customer_file = os.path.join(reference_dir, f"{customer['id']}.json")

    with open(customer_file, "w") as f:
        json.dump(customer, f)

    return True
