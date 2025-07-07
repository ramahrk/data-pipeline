"""
Product data processor module.
"""

import gzip
import json
import os
import time as pytime

from prometheus_client import (CollectorRegistry, Counter, Histogram,
                               push_to_gateway, start_http_server)

from src.config import (OUTPUT_PATH, PROMETHEUS_PUSHGATEWAY, QUARANTINE_PATH,
                        REFERENCE_DATA_PATH)
from src.metrics import (PRODUCTS_INVALID_COUNTER, PRODUCTS_PROCESSED_COUNTER,
                         PRODUCTS_PROCESSING_DURATION, PRODUCTS_VALID_COUNTER,
                         etl_errors, etl_records_processed,
                         etl_records_processed_pushgateway, registry)
from src.utils.data_access import find_product_by_sku
from src.utils.logging import log_processing_stats, setup_logger
from src.utils.validation import validate_product

logger = setup_logger(__name__)

# Control whether to expose metrics (set DISABLE_METRICS_PUSH=1 to disable)
DISABLE_METRICS_PUSH = os.getenv("DISABLE_METRICS_PUSH", "0") == "1"


# Start Prometheus metrics server if metrics enabled
# if not DISABLE_METRICS_PUSH:
#     try:
#         start_http_server(8002, registry=registry)
#         logger.info("Prometheus metrics server started on port 8002")
#     except Exception as e:
#         logger.warning(f"Could not start Prometheus metrics server: {e}")


def push_metrics_to_gateway():
    try:
        push_to_gateway(
            PROMETHEUS_PUSHGATEWAY, job="etl_product_processing", registry=registry
        )
        logger.info("Metrics pushed to Prometheus Pushgateway")
    except Exception as e:
        logger.error(f"Failed to push metrics to Pushgateway: {e}")


def process_products(input_file, source_file=None):
    logger.info(f"Processing products from {input_file}, source file: {source_file}")
    start_time = pytime.time()

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

    output_dir = os.path.join(OUTPUT_PATH, date_part, hour_part)
    os.makedirs(output_dir, exist_ok=True)

    quarantine_dir = os.path.join(QUARANTINE_PATH, date_part, hour_part)
    os.makedirs(quarantine_dir, exist_ok=True)

    reference_dir = os.path.join(REFERENCE_DATA_PATH, "products")
    os.makedirs(reference_dir, exist_ok=True)

    stats = {"processed": 0, "valid": 0, "invalid": 0, "processing_time": 0}

    valid_products = []
    invalid_products = []

    with PRODUCTS_PROCESSING_DURATION.time():
        with gzip.open(input_file, "rt") as f:
            for line in f:
                stats["processed"] += 1
                etl_records_processed.inc()
                etl_records_processed_pushgateway.inc()
                PRODUCTS_PROCESSED_COUNTER.inc()
                try:
                    product = json.loads(line.strip())

                    validation_errors = validate_product(product)

                    if validation_errors:
                        product["_errors"] = validation_errors
                        invalid_products.append(product)
                        stats["invalid"] += 1
                        PRODUCTS_INVALID_COUNTER.inc()
                        logger.warning(
                            f"Invalid product record: {product.get('sku', 'unknown')}"
                        )
                    else:
                        valid_products.append(product)
                        stats["valid"] += 1
                        PRODUCTS_VALID_COUNTER.inc()
                        etl_records_processed.inc()
                        etl_records_processed_pushgateway.inc()

                except json.JSONDecodeError:
                    stats["invalid"] += 1
                    PRODUCTS_INVALID_COUNTER.inc()
                    logger.error(
                        f"Invalid JSON in product record at line {stats['processed']}"
                    )
                except Exception as e:
                    stats["invalid"] += 1
                    PRODUCTS_INVALID_COUNTER.inc()
                    logger.error(f"Error processing product record: {str(e)}")

        output_file = os.path.join(output_dir, "products.json.gz")
        with gzip.open(output_file, "wt") as f:
            for product in valid_products:
                f.write(json.dumps(product) + "\n")

        if invalid_products:
            quarantine_file = os.path.join(quarantine_dir, "products_invalid.json.gz")
            with gzip.open(quarantine_file, "wt") as f:
                for product in invalid_products:
                    f.write(json.dumps(product) + "\n")

        os.makedirs(REFERENCE_DATA_PATH, exist_ok=True)

        try:
            reference_file = os.path.join(REFERENCE_DATA_PATH, "products.json.gz")
            with gzip.open(reference_file, "wt") as f:
                for product in valid_products:
                    f.write(json.dumps(product) + "\n")
        except Exception as e:
            logger.warning(f"Failed to write merged product reference file: {e}")

    stats["processing_time"] = pytime.time() - start_time

    # Push metrics after processing
    if not DISABLE_METRICS_PUSH:
        push_metrics_to_gateway()

    log_processing_stats("products", stats)

    stats_file = os.path.join(output_dir, "products_stats.json")
    with open(stats_file, "w") as f:
        json.dump(stats, f, indent=2)

    return stats
