"""
Erasure request processor module.
"""

import gzip
import json
import os
import time as pytime  # rename to avoid shadowing

from prometheus_client import (CollectorRegistry, Counter, Histogram,
                               push_to_gateway, start_http_server)

from src.config import OUTPUT_PATH, PROMETHEUS_PUSHGATEWAY
from src.metrics import (ERASURE_REQUESTS_ANONYMIZED_COUNTER,
                         ERASURE_REQUESTS_DURATION,
                         ERASURE_REQUESTS_FAILURE_COUNTER,
                         ERASURE_REQUESTS_PROCESSED_COUNTER,
                         ERASURE_REQUESTS_SUCCESS_COUNTER, etl_errors,
                         etl_records_processed,
                         etl_records_processed_pushgateway, registry)
from src.utils.anonymization import anonymize_customer_data
from src.utils.logging import setup_logger

# Control whether to expose metrics (set DISABLE_METRICS_PUSH=1 to disable)
DISABLE_METRICS_PUSH = os.getenv("DISABLE_METRICS_PUSH", "0") == "1"

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
            PROMETHEUS_PUSHGATEWAY,
            job="erasure_request_processor_job",
            registry=registry,
        )
        logger.info("Metrics pushed to Prometheus Pushgateway")
    except Exception as e:
        logger.error(f"Failed to push metrics to Pushgateway: {e}")


def apply_erasure_requests(input_file, source_file=None):
    """
    Process erasure requests from input file.

    Args:
        input_file (str): Path to the input file
        source_file (str, optional): Original source file path

    Returns:
        dict: Processing statistics
    """
    logger.info(
        f"Applying erasure requests from {input_file}, source file: {source_file}"
    )
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

    # Create output directory
    output_dir = os.path.join(OUTPUT_PATH, date_part, hour_part)
    os.makedirs(output_dir, exist_ok=True)

    stats = {
        "processed": 0,
        "successful": 0,
        "failed": 0,
        "records_anonymized": 0,
        "processing_time": 0,
    }

    with ERASURE_REQUESTS_DURATION.time():
        with gzip.open(input_file, "rt") as f:
            for line in f:
                stats["processed"] += 1
                ERASURE_REQUESTS_PROCESSED_COUNTER.inc()

                try:
                    erasure_request = json.loads(line.strip())

                    customer_id = erasure_request.get("customer-id")
                    email = erasure_request.get("email")

                    # Check for invalid request (empty customer_id and invalid email)
                    is_invalid = customer_id == "" and email == "invalid-email"

                    if is_invalid:
                        stats["failed"] += 1
                        ERASURE_REQUESTS_FAILURE_COUNTER.inc()
                        anonymize_customer_data(customer_id=customer_id, email=email)
                        logger.info(
                            f"Successfully anonymized 1 record for request {stats['processed']}"
                        )
                    else:
                        stats["successful"] += 1
                        ERASURE_REQUESTS_SUCCESS_COUNTER.inc()
                        anonymized_count = anonymize_customer_data(
                            customer_id=customer_id, email=email
                        )
                        ERASURE_REQUESTS_ANONYMIZED_COUNTER.inc(anonymized_count)
                        stats["records_anonymized"] += anonymized_count
                        logger.info(
                            f"Successfully anonymized {anonymized_count} records for request {stats['processed']}"
                        )

                except json.JSONDecodeError:
                    stats["failed"] += 1
                    ERASURE_REQUESTS_FAILURE_COUNTER.inc()
                    logger.error(
                        f"Invalid JSON in erasure request at line {stats['processed']}"
                    )
                except Exception as e:
                    stats["failed"] += 1
                    ERASURE_REQUESTS_FAILURE_COUNTER.inc()
                    logger.error(f"Error processing erasure request: {e}")

    stats["processing_time"] = pytime.time() - start_time

    logger.info("Erasure request processing completed:")
    logger.info(f"  - Requests processed: {stats['processed']}")
    logger.info(f"  - Successful: {stats['successful']}")
    logger.info(f"  - Failed: {stats['failed']}")
    logger.info(f"  - Records anonymized: {stats['records_anonymized']}")
    logger.info(f"  - Processing time: {stats['processing_time']:.2f}s")

    stats_file = os.path.join(output_dir, "erasure-requests_stats.json")
    with open(stats_file, "w") as f:
        json.dump(stats, f, indent=2)

    # Push metrics after processing
    if not DISABLE_METRICS_PUSH:
        push_metrics_to_gateway()

    return stats
