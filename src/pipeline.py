"""
Main ETL pipeline orchestration module.
"""

import argparse
import json
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Optional

from prometheus_client import (CollectorRegistry, Counter, Gauge,
                               push_to_gateway, start_http_server)

from src.config import (INPUT_PATH, OUTPUT_PATH, PROMETHEUS_PUSHGATEWAY,
                        REFERENCE_DATA_PATH)
from src.metrics import (etl_errors, etl_records_processed,
                         etl_records_processed_pushgateway,
                         pushgateway_registry, registry)
from src.processors.customer import process_customers
from src.processors.erasure import apply_erasure_requests
from src.processors.product import process_products
from src.processors.transaction import process_transactions
from src.utils.data_access import (load_all_products_for_date,
                                   load_customers_by_id, load_products_by_sku)
from src.utils.logging import setup_logger

DISABLE_METRICS_PUSH = os.getenv("DISABLE_METRICS_PUSH", "0") == "1"


# Set up logger
logger = setup_logger(__name__)
# Define custom counters/gauges globally


def start_metrics_server(port=8002):
    """
    Start Prometheus metrics server for monitoring.
    """
    if is_metrics_push_disabled():
        return
    try:
        start_http_server(port, registry=registry)
        logger.info(f"✅ Prometheus metrics server started on port {port}")
    except OSError as e:
        logger.warning(f"⚠️ Could not start Prometheus metrics server: {e}")


# Push gateway setup


def scan_for_new_data(base_path, date, hour=None):
    """
    Scan for new data files in the specified date/hour directory.

    Args:
        base_path (str): Base directory path
        date (str): Date in YYYY-MM-DD format
        hour (int, optional): Hour (0-23) or None for all hours

    Returns:
        dict: Dictionary mapping dataset names to file paths
    """
    path = os.path.join(base_path, f"date={date}")
    if not os.path.exists(path):
        logger.warning(f"Path does not exist: {path}")
        return {}

    available_files = {}

    # If hour is None, scan all hour directories
    if hour is None:
        # Look for directories with both formats: hour=0 and hour=00
        hour_dirs = [d for d in os.listdir(path) if d.startswith("hour=")]
        logger.info(f"Found hour directories: {hour_dirs}")

        for hour_dir in hour_dirs:
            hour_path = os.path.join(path, hour_dir)

            # Extract hour number
            try:
                hour_str = hour_dir.split("=")[1]
                hour_num = int(hour_str)

                # Scan for datasets in this hour directory
                for dataset in [
                    "customers",
                    "products",
                    "transactions",
                    "erasure-requests",
                ]:
                    file_path = os.path.join(hour_path, f"{dataset}.json.gz")
                    if os.path.exists(file_path):
                        available_files[f"{dataset}_{hour_num}"] = file_path
                        logger.info(
                            f"Found {dataset} file for hour {hour_num}: {file_path}"
                        )
            except ValueError:
                logger.warning(f"Invalid hour format: {hour_dir}")

        return available_files

    # Try both padded and unpadded hour formats
    hour_path = None

    # Try padded format first (hour=00)
    padded_hour_dir = f"hour={hour:02d}"
    padded_path = os.path.join(path, padded_hour_dir)

    # Try unpadded format (hour=0)
    unpadded_hour_dir = f"hour={hour}"
    unpadded_path = os.path.join(path, unpadded_hour_dir)

    # Check which format exists
    if os.path.exists(padded_path):
        hour_path = padded_path
        logger.info(f"Using padded hour path: {hour_path}")
    elif os.path.exists(unpadded_path):
        hour_path = unpadded_path
        logger.info(f"Using unpadded hour path: {hour_path}")
    else:
        logger.warning(f"Hour path does not exist for date={date}, hour={hour}")
        return {}

    # Scan for specific datasets in the hour directory
    for dataset in ["customers", "products", "transactions", "erasure-requests"]:
        file_path = os.path.join(hour_path, f"{dataset}.json.gz")
        if os.path.exists(file_path):
            available_files[dataset] = file_path
            logger.info(f"Found {dataset} file: {file_path}")

    return available_files


def needs_reference_data(dataset_name):
    """
    Check if reference data is needed for a dataset.

    Args:
        dataset_name (str): Name of the dataset

    Returns:
        bool: True if reference data is needed, False otherwise
    """
    if dataset_name == "products":
        # Check if we have product reference data
        ref_path = os.path.join(REFERENCE_DATA_PATH, "products.json.gz")
        return not os.path.exists(ref_path)
    return False


def generate_stats(date, hour=None):
    """
    Generate and log operational statistics for processed data.

    Args:
        date (str): Date in YYYY-MM-DD format
        hour (int, optional): Hour (0-23) or None for all hours
    """
    stats = {
        "date": date,
        "hour": hour,
        "timestamp": datetime.now().isoformat(),
        "datasets_processed": [],
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "anonymized_records": 0,
    }

    # Get stats from output directory
    output_dir = os.path.join(OUTPUT_PATH, f"date={date}")
    if hour is not None:
        output_dir = os.path.join(output_dir, f"hour={hour}")
    else:
        # If processing all hours, look in each hour directory
        for h in range(24):
            # Try both formats: with and without leading zero
            hour_formats = [f"hour={h}", f"hour={h:02d}"]

            for hour_format in hour_formats:
                hour_dir = os.path.join(output_dir, hour_format)
                if os.path.exists(hour_dir):
                    # Check for stats files in this hour directory
                    for dataset in [
                        "customers",
                        "products",
                        "transactions",
                        "erasure-requests",
                    ]:
                        stats_file = os.path.join(hour_dir, f"{dataset}_stats.json")
                        if os.path.exists(stats_file):
                            with open(stats_file, "r") as f:
                                try:
                                    dataset_stats = json.load(f)
                                    logger.debug(
                                        f"Loaded stats for dataset {dataset}: {dataset_stats}"
                                    )

                                    if dataset not in stats["datasets_processed"]:
                                        stats["datasets_processed"].append(dataset)
                                    stats["total_records"] += dataset_stats.get(
                                        "processed", 0
                                    )
                                    stats["valid_records"] += dataset_stats.get(
                                        "valid", 0
                                    )
                                    stats["invalid_records"] += dataset_stats.get(
                                        "invalid", 0
                                    )

                                    # Check for anonymization in both customer and erasure request stats
                                    if dataset == "customers":
                                        stats[
                                            "anonymized_records"
                                        ] += dataset_stats.get("anonymized", 0)
                                    elif dataset == "erasure-requests":
                                        stats[
                                            "anonymized_records"
                                        ] += dataset_stats.get("records_anonymized", 0)
                                except json.JSONDecodeError:
                                    logger.warning(
                                        f"Invalid JSON in stats file: {stats_file}"
                                    )

    # If processing a specific hour, check that hour directory
    if hour is not None:
        # output_dir points to date folder only
        date_dir = output_dir  # rename for clarity

        found_hour_dir = False
        for hour_dir_name in [f"hour={hour}", f"hour={hour:02d}"]:
            hour_dir_path = os.path.join(date_dir, hour_dir_name)
            logger.debug(f"Checking hour directory: {hour_dir_path}")
            if os.path.exists(hour_dir_path):
                found_hour_dir = True
                for dataset in [
                    "customers",
                    "products",
                    "transactions",
                    "erasure-requests",
                ]:
                    stats_file = os.path.join(hour_dir_path, f"{dataset}_stats.json")
                    if os.path.exists(stats_file):
                        with open(stats_file, "r") as f:
                            try:
                                dataset_stats = json.load(f)
                                if dataset not in stats["datasets_processed"]:
                                    stats["datasets_processed"].append(dataset)
                                stats["total_records"] += dataset_stats.get(
                                    "processed", 0
                                )
                                stats["valid_records"] += dataset_stats.get("valid", 0)
                                stats["invalid_records"] += dataset_stats.get(
                                    "invalid", 0
                                )
                                # Track anonymized records if applicable
                                if dataset == "customers":
                                    stats["anonymized_records"] += dataset_stats.get(
                                        "anonymized", 0
                                    )
                                elif dataset == "erasure-requests":
                                    stats["anonymized_records"] += dataset_stats.get(
                                        "records_anonymized", 0
                                    )
                            except json.JSONDecodeError:
                                logger.warning(
                                    f"Invalid JSON in stats file: {stats_file}"
                                )
                break
        if not found_hour_dir:
            logger.warning(f"No hour directory found for date={date}, hour={hour}")

    # Log the aggregated stats
    logger.info(
        f"Processing summary for date={date}, hour={hour if hour is not None else 'all'}:"
    )
    logger.info(f"  - Datasets processed: {', '.join(stats['datasets_processed'])}")
    logger.info(f"  - Total records: {stats['total_records']}")
    logger.info(f"  - Valid records: {stats['valid_records']}")
    logger.info(f"  - Invalid records: {stats['invalid_records']}")
    logger.info(f"  - Anonymized records: {stats['anonymized_records']}")

    if not is_metrics_push_disabled():
        # Push to gateway with labels
        push_to_gateway(
            PROMETHEUS_PUSHGATEWAY,
            job="etl_pipeline",
            registry=pushgateway_registry,
            grouping_key={"instance": "data-pipeline-etl-new"},
        )

    # Write stats to file
    stats_dir = os.path.join(OUTPUT_PATH, "stats")
    os.makedirs(stats_dir, exist_ok=True)

    stats_file = os.path.join(
        stats_dir, f"stats_{date}_{hour if hour is not None else 'all'}.json"
    )
    with open(stats_file, "w") as f:
        json.dump(stats, f, indent=2)


def find_daily_product_file(date: str) -> Optional[str]:
    """Search all hour directories for the product file for the given date."""
    base_path = os.path.join(INPUT_PATH, f"date={date}")
    for h in range(24):
        for fmt in [f"hour={h}", f"hour={h:02d}"]:
            candidate = os.path.join(base_path, fmt, "products.json.gz")
            if os.path.exists(candidate):
                return candidate
    return None


def process_data_batch(date, hour=None, base_path=INPUT_PATH):
    """
    Process a batch of data for a specific date and hour.

    Args:
        date (str): Date in YYYY-MM-DD format
        hour (int, optional): Hour (0-23) or None to process all hours
        base_path (str): Base directory path where input data is located

    Returns:
        None
    """
    logger.info(
        f"Processing data for date={date}, hour={hour if hour is not None else 'all'}"
    )

    # Find available datasets
    available_files = scan_for_new_data(base_path, date, hour)
    # 1. Load all products for the date once at start
    products_by_sku = load_all_products_for_date(date)

    if not available_files:
        logger.warning(
            f"No data files found for date={date}, hour={hour if hour is not None else 'all'}"
        )
        generate_stats(date, hour)
        return

    # 2. Process products files from available_files (optional, to update reference data)
    product_files = [
        f
        for k, f in available_files.items()
        if k == "products" or k.startswith("products_")
    ]
    for product_file in product_files:
        logger.info(f"Processing products from {product_file}")
        try:
            process_products(product_file)
        except Exception as e:
            logger.error(f"Failed to process products from {product_file}: {e}")
            etl_errors.inc()
        finally:
            etl_records_processed.inc()
            etl_records_processed_pushgateway.inc()

    # 3. Load all customers. If hour specified, process that hour only, else all 24 hours
    hours_to_process = [hour] if hour is not None else range(24)
    base_date_path = os.path.join(base_path, f"date={date}")
    customers_by_id = {}  # Initialize dictionary before loop
    for h in hours_to_process:
        # find customer files for this hour
        for hour_fmt in [f"hour={h}", f"hour={h:02d}"]:
            customer_file = os.path.join(base_date_path, hour_fmt, "customers.json.gz")
            if os.path.exists(customer_file):
                logger.info(f"Processing customers from {customer_file}")
                try:
                    process_customers(customer_file)
                    # Load processed customers for this hour and merge
                    hourly_customers = load_customers_by_id(customer_file)
                    customers_by_id.update(hourly_customers)
                except Exception as e:
                    logger.error(
                        f"Failed processing customers for {customer_file}: {e}"
                    )
                    etl_errors.inc()

    # 4. Process transactions, passing full products_by_sku dict loaded earlier
    transaction_files = [
        f
        for k, f in available_files.items()
        if k == "transactions" or k.startswith("transactions_")
    ]
    for transaction_file in transaction_files:
        logger.info(f"Processing transactions from {transaction_file}")
        try:
            process_transactions(
                transaction_file,
                products_by_sku=products_by_sku,
                customers_by_id=customers_by_id,
            )
        except Exception as e:
            logger.error(f"Failed to process transactions from {transaction_file}: {e}")
            etl_errors.inc()
        finally:
            etl_records_processed.inc()
            etl_records_processed_pushgateway.inc()

    # 5. Apply erasure requests from the PREVIOUS DAY
    prev_date = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    erasure_files = []

    for h in range(24):  # Search all 24 hours
        for hour_fmt in [f"hour={h}", f"hour={h:02d}"]:
            erasure_file_path = os.path.join(
                base_path, f"date={prev_date}", hour_fmt, "erasure-requests.json.gz"
            )
            if os.path.exists(erasure_file_path):
                erasure_files.append(erasure_file_path)

    for erasure_file in erasure_files:
        logger.info(f"Applying erasure requests from {erasure_file}")
        try:
            apply_erasure_requests(erasure_file)
        except Exception as e:
            logger.error(f"Failed to apply erasure requests from {erasure_file}: {e}")
            etl_errors.inc()
        finally:
            etl_records_processed.inc()
            etl_records_processed_pushgateway.inc()

    # Generate operational statistics
    generate_stats(date, hour)


def run_pipeline(start_date=None, end_date=None, specific_hour=None):
    """
    Run the ETL pipeline for a date range.

    Args:
        start_date (str, optional): Start date in YYYY-MM-DD format, defaults to today
        end_date (str, optional): End date in YYYY-MM-DD format, defaults to start_date
        specific_hour (int, optional): Process only a specific hour (0-23), or None for all hours
    """
    if not start_date:
        start_date = datetime.now().strftime("%Y-%m-%d")
    if not end_date:
        end_date = start_date

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    logger.info(f"Starting pipeline run from {start_date} to {end_date}")

    current_dt = start_dt
    while current_dt <= end_dt:
        current_date = current_dt.strftime("%Y-%m-%d")

        if specific_hour is not None:
            process_data_batch(current_date, specific_hour)
        else:
            process_data_batch(current_date)

        current_dt += timedelta(days=1)


def is_metrics_push_disabled(args=None):
    if args is None:
        return os.getenv("DISABLE_METRICS_PUSH", "0") == "1"
    return (
        os.getenv("DISABLE_METRICS_PUSH", "0") == "1"
        or getattr(args, "ci_mode", False)
        or getattr(args, "disable_metrics_push", False)
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run the ETL pipeline")
    parser.add_argument("--start-date", help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", help="End date in YYYY-MM-DD format")
    parser.add_argument("--hour", type=int, help="Process only a specific hour (0-23)")
    parser.add_argument(
        "--metrics-port", type=int, help="Port to expose Prometheus metrics"
    )
    parser.add_argument(
        "--ci-mode", action="store_true", help="Run synchronously for CI"
    )
    parser.add_argument(
        "--disable-metrics-push",
        action="store_true",
        help="Disable Prometheus metrics push",
    )

    args = parser.parse_args()
    DISABLE_METRICS_PUSH = is_metrics_push_disabled(args)

    if args.ci_mode:
        logger.info("Running in CI mode, disabling metrics push")
        run_pipeline(args.start_date, args.end_date, args.hour)

    else:
        # Start Prometheus metrics server if port provided
        if args.metrics_port:
            start_metrics_server(args.metrics_port)

        thread = threading.Thread(
            target=run_pipeline,
            args=(args.start_date, args.end_date, args.hour),
            daemon=True,
        )
        thread.start()

        logger.info("Pipeline thread started, keeping process alive...")

        try:
            while thread.is_alive():
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Pipeline interrupted and shutting down.")
