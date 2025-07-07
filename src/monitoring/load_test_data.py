"""
Test monitoring with real data from /data/input.
"""

import json
import logging
import os
import subprocess
import time
from datetime import datetime

import requests

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def find_test_dates():
    """Find dates in test data directory."""
    dates = []
    base_path = "data/input"

    if not os.path.exists(base_path):
        logger.error(f"Test data directory {base_path} does not exist")
        return dates

    for item in os.listdir(base_path):
        if item.startswith("date="):
            date = item.split("=")[1]
            dates.append(date)

    return sorted(dates)


def run_pipeline_with_metrics(date):
    """Run the pipeline for a specific date and check metrics."""
    logger.info(f"Processing data for date: {date}")

    # Run the pipeline
    cmd = ["python", "-m", "src.pipeline", "--start-date", date, "--end-date", date]
    process = subprocess.run(cmd, capture_output=True, text=True)

    if process.returncode != 0:
        logger.error(f"Pipeline failed with exit code {process.returncode}")
        logger.error(f"Stdout: {process.stdout}")
        logger.error(f"Stderr: {process.stderr}")
        return False

    logger.info(f"Pipeline completed successfully for date {date}")
    logger.info(process.stdout)

    # Wait for metrics to be scraped
    logger.info("Waiting for Prometheus to scrape metrics...")
    time.sleep(10)

    # Query Prometheus for metrics
    logger.info("Querying Prometheus for metrics...")
    try:
        # Check for records_processed_total metric
        response = requests.get(
            "http://prometheus:9090/api/v1/query?query=records_processed_total"
        )
        data = response.json()

        if data["status"] == "success" and len(data["data"]["result"]) > 0:
            logger.info("Found records_processed_total metric in Prometheus")
            for result in data["data"]["result"]:
                logger.info(f"Metric: {result['metric']}, Value: {result['value'][1]}")
            return True
        else:
            logger.error("records_processed_total metric not found in Prometheus")
            logger.error(f"Prometheus response: {json.dumps(data, indent=2)}")
            return False
    except Exception as e:
        logger.error(f"Error querying Prometheus: {str(e)}")
        return False


def main():
    """Main test function."""
    # Find test dates
    dates = find_test_dates()
    if not dates:
        logger.error("No test dates found in data/input")
        return False

    logger.info(f"Found test dates: {dates}")

    # Process each date
    success = True
    for date in dates:
        if not run_pipeline_with_metrics(date):
            success = False

    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
