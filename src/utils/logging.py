"""
Logging utilities for the ETL pipeline.
"""

import json
import logging
import os
import time
from datetime import datetime
from functools import wraps

from src.config import LOG_FILE, LOG_LEVEL


# Configure logging
def setup_logger(name):
    """
    Set up a logger with the specified name.

    Args:
        name (str): Logger name

    Returns:
        logging.Logger: Configured logger
    """
    logger = logging.getLogger(name)

    # Only configure if not already configured
    if not logger.handlers:
        logger.setLevel(getattr(logging, LOG_LEVEL))

        # Create directory for log file if it doesn't exist
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

        # File handler
        file_handler = logging.FileHandler(LOG_FILE)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    return logger


# Set up logger for this module
logger = setup_logger(__name__)


def log_processing_stats(dataset_name, stats):
    """
    Log processing statistics for a dataset.

    Args:
        dataset_name (str): Name of the dataset
        stats (dict): Statistics dictionary
    """
    logger.info(f"Processing completed for {dataset_name}:")
    logger.info(f"  - Records processed: {stats.get('processed', 0)}")
    logger.info(f"  - Records valid: {stats.get('valid', 0)}")
    logger.info(f"  - Records invalid: {stats.get('invalid', 0)}")

    if "anonymized" in stats:
        logger.info(f"  - Records anonymized: {stats.get('anonymized', 0)}")

    logger.info(f"  - Processing time: {stats.get('processing_time', 0):.2f}s")


def log_execution_time(func):
    """
    Decorator to log the execution time of a function.

    Args:
        func (callable): Function to decorate

    Returns:
        callable: Decorated function
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.debug(f"Starting {func.__name__}")

        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.debug(f"Completed {func.__name__} in {execution_time:.2f}s")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(
                f"Error in {func.__name__} after {execution_time:.2f}s: {str(e)}"
            )
            raise

    return wrapper


def log_error(error_type, message, details=None):
    """
    Log an error with structured details.

    Args:
        error_type (str): Type of error
        message (str): Error message
        details (dict, optional): Additional error details
    """
    error_data = {
        "timestamp": datetime.now().isoformat(),
        "error_type": error_type,
        "message": message,
    }

    if details:
        error_data["details"] = details

    logger.error(f"Error: {error_type} - {message}")

    if details:
        logger.debug(f"Error details: {json.dumps(details)}")


def log_validation_error(dataset, record_id, errors):
    """
    Log a validation error.

    Args:
        dataset (str): Dataset name
        record_id (str): Record ID
        errors (list): List of validation errors
    """
    log_error(
        error_type="ValidationError",
        message=f"Validation failed for {dataset} record {record_id}",
        details={"dataset": dataset, "record_id": record_id, "errors": errors},
    )
