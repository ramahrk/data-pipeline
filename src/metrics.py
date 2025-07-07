# src/metrics.py

import logging

# Default registry (for HTTP server export)
from prometheus_client import REGISTRY as default_registry
from prometheus_client import (CollectorRegistry, Counter, Gauge, Histogram,
                               push_to_gateway)

from src.config import PROMETHEUS_PUSHGATEWAY

registry = CollectorRegistry()

# Custom registry for pushgateway
pushgateway_registry = CollectorRegistry()


logger = logging.getLogger(__name__)

# ===================
# Generic ETL Metrics
# ===================
etl_records_processed = Counter(
    "etl_records_processed_total",
    "Number of records processed",
    registry=pushgateway_registry,
)
etl_records_processed_pushgateway = Counter(
    "etl_records_processed_total_pushgateway",
    "Number of records processed",
    registry=pushgateway_registry,
)
etl_errors = Counter(
    "etl_errors_total", "Total ETL errors", registry=pushgateway_registry
)
etl_records_anonymized = Gauge(
    "etl_records_anonymized_total",
    "Number of records anonymized",
    registry=pushgateway_registry,
)

# ====================
# Kafka Processing Metrics
# ====================
MESSAGE_CONSUMED_COUNTER = Counter(
    "kafka_messages_consumed_total",
    "Total Kafka messages consumed",
    registry=pushgateway_registry,
)

MESSAGE_PROCESSED_COUNTER = Counter(
    "kafka_messages_processed_total",
    "Total Kafka messages processed",
    registry=pushgateway_registry,
)

MESSAGE_PROCESSED_VALID_COUNTER = Counter(
    "kafka_valid_total",
    "Total valid Kafka messages",
    registry=pushgateway_registry,
)

MESSAGE_PROCESSED_INVALID_COUNTER = Counter(
    "kafka_invalid_total",
    "Total invalid Kafka messages",
    registry=pushgateway_registry,
)

MESSAGE_PROCESSING_DURATION = Histogram(
    "kafka_message_processing_duration_seconds",
    "Kafka message processing duration in seconds",
    registry=pushgateway_registry,
)

MESSAGE_PRODUCTION_DURATION = Histogram(
    "message_production_duration_seconds", "Time taken to produce messages to Kafka"
)

MESSAGES_PRODUCED = Counter(
    "kafka_messages_produced_total",
    "Total Kafka messages produced",
    registry=pushgateway_registry,
)

MESSAGES_FAILED = Counter(
    "kafka_messages_failed_total",
    "Total Kafka message produce failures",
    registry=pushgateway_registry,
)

# ====================
# Product Processing Metrics
# ====================
PRODUCTS_PROCESSED_COUNTER = Counter(
    "products_processed_total",
    "Total product records processed",
    registry=pushgateway_registry,
)
PRODUCTS_VALID_COUNTER = Counter(
    "products_valid_total", "Total valid product records", registry=pushgateway_registry
)
PRODUCTS_INVALID_COUNTER = Counter(
    "products_invalid_total",
    "Total invalid product records",
    registry=pushgateway_registry,
)
PRODUCTS_PROCESSING_DURATION = Histogram(
    "products_processing_duration_seconds",
    "Product processing duration in seconds",
    registry=pushgateway_registry,
)

# ====================
# Erasure Request Metrics
# ====================
ERASURE_REQUESTS_PROCESSED_COUNTER = Counter(
    "erasure_requests_processed_total",
    "Total erasure requests processed",
    registry=pushgateway_registry,
)
ERASURE_REQUESTS_FAILURE_COUNTER = Counter(
    "erasure_requests_failed_total",
    "Total failed erasure requests",
    registry=pushgateway_registry,
)
ERASURE_REQUESTS_SUCCESS_COUNTER = Counter(
    "erasure_requests_successful_total",
    "Total successful erasure requests",
    registry=pushgateway_registry,
)
ERASURE_REQUESTS_ANONYMIZED_COUNTER = Counter(
    "records_anonymized_total",
    "Total records anonymized",
    registry=pushgateway_registry,
)
ERASURE_REQUESTS_DURATION = Histogram(
    "erasure_request_processing_duration_seconds",
    "Erasure request processing duration in seconds",
    registry=pushgateway_registry,
)

# ====================
# Transaction Metrics
# ====================
TRANSACTIONS_PROCESSED_COUNTER = Counter(
    "transactions_processed_total",
    "Total transaction records processed",
    registry=pushgateway_registry,
)
TRANSACTIONS_VALID_COUNTER = Counter(
    "transactions_valid_total",
    "Total valid transaction records",
    registry=pushgateway_registry,
)
TRANSACTIONS_INVALID_COUNTER = Counter(
    "transactions_invalid_total",
    "Total invalid transaction records",
    registry=pushgateway_registry,
)
TRANSACTIONS_PROCESSING_DURATION = Histogram(
    "transactions_processing_duration_seconds",
    "Transaction processing duration in seconds",
    registry=pushgateway_registry,
)


# ====================
# Push Helpers
# ====================
def push_metrics(job_name="etl_pipeline", registry=pushgateway_registry):
    try:
        push_to_gateway(PROMETHEUS_PUSHGATEWAY, job=job_name, registry=registry)
    except Exception as e:
        logger.warning(f"Failed to push metrics: {e}")
