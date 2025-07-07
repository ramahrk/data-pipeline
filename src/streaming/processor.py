# The `StreamingProcessor` class processes batches of messages from different Kafka topics, applies
# specific processors to each topic, and pushes metrics to Prometheus Pushgateway if enabled.
"""
Streaming data processor module.
"""

import gzip
import json
import os
import time
import time as pytime

from prometheus_client import (CollectorRegistry, Counter, Histogram,
                               push_to_gateway, start_http_server)

from src.config import (KAFKA_TOPICS, OUTPUT_PATH, PROMETHEUS_PUSHGATEWAY,
                        QUARANTINE_PATH)
from src.metrics import (MESSAGE_PROCESSED_COUNTER,
                         MESSAGE_PROCESSING_DURATION, registry)
from src.processors.customer import process_customers
from src.processors.erasure import apply_erasure_requests
from src.processors.product import process_products
from src.processors.transaction import process_transactions
from src.utils.logging import setup_logger

# Control whether to expose metrics (set DISABLE_METRICS_PUSH=1 to disable)
DISABLE_METRICS_PUSH = os.getenv("DISABLE_METRICS_PUSH", "0") == "1"


class StreamingProcessor:
    """
    Streaming data processor.
    """

    def __init__(self):
        """
        Initialize the streaming processor.
        """
        # Set up logger
        self._logger = setup_logger(__name__)

        self.products_by_sku = None
        self.customers_by_id = None
        self.processors = {
            KAFKA_TOPICS["customers"]: self._process_customer_batch,
            KAFKA_TOPICS["products"]: self._process_product_batch,
            KAFKA_TOPICS["transactions"]: self._process_transaction_batch,
            KAFKA_TOPICS["erasure_requests"]: self._process_erasure_batch,
        }

    def _load_reference_data(self):
        if self.products_by_sku is None:
            # Load products by SKU from reference path or input
            from src.processors.product import load_products

            # (Define how to load or pass path)
            self.products_by_sku = load_products(...)
        if self.customers_by_id is None:
            from src.utils.data_access import load_customers_by_id

            self.customers_by_id = load_customers_by_id()

    def push_metrics_to_gateway(self):
        try:
            push_to_gateway(
                PROMETHEUS_PUSHGATEWAY, job="streaming_processor_job", registry=registry
            )
            self._logger.info("Metrics pushed to Prometheus Pushgateway")
        except Exception as e:
            self._logger.error(f"Failed to push metrics to Pushgateway: {e}")

    def process_batch(self, batch):
        """
        Process a batch of messages.

        Args:
            batch (list): List of messages
        """
        if not batch:
            return

        start_time = time.time()  # ✅ Start timing

        # Group messages first by topic, then by source file (_source_file)
        messages_by_topic_and_source = {}

        for message in batch:
            topic = message["topic"]
            source_file = message["value"].get("_source_file", "unknown_source")

            if topic not in messages_by_topic_and_source:
                messages_by_topic_and_source[topic] = {}
            if source_file not in messages_by_topic_and_source[topic]:
                messages_by_topic_and_source[topic][source_file] = []

            messages_by_topic_and_source[topic][source_file].append(message)
        try:
            # Process each topic and source-file batch separately
            for topic, source_groups in messages_by_topic_and_source.items():
                if topic not in self.processors:
                    self._logger.warning(f"No processor found for topic: {topic}")
                    continue

                for source_file, messages in source_groups.items():
                    self._logger.info(
                        f"Processing {len(messages)} messages for topic '{topic}' from source '{source_file}'"
                    )
                    self.processors[topic](messages, source_file)
                    MESSAGE_PROCESSED_COUNTER.inc(
                        len(messages)
                    )  # Increment counter after processing

            # Push metrics once after all source groups
            if not DISABLE_METRICS_PUSH:
                self.push_metrics_to_gateway()

        except Exception as e:
            self._logger.error(f"Error processing batch: {e}")
        finally:
            MESSAGE_PROCESSING_DURATION.observe(
                pytime.time() - start_time
            )  # Measure and observe the duration

    def _process_customer_batch(self, messages, source_file):
        customers = [msg["value"] for msg in messages]
        temp_dir = os.path.join(os.getcwd(), "temp")
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = os.path.join(temp_dir, f"customers_{int(time.time())}.json.gz")
        with gzip.open(temp_file, "wt") as f:
            for customer in customers:
                f.write(json.dumps(customer) + "\n")
        try:
            process_customers(temp_file, source_file=source_file)
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def _process_product_batch(self, messages, source_file):
        products = [msg["value"] for msg in messages]
        temp_dir = os.path.join(os.getcwd(), "temp")
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = os.path.join(temp_dir, f"products_{int(time.time())}.json.gz")
        with gzip.open(temp_file, "wt") as f:
            for product in products:
                f.write(json.dumps(product) + "\n")
        try:
            process_products(temp_file, source_file=source_file)
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def _process_transaction_batch(self, messages, source_file):
        self._load_reference_data()
        transactions = [msg["value"] for msg in messages]
        temp_dir = os.path.join(os.getcwd(), "temp")
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = os.path.join(temp_dir, f"transactions_{int(time.time())}.json.gz")
        with gzip.open(temp_file, "wt") as f:
            for transaction in transactions:
                f.write(json.dumps(transaction) + "\n")
        try:
            process_transactions(
                temp_file,
                source_file=source_file,
                products_by_sku=self.products_by_sku,
                customers_by_id=self.customers_by_id,
            )
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def _process_erasure_batch(self, messages, source_file):
        erasure_requests = [msg["value"] for msg in messages]
        temp_dir = os.path.join(os.getcwd(), "temp")
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = os.path.join(
            temp_dir, f"erasure_requests_{int(time.time())}.json.gz"
        )
        with gzip.open(temp_file, "wt") as f:
            for request in erasure_requests:
                f.write(json.dumps(request) + "\n")
        try:
            apply_erasure_requests(temp_file, source_file=source_file)
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
