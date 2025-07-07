"""
Kafka producer for streaming data processing.
"""

import json
import os
import time

from confluent_kafka import Producer
from prometheus_client import push_to_gateway, start_http_server

from src.config import KAFKA_BOOTSTRAP_SERVERS, PROMETHEUS_PUSHGATEWAY
from src.metrics import (MESSAGE_PRODUCED_COUNTER, MESSAGE_PRODUCTION_DURATION,
                         registry)
from src.utils.logging import setup_logger


class StreamingProducer:
    """
    Kafka producer for streaming data processing.
    """

    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS):
        """
        Initialize the Kafka producer and Prometheus metrics.
        """
        self.bootstrap_servers = bootstrap_servers
        self.producer = None

        # Setup logger first (needed before metrics server logs)
        self._logger = setup_logger(__name__)

        # Control whether to expose metrics (set DISABLE_METRICS_PUSH=1 to disable)
        self.disable_metrics_push = os.getenv("DISABLE_METRICS_PUSH", "0") == "1"

        # Use the centralized registry for metrics
        self.registry = registry

    def connect(self):
        """
        Connect to Kafka.
        """
        try:
            conf = {
                "bootstrap.servers": self.bootstrap_servers,
                "client.id": "data-pipeline-producer",
            }
            self.producer = Producer(conf)
            self._logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
            return True
        except Exception as e:
            self._logger.error(f"Failed to connect to Kafka: {str(e)}")
            return False

    def delivery_report(self, err, msg):
        """
        Delivery report callback.
        """
        if err is not None:
            self._logger.error(f"Message delivery failed: {err}")
        else:
            self._logger.debug(
                f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
            )

    def push_metrics_to_gateway(self):
        """
        Push current metrics to Prometheus Pushgateway.
        """
        try:
            push_to_gateway(
                PROMETHEUS_PUSHGATEWAY,
                job="streaming_producer_job",
                registry=self.registry,
            )
            self._logger.info("Metrics pushed to Prometheus Pushgateway")
        except Exception as e:
            self._logger.error(f"Failed to push metrics to Pushgateway: {e}")

    def produce(self, topic, key, value, source_file=None, push_metrics=False):
        """
        Produce a message to Kafka.

        Args:
            topic (str): Topic name
            key (str): Message key
            value (dict): Message payload
            source_file (str, optional): Original source file path
            push_metrics (bool): Whether to push metrics after this message

        Returns:
            bool: True if produced successfully, False otherwise
        """
        if not self.producer:
            if not self.connect():
                return False

        try:
            # Inject _source_file metadata if provided
            if source_file:
                value = dict(value)  # copy to avoid mutating caller data
                value["_source_file"] = source_file

            value_json = json.dumps(value).encode("utf-8")

            start_time = time.time()
            self.producer.produce(
                topic=topic,
                key=key.encode("utf-8") if key else None,
                value=value_json,
                callback=self.delivery_report,
            )
            # Poll producer to handle delivery reports asynchronously
            self.producer.poll(0)

            MESSAGE_PRODUCED_COUNTER.inc()
            MESSAGE_PRODUCTION_DURATION.observe(time.time() - start_time)

            # Flush periodically or if pushing metrics immediately
            if push_metrics or self.producer.flush(0) != 0:
                self.producer.flush()

            if not self.disable_metrics_push and push_metrics:
                self.push_metrics_to_gateway()

            return True
        except Exception as e:
            self._logger.error(f"Failed to produce message: {str(e)}")
            return False

    def close(self):
        """
        Close the Kafka producer.
        """
        if self.producer:
            self.producer.flush()
            self._logger.info("Kafka producer closed")
