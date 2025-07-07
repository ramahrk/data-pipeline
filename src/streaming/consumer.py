import json
import os
import time

from confluent_kafka import Consumer, KafkaError
from prometheus_client import (CollectorRegistry, Counter, Histogram,
                               push_to_gateway, start_http_server)

from src.config import (KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID,
                        PROMETHEUS_PUSHGATEWAY)
from src.metrics import (MESSAGE_CONSUMED_COUNTER, MESSAGE_PROCESSING_DURATION,
                         registry)
from src.utils.logging import setup_logger

DISABLE_METRICS_PUSH = os.getenv("DISABLE_METRICS_PUSH", "0") == "1"
logger = setup_logger(__name__)


class StreamingConsumer:
    """
    Kafka consumer for streaming data processing with Prometheus metrics.
    """

    def __init__(self, topics, group_id=None, bootstrap_servers=None):
        self.topics = topics if isinstance(topics, list) else [topics]
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
        )

        # Read group_id from env var if not passed explicitly
        self.group_id = group_id or os.getenv(
            "KAFKA_GROUP_ID", "data-pipeline-etl-default"
        )

        self.consumer = None
        # Set up logger
        self._logger = setup_logger(__name__)

    def connect(self):
        """
        Connect to Kafka and subscribe to topics.
        """
        try:
            conf = {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }

            # Debug print to check config types and values
            print(f"Kafka config dict: {conf}")

            self.consumer = Consumer(conf)
            self.consumer.subscribe(self.topics)
            logger.info(
                f"Connected to Kafka and subscribed to topics: {', '.join(self.topics)}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False

    def push_metrics_to_gateway(self):
        try:
            push_to_gateway(
                PROMETHEUS_PUSHGATEWAY, job="kafka_consumer_job", registry=registry
            )
            logger.info("Metrics pushed to Prometheus Pushgateway")
        except Exception as e:
            logger.error(f"Failed to push metrics to Pushgateway: {e}")

    def consume(self, processor_func, batch_size=100, timeout=1.0):
        if not self.consumer and not self.connect():
            return 0

        messages_processed = 0
        batch = []

        try:
            while messages_processed < batch_size:
                msg = self.consumer.poll(timeout=timeout)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self._logger.debug(
                            f"Reached end of partition {msg.topic()}/{msg.partition()}"
                        )
                    else:
                        self._logger.error(f"Error consuming message: {msg.error()}")
                    continue

                try:
                    message_value = json.loads(msg.value().decode("utf-8"))
                    batch.append(
                        {
                            "topic": msg.topic(),
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                            "value": message_value,
                        }
                    )
                    messages_processed += 1
                    MESSAGE_CONSUMED_COUNTER.inc()

                    if len(batch) >= batch_size:
                        self._process_batch_grouped_by_source_file(
                            batch, processor_func
                        )
                        self.consumer.commit()
                        batch = []

                        if not DISABLE_METRICS_PUSH:
                            self.push_metrics_to_gateway()

                except json.JSONDecodeError:
                    self._logger.error(f"Invalid JSON in message: {msg.value()}")
                except Exception as e:
                    self._logger.error(f"Error processing message: {str(e)}")

            if batch:
                self._process_batch_grouped_by_source_file(batch, processor_func)
                self.consumer.commit()

                if not DISABLE_METRICS_PUSH:
                    self.push_metrics_to_gateway()

        except Exception as e:
            self._logger.error(f"Error consuming messages: {str(e)}")

        return messages_processed

    def _process_batch_grouped_by_source_file(self, batch, processor_func):
        messages_by_file = {}
        for msg in batch:
            source_file = msg["value"].get("_source_file")
            if not source_file:
                self._logger.warning("Message missing '_source_file' field; skipping")
                continue
            messages_by_file.setdefault(source_file, []).append(msg)

        for source_file, messages in messages_by_file.items():
            self._logger.info(
                f"Processing batch of {len(messages)} messages from source file: {source_file}"
            )
            processor_func(messages)

    def close(self):
        if self.consumer:
            self.consumer.close()
            self._logger.info("Kafka consumer closed")
