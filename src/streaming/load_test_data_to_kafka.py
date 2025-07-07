import gzip
import json
import os
import time

from confluent_kafka import Producer
from prometheus_client import (CollectorRegistry, Counter, Histogram,
                               push_to_gateway, start_http_server)

from src.config import PROMETHEUS_PUSHGATEWAY
from src.metrics import (MESSAGE_PROCESSING_DURATION,
                         MESSAGE_PRODUCTION_DURATION, MESSAGES_FAILED,
                         MESSAGES_PRODUCED, registry)
from src.utils.logging import setup_logger

logger = setup_logger(__name__)


def delivery_report(err, msg):
    """Delivery callback for Kafka producer."""
    if err is not None:
        MESSAGES_FAILED.inc()
        logger.info(f"Message delivery failed: {err}")
    else:
        logger.info(
            f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


def find_test_data_files(base_path="data/input"):
    """Find test data files in the input directory."""
    test_files = {}

    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.endswith(".json.gz"):
                dataset_name = file.split(".")[0]
                file_path = os.path.join(root, file)

                if dataset_name not in test_files:
                    test_files[dataset_name] = []

                test_files[dataset_name].append(file_path)
                logger.info(f"Found test data file: {file_path}")

    return test_files


def load_and_produce_data(file_path, topic, producer, delay=0.1):
    """Load data from a file and produce it to Kafka."""
    logger.info(f"Loading data from {file_path} and producing to topic {topic}")

    count = 0
    with gzip.open(file_path, "rt") as f:
        for line in f:
            try:
                record = json.loads(line.strip())

                # Determine key
                key = (
                    record.get("id")
                    or record.get("sku")
                    or record.get("transaction_id")
                    or record.get("customer-id")
                    or record.get("email")
                )
                # Inject _source_file metadata
                record["_source_file"] = file_path

                with MESSAGE_PRODUCTION_DURATION.time():
                    producer.produce(
                        topic,
                        key=str(key) if key else str(count),
                        value=json.dumps(record).encode("utf-8"),
                        callback=delivery_report,
                    )
                MESSAGES_PRODUCED.inc()
                count += 1

                if count % 100 == 0:
                    producer.flush()

                time.sleep(delay)

            except json.JSONDecodeError as e:
                MESSAGES_FAILED.inc()
                logger.error(f"Invalid JSON in file {file_path}: {e}")
            except Exception as e:
                MESSAGES_FAILED.inc()
                logger.error(f"Error producing record: {str(e)}")


def push_metrics():
    try:
        push_to_gateway(
            PROMETHEUS_PUSHGATEWAY, job="kafka_producer_job", registry=registry
        )
        logger.info("Metrics pushed to Prometheus Pushgateway")
    except Exception as e:
        logger.info(f"Failed to push metrics to Pushgateway: {e}")


if __name__ == "__main__":
    logger.info("Starting Kafka producer")
    # Use 'kafka:9092' if running inside Docker Compose, 'localhost:9092' if running on host
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    # Start Prometheus HTTP server for metrics scraping if enabled
    # if os.getenv("DISABLE_METRICS_PUSH", "0") not in (
    #     "1",
    #     "true",
    #     "True",
    #     "yes",
    #     "YES",
    # ):
    #     try:
    #         start_http_server(8002, registry=registry)
    #         logger.info("Prometheus metrics HTTP server started on port 8002")
    #     except OSError as e:
    #         logger.error(f"Prometheus server already running or port in use: {e}")

    # Kafka Producer config
    producer_config = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "ci-producer",
    }

    logger.info(f"Using Kafka config: {producer_config}")

    # Wait for Kafka to become available and create producer
    max_retries = 30
    for attempt in range(max_retries):
        try:
            producer = Producer(producer_config)
            logger.info("Kafka producer successfully created")
            break
        except Exception as e:
            logger.warning(f"[{attempt+1}/{max_retries}] Waiting for Kafka: {e}")
            time.sleep(5)
    else:
        logger.error("Kafka is unavailable after multiple retries. Exiting.")
        exit(1)

    # Load and send test data
    test_files = find_test_data_files("data/input")
    for topic, file_paths in test_files.items():
        for file_path in file_paths:
            load_and_produce_data(file_path, topic, producer)

    producer.flush()
    logger.info("All messages sent.")

    # Push Prometheus metrics if enabled
    if os.getenv("DISABLE_METRICS_PUSH", "0") not in (
        "1",
        "true",
        "True",
        "yes",
        "YES",
    ):
        push_metrics()
