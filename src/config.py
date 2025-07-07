"""
Configuration settings for the ETL pipeline.
"""

import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_PATH = os.environ.get("INPUT_PATH", os.path.join(BASE_DIR, "data", "input"))
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", os.path.join(BASE_DIR, "data", "output"))
QUARANTINE_PATH = os.environ.get(
    "QUARANTINE_PATH", os.path.join(BASE_DIR, "data", "quarantine")
)

# Ensure directories exist
for path in [INPUT_PATH, OUTPUT_PATH, QUARANTINE_PATH]:
    os.makedirs(path, exist_ok=True)

# Reference data paths
REFERENCE_DATA_PATH = os.path.join(BASE_DIR, "data", "reference")
os.makedirs(REFERENCE_DATA_PATH, exist_ok=True)


# Logging configuration
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_FILE = os.environ.get("LOG_FILE", os.path.join(BASE_DIR, "logs", "pipeline.log"))
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# Metrics configuration
METRICS_ENABLED = os.environ.get("METRICS_ENABLED", "True").lower() == "true"
METRICS_PATH = os.environ.get("METRICS_PATH", os.path.join(BASE_DIR, "metrics"))
os.makedirs(METRICS_PATH, exist_ok=True)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
# KAFKA_BOOTSTRAP_LOCAL_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_LOCAL_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "data-pipeline-etl-new")
KAFKA_TOPICS = {
    "customers": "customers",
    "products": "products",
    "transactions": "transactions",
    "erasure_requests": "erasure-requests",
}


# Prometheus Pushgateway URL
raw_gateway = os.environ.get("PROMETHEUS_PUSHGATEWAY", "pushgateway:9091")

# Ensure the URL includes scheme
if not raw_gateway.startswith("http://") and not raw_gateway.startswith("https://"):
    raw_gateway = f"http://{raw_gateway}"

PROMETHEUS_PUSHGATEWAY = raw_gateway
