#!/bin/bash

# Make sure you're in the directory with your docker-compose.yml
# This script runs your streaming consumer inside the data-pipeline-etl container

echo "Starting Kafka consumer in data-pipeline-etl-ci container..."

docker compose exec data-pipeline-etl-ci \
  python -c "from src.streaming.consumer import StreamingConsumer; from src.streaming.processor import StreamingProcessor; consumer = StreamingConsumer(['customers', 'products', 'transactions', 'erasure-requests']); processor = StreamingProcessor(); consumer.consume(processor.process_batch)"
