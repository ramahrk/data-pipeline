#!/bin/bash

echo "⏳ Starting Kafka consumer inside data-pipeline-etl-ci container..."
docker compose exec -d data-pipeline-etl-ci \
  python -c "from src.streaming.consumer import StreamingConsumer; from src.streaming.processor import StreamingProcessor; consumer = StreamingConsumer(['customers', 'products', 'transactions', 'erasure-requests']); processor = StreamingProcessor(); consumer.consume(processor.process_batch)"

# Wait briefly to ensure consumer is ready
sleep 5

echo "🚀 Sending test data to Kafka topics using load_test_data_to_kafka.py..."
export PYTHONPATH=.
python ./src/streaming/load_test_data_to_kafka.py

# Optionally, to stop the consumer:
# docker compose exec data-pipeline-etl-ci pkill -f 'python -c.*StreamingConsumer'
