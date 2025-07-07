#!/bin/bash

# Start the Kafka consumer in the background inside the container
echo "Starting Kafka consumer inside data-pipeline-etl-new container..."
docker compose exec data-pipeline-etl-new \
  python -c "from src.streaming.consumer import StreamingConsumer; from src.streaming.processor import StreamingProcessor; consumer = StreamingConsumer(['customers', 'products', 'transactions', 'erasure-requests']); processor = StreamingProcessor(); consumer.consume(processor.process_batch)" &
CONSUMER_PID=$!

# Wait briefly to ensure consumer is ready
sleep 5

# Run the Kafka producer script from host
echo "Sending test data to Kafka topics using load_test_data_to_kafka.py..."
export PYTHONPATH=.
python ./src/streaming/load_test_data_to_kafka.py

# Optional: kill consumer if needed (CTRL+C will kill all)
# kill $CONSUMER_PID
