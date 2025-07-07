#!/bin/bash
# echo "⏳ Waiting for Kafka to become available..."
# until docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list &>/dev/null; do
#   sleep 2
# done

echo "📦 Creating Kafka topics: customers, products, transactions, erasure-requests..."
docker compose exec -T kafka kafka-topics --create --if-not-exists --topic customers --partitions 1 --replication-factor 1 --bootstrap-server kafka:29092
docker compose exec -T kafka kafka-topics --create --if-not-exists --topic products --partitions 1 --replication-factor 1 --bootstrap-server kafka:29092
docker compose exec -T kafka kafka-topics --create --if-not-exists --topic transactions --partitions 1 --replication-factor 1 --bootstrap-server kafka:29092
docker compose exec -T kafka kafka-topics --create --if-not-exists --topic erasure-requests --partitions 1 --replication-factor 1 --bootstrap-server kafka:29092

echo "✅ Topics created (or already exist)."
