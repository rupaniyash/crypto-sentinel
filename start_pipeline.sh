#!/bin/bash

echo "🛑 Stopping containers..."
docker compose down

echo "🧹 Cleaning up old Data & Checkpoints..."
# Fix permissions so we can actually delete the files
chmod -R 777 spark_data
# We wipe the data to ensure Spark starts fresh without "Zombie Checkpoint" errors
rm -rf spark_data/checkpoint spark_data/storage/*

echo "🚀 Starting Infrastructure..."
# We skip Airflow to save RAM. Only the essentials.
docker compose up -d zookeeper kafka spark-master spark-worker dashboard

echo "⏳ Waiting 15s for Kafka to wake up..."
sleep 15

echo "🔌 Starting Connector..."
docker compose up -d connector

echo "🧠 Submitting Spark Job..."
# Running as root (-u 0) to bypass Permission Denied errors
docker exec -u 0 -d crypto-spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/spark_processor.py

echo "✅ System is Live! Tail logs below (Press Ctrl+C to exit logs):"
docker logs -f crypto-connector --tail 20