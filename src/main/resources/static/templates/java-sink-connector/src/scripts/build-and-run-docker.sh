#!/bin/bash

# Step 1: Build the Kafka Connector image
echo "Building Kafka Connector Docker image..."
docker-compose build connector

# Step 2: Start Kafka and the Connector
echo "Starting Kafka and Kafka Connector..."
docker-compose up -d

echo "Kafka and Kafka Connector are up and running."
