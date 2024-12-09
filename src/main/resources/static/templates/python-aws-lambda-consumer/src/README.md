# Python AWS Lambda Kafka Consumer

A serverless application that consumes messages from a Kafka topic using AWS Lambda and SAM (Serverless Application Model).

## Description

This project implements a serverless Kafka consumer using AWS Lambda that can process messages from a Kafka topic. It supports SASL/SSL authentication and configurable batch processing.

## Prerequisites

- AWS CLI installed and configured 
- AWS SAM CLI installed
- Python 3.13
- Access to a Confluent Kafka cluster with SASL/SSL authentication
- Kafka credentials (API Key and Secret)

## Architecture

The application consists of:
- AWS Lambda function configured as a Kafka consumer
- AWS Secrets Manager to store Kafka credentials
- Event Source Mapping for Confluent Kafka integration

## Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| KafkaBootstrapServer | Kafka bootstrap server URL | - |
| ConsumerGroupId | Kafka consumer group ID | - |
| ApiKey | Kafka API Key (SASL username) | - |
| Secret | Kafka Secret (SASL password) | - |
| Topic | Kafka topic to consume from | - |
| StartingPosition | Starting position for consumption | LATEST |
| BatchSize | Number of records to fetch per batch | 100 |
| MaximumBatchingWindowInSeconds | Maximum time to wait for batch | 30 |

## Deployment

1. Build the application:
```bash
sam build
sam deploy --guided --config-file samconfig.toml
```
2. Check the Cloudwatch logs of the Lambda function for the messages printed from the Lambda function.