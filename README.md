# Liftline

A distributed ingestion pipeline for ski-lift scan events: HTTP API, RabbitMQ broker,
and a batching consumer that persists to DynamoDB with Redis caching for read queries.

Explores admission control feedback loops, publisher confirms, and at-least-once
settlement under a synthetic 200k-event ingest workload.

## Layout

- `Server/SkierServer/`: Ingest and read APIs, token bucket admission control
- `Consumer/SkierConsumer/`: AMQP listener, batching DynamoDB writer, unique skier counting
- `Client/SkierClient/`: Open-loop and closed-loop load generator with HdrHistogram
- `Infra/SchemaTool/`: DynamoDB schema definition as code

## Prerequisites

Java 17, Maven 3.8+, and Docker (for LocalStack DynamoDB, RabbitMQ, and Redis).

