# Vehicle Telemetry Streaming Project

## Overview

This project demonstrates:

- Kafka + Schema Registry
- Avro Key + Value schemas
- Apache Flink streaming job
- Dockerized local environment

## Architecture

Telemetry → Kafka → Flink → Processed Kafka Topics

## Setup

### 1. Start Infrastructure

```bash
cd src/e2e/resrouces
docker compose up -d
```

### 2. Run the TelemetryE2ETest file