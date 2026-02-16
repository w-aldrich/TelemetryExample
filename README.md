# Vehicle Telemetry Streaming Project

## Overview

This project demonstrates:

- Kafka + Schema Registry
- Avro Key + Value schemas
- Apache Flink streaming job
- RocksDB state backend
- Dockerized local environment

## Architecture

Telemetry → Kafka → Flink → Processed Kafka Topics

## Setup

### 1. Start Infrastructure

```bash
cd docker
docker compose up -d
```

### 2. Build Project Steps
```bash
mvn clean package
```

### 3. Run Flink Job
```bash
mvn exec:java -Dexec.mainClass="com.example.TelemetryJob"
```