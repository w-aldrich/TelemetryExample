# Vehicle Telemetry Streaming — TelemetryExample

A reference implementation of a real-time vehicle telemetry processing pipeline built with **Apache Flink**, **Apache Kafka**, **Confluent Schema Registry**, and **Apache Avro**. The project demonstrates a fan-out streaming pattern: a single inbound Kafka topic is consumed by Flink, which routes and aggregates events into one or more typed outbound topics.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Tech Stack & Dependencies](#tech-stack--dependencies)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Avro Schemas](#avro-schemas)
- [Telemetry Event Types](#telemetry-event-types)
- [Flink Job Walkthrough](#flink-job-walkthrough)
- [State Management & TTL](#state-management--ttl)
- [Watermark Strategy](#watermark-strategy)
- [Error Handling & DLQ](#error-handling--dlq)
- [Testing](#testing)
- [Sample Data](#sample-data)
- [Known TODOs](#known-todos)

---

## Overview

This project processes real-time telemetry emitted by vehicles (and surrounding context like weather, traffic, and road conditions). Events are published to a single Kafka inbound topic as Avro-encoded records. A stateful Flink job consumes these, classifies each event by `TelemetryType`, routes it to the appropriate processor, and publishes aggregated results to outbound topics.

The currently implemented pipeline aggregates **speed, acceleration, and odometer** events per vehicle per day into a `SpeedInformation` record, which is published to a dedicated outbound speed topic.

---

## Architecture

```
Vehicle / Simulator / E2E Test
          │
          ▼
┌─────────────────────────┐
│   Kafka Inbound Topic   │  ← Avro (key: TelemetryKey, value: TelemetryValue)
│      "someTopic"        │
└────────────┬────────────┘
             │
             ▼
┌────────────────────────────────────────────────────────┐
│                   Apache Flink Job                     │
│                                                        │
│  KafkaSource<KafkaRecord>                              │
│       │                                                │
│       ▼  (WatermarkStrategy: BoundedOutOfOrderness     │
│           24hrs, timestamp from eventTimestamp)        │
│       │                                                │
│  InboundRouter (ProcessFunction)                       │
│   ├── deserialization error? → DLQ side output         │
│   ├── tombstone record?      → DLQ side output         │
│   └── by TelemetryType:                                │
│        SPEED / ACCELERATION / ODOMETER                 │
│             └──► speedInfo side output                 │
│                       │                                │
│                       ▼ keyBy(routingKey)              │
│                  ProcessSpeed                          │
│               (KeyedProcessFunction)                   │
│               ValueState<SpeedInformation>             │
│               TTL: 24hrs OnReadAndWrite                │
│                       │                                │
│                       ▼                                │
└───────────────────────┼────────────────────────────────┘
                        │
                        ▼
         ┌──────────────────────────┐
         │  Kafka Outbound Topic    │  ← Avro (key: vehicleId+date, value: SpeedInformation)
         │   "someOutboundTopic"    │
         └──────────────────────────┘
```

**Routing key** format: `{vehicleId}_{UTC date}` — ensuring state is scoped per vehicle per calendar day.

---

## Project Structure

```
TelemetryExample/
├── pom.xml                                # Maven build + dependency management
├── schemas/
│   ├── inboundAvsc/
│   │   ├── key.avsc                       # Inbound Kafka key schema (vehicleId, vin, fleetId)
│   │   └── value.avsc                     # Inbound value schema (15 telemetry payload types)
│   └── outboundAvsc/
│       ├── key.avsc                       # Outbound key schema (vehicleId, date)
│       └── valueSpeedInformation.avsc     # Outbound speed aggregation schema
├── sampleData/
│   ├── exampleKey                         # Sample TelemetryKey JSON
│   └── exampleValues/
│       ├── vehicle/                       # speed, acceleration, brake, battery, etc.
│       └── nonVehicleTelemetry/           # weather, traffic, road condition, etc.
└── src/
    ├── main/
    │   ├── java/com/example/
    │   │   ├── TelemetryJob.java              # Flink job entry point
    │   │   ├── config/
    │   │   │   └── AppConfig.java             # Loads application.properties
    │   │   ├── flink/
    │   │   │   ├── ProcessSpeed.java          # Stateful speed/acc/odometer aggregation
    │   │   │   └── ProcessVehicle.java        # Stub for future vehicle processing
    │   │   ├── model/
    │   │   │   ├── InboundRouter.java         # Routes events to typed side outputs
    │   │   │   ├── KafkaRecord.java           # Internal wrapper (key + value GenericRecord)
    │   │   │   ├── PartitionOffset.java       # Partition/offset tracking record
    │   │   │   ├── outbound/
    │   │   │   │   ├── Outbound.java          # Interface: toKafkaRecord()
    │   │   │   │   ├── OutboundKeyVIDDate.java # Outbound key (vehicleId + date)
    │   │   │   │   ├── SpeedInformation.java  # Stateful speed aggregation model
    │   │   │   │   └── SpeedInformationOutbound.java # DTO for Avro serialization
    │   │   │   └── telemetryEnums/
    │   │   │       ├── TelemetryType.java     # All 15 telemetry type values + helpers
    │   │   │       └── Status.java            # ACTIVE / STANDBY / INACTIVE
    │   │   └── util/
    │   │       ├── deserialization/
    │   │       │   ├── GenericDeserialization.java  # KafkaRecordDeserializationSchema impl
    │   │       │   └── errors/DeserializationError.java
    │   │       ├── serialization/
    │   │       │   └── GenericSerialization.java    # KafkaRecordSerializationSchema impl
    │   │       └── helpers/
    │   │           └── GenericRecordHelper.java     # Reflection-based POJO → GenericRecord
    │   └── resources/
    │       └── application.properties
    ├── test/
    │   └── java/com/example/
    │       ├── SpeedInformationTest.java        # Unit tests for SpeedInformation aggregation
    │       └── OutboundKeyVIDDateTest.java      # Unit tests for outbound key serialization
    └── e2e/
        ├── java/com/example/telemetry/
        │   ├── TelemetryE2ETest.java            # Full end-to-end integration test
        │   ├── infraSetup/
        │   │   ├── KafkaAdminHelper.java        # Creates Kafka topics
        │   │   ├── SchemaRegistryHelper.java    # Registers Avro schemas
        │   │   └── MiniCluster.java             # Flink MiniCluster lifecycle manager
        │   ├── telemetryEvents/                 # Event POJOs used for test data generation
        │   │   ├── vehicle/                     # Speed, Acceleration, Brake, Battery, etc.
        │   │   └── nonVehicle/                  # Weather, Traffic, RoadCondition, etc.
        │   └── utils/
        │       ├── InboundProducer.java         # Produces typed Avro events to Kafka
        │       ├── OutboundConsumer.java        # Consumes and collects outbound records
        │       └── RandomGenerator.java         # Random test data utilities
        └── resources/
            └── docker-compose.yml              # Local Confluent stack
```

---

## Tech Stack & Dependencies

| Component | Library / Version |
|---|---|
| Stream Processor | Apache Flink `1.18.1` |
| Kafka Connector | `flink-connector-kafka` `3.0.2-1.18` |
| State Backend | RocksDB (`flink-statebackend-rocksdb`) |
| Serialization | Apache Avro `1.11.3` |
| Schema Registry | Confluent `7.5.0` |
| Kafka Client | `kafka-clients` `3.7.0` |
| Test Framework | JUnit Jupiter `5.10.2` |
| Async Assertions | Awaitility `4.2.1` |
| Java Version | Java 17 |
| Build Tool | Maven |

---

## Prerequisites

- **Java 17+**
- **Maven 3.6+**
- **Docker** and **Docker Compose**

---

## Getting Started

### 1. Start Local Infrastructure

```bash
cd src/e2e/resources
docker compose up -d
```

This starts four containers:

| Container | Port | Purpose |
|---|---|---|
| `zookeeper` | 2181 | Kafka coordination |
| `broker` | 9092 | Kafka broker |
| `schema-registry` | 8081 | Confluent Schema Registry |
| `control-center` | 9021 | Confluent monitoring UI |

Wait ~30–60 seconds for all services to become healthy before proceeding.

### 2. Build the Project

```bash
mvn clean package -DskipTests
```

### 3. Run Unit Tests

```bash
mvn test
```

E2E tests are excluded from the default test phase automatically via the Surefire configuration.

### 4. Run the End-to-End Test

The E2E test requires Docker Compose to already be running (step 1). It uses a Flink `MiniCluster` launched in-process.

```bash
mvn verify -Pe2e
```

This will:
1. Register all Avro schemas with the Schema Registry
2. Create the inbound and outbound Kafka topics
3. Start a Flink `MiniCluster` and run `TelemetryJob`
4. Produce sample telemetry events to the inbound topic
5. Consume outbound records and assert correctness

### 5. Monitor in Control Center

Open `http://localhost:9021/clusters` to browse topic contents, consumer group lag, and registered schemas.

---

## Configuration

All runtime configuration lives in `src/main/resources/application.properties`:

```properties
# Kafka
kafka.bootstrap.servers=localhost:9092
kafka.transaction.timeout.ms=1000
kafka.consumer.group.id=telemetry-flink-group

# Schema Registry
schema.registry.url=http://localhost:8081

# Topics
telemetry.inbound.topic=someTopic
telemetry.outbound.speed.topic=someOutboundTopic

# Flink
flink.checkpoint.interval.ms=10000
flink.parallelism=1

# State
state.backend=rocksdb
state.ttl.minutes=10

# Avro
avro.use.generic=true
```

`AppConfig` loads this file at startup and exposes typed getters for each property. During E2E tests, `kafka.bootstrap.servers` and `schema.registry.url` are overridden by Maven Failsafe system properties.

---

## Avro Schemas

### Inbound Key — `schemas/inboundAvsc/key.avsc`

```json
{
  "name": "TelemetryKey",
  "fields": [
    { "name": "vehicleId", "type": "string" },
    { "name": "vin",       "type": "string" },
    { "name": "fleetId",   "type": ["null", "string"], "default": null }
  ]
}
```

### Inbound Value — `schemas/inboundAvsc/value.avsc`

The value schema uses an Avro **union** on the `payload` field to carry any of 15 telemetry types through a single topic. A `TelemetryType` enum field identifies which union branch is active.

| Payload Record | Key Fields |
|---|---|
| `SpeedTelemetry` | `speedKph: double` |
| `AccelerationTelemetry` | `xAxis, yAxis, zAxis: double` |
| `OdometerTelemetry` | `totalKilometers: double` |
| `EngineTelemetry` | `rpm: int`, `engineTempC: double` |
| `FuelTelemetry` | `fuelLevelPercent: double` |
| `LocationTelemetry` | `latitude, longitude, altitudeMeters: double` |
| `TirePressureTelemetry` | `frontLeftPsi, frontRightPsi, rearLeftPsi, rearRightPsi: double` |
| `BatteryTelemetry` | `voltage, currentAmps: double` |
| `BrakeTelemetry` | `brakePadWearPercent: double` |
| `DiagnosticTelemetry` | `errorCode: string`, `severity: string` |
| `WeatherTelemetry` | `temperatureC, humidityPercent, windSpeedKph, precipitationMm: double` |
| `RoadConditionTelemetry` | `surfaceType: string`, `surfaceTemperatureC: double`, `hazardDetected: boolean` |
| `TrafficTelemetry` | `congestionLevel: int`, `averageSpeedKph: double`, `incidentReported: boolean` |
| `InfrastructureTelemetry` | `sensorId: string`, `signalStrength: double`, `status: string` |
| `MobileDeviceTelemetry` | `deviceId: string`, `batteryLevelPercent: double`, `networkType, appVersion: string` |

The top-level value record also contains `eventId: string`, `eventTimestamp: long`, and a `vehicleInfo` sub-record (`make`, `model`, `year`, `engineType`).

### Outbound Key — `schemas/outboundAvsc/key.avsc`

```json
{
  "name": "TelemetryKey",
  "fields": [
    { "name": "vehicleId", "type": "string" },
    { "name": "date",      "type": "long" }
  ]
}
```

### Outbound Speed Value — `schemas/outboundAvsc/valueSpeedInformation.avsc`

```json
{
  "name": "TelemetryValue",
  "fields": [
    { "name": "averageXAcceleration", "type": "double" },
    { "name": "averageYAcceleration", "type": "double" },
    { "name": "averageZAcceleration", "type": "double" },
    { "name": "averageSpeed",         "type": "double" },
    { "name": "totalKmDriven",        "type": "double" }
  ]
}
```

---

## Telemetry Event Types

The `TelemetryType` enum defines all 15 supported event types and provides two helpers:

- `fromGRToType(GenericRecord)` — extracts the enum value directly from a deserialized Avro record
- `typeToTelemetryName()` — converts e.g. `TIRE_PRESSURE` → `"TirePressureTelemetry"` for matching against Avro union type names in the inbound producer

The `InboundRouter` currently routes the following three types to the speed aggregation pipeline:

| TelemetryType | Routed To |
|---|---|
| `SPEED` | `speedInfoOutputTag` |
| `ACCELERATION` | `speedInfoOutputTag` |
| `ODOMETER` | `speedInfoOutputTag` |

All other types fall through the switch with no-op behavior today (future work).

---

## Flink Job Walkthrough

### Entry Point — `TelemetryJob`

`TelemetryJob.main()` calls `run(StreamExecutionEnvironment)`, which is also the hook used by the E2E test to inject a `MiniCluster` environment.

```java
// Source
KafkaSource<KafkaRecord> source = KafkaSource.<KafkaRecord>builder()
    .setBootstrapServers(bootstrapServers)
    .setTopics(inboundTopic)
    .setGroupId(appConfig.getConsumerGroupId())
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setDeserializer(new GenericDeserialization(inboundTopic, schemaRegUrl))
    .build();

// Sink
KafkaSink<KafkaRecord> speedSink = KafkaSink.<KafkaRecord>builder()
    .setBootstrapServers(bootstrapServers)
    .setRecordSerializer(new GenericSerialization(outboundSpeedTopic, schemaRegUrl))
    .build();

// Pipeline
SingleOutputStreamOperator<KafkaRecord> router =
    env.fromSource(source, watermarkStrategy, inboundTopic)
       .process(inboundRouter);

router.getSideOutput(inboundRouter.getSpeedInfoOutputTag())
      .keyBy(KafkaRecord::getRoutingKey)
      .process(new ProcessSpeed())
      .sinkTo(speedSink);
```

Checkpointing is configured at a 10-second interval with `EXACTLY_ONCE` semantics. Parallelism is set to 1 (intended to be raised once the pipeline is production-ready). A no-restart strategy is set explicitly to surface failures immediately during development.

### Serialization — `GenericDeserialization` / `GenericSerialization`

Both use Confluent's `KafkaAvroDeserializer` / `KafkaAvroSerializer` configured for **generic** (not specific) Avro records (`specific.avro.reader=false`). The serializers are `transient` and initialized lazily so they remain safe to serialize across Flink task manager boundaries. Deserialization errors for key and value are caught independently and attached to the `KafkaRecord` rather than thrown, enabling downstream DLQ routing without crashing the pipeline.

### Internal Model — `KafkaRecord`

`KafkaRecord` is the internal wrapper that flows through the entire Flink pipeline:

| Field | Type | Description |
|---|---|---|
| `key` | `GenericRecord` | Deserialized Avro key |
| `value` | `GenericRecord` | Deserialized Avro value |
| `routingKey` | `String` | `{vehicleId}_{UTC date}` computed from `eventTimestamp` |
| `partition` / `offset` | `int` / `long` | Source partition and offset for traceability |
| `keyDeserializationError` | `Optional<DeserializationError>` | Set if key deserialization failed |
| `valueDeserializationError` | `Optional<DeserializationError>` | Set if value deserialization failed |

Tombstone detection: `key != null && value == null`.

### `GenericRecordHelper`

Converts any POJO to an Avro `GenericRecord` via reflection — iterates all declared fields, makes them accessible, skips fields with `"Schema"` in their name, and calls `record.put(fieldName, value)`. Used to convert outbound model objects (`SpeedInformationOutbound`, `OutboundKeyVIDDate`) into `GenericRecord` for serialization.

---

## State Management & TTL

`ProcessSpeed` is a `KeyedProcessFunction<String, KafkaRecord, KafkaRecord>` keyed by `routingKey` (`vehicleId_date`). It maintains a single `ValueState<SpeedInformation>` that accumulates running totals per vehicle per day. On each event it updates the relevant fields, calls `state.update(inState)`, and emits the current aggregated record.

**State TTL configuration:**

```java
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.hours(24))
    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .cleanupFullSnapshot()
    .build();
```

| Setting | Value | Effect |
|---|---|---|
| Duration | 24 hours | State for an inactive vehicle expires after one day |
| Update Type | `OnReadAndWrite` | TTL resets on every read or write |
| Visibility | `NeverReturnExpired` | Expired state is treated as null before GC runs |
| Cleanup | `cleanupFullSnapshot` | Expired entries purged at checkpoint/savepoint time |

> When `EmbeddedRocksDBStateBackend` is enabled, consider switching to `cleanupInRocksdbCompactFilter()` for more aggressive background cleanup during compaction.

**`SpeedInformation` aggregation logic:**

| Event Type | Fields Updated | Output Field |
|---|---|---|
| `SPEED` | `sumSpeed`, `countSpeed` | `averageSpeed` |
| `ACCELERATION` | `sumX/Y/Z`, `countX/Y/Z` | `averageX/Y/ZAcceleration` |
| `ODOMETER` | `startKm` (first reading), `currentKm` (subsequent) | `totalKmDriven` = `currentKm - startKm` |

---

## Watermark Strategy

```java
WatermarkStrategy.<KafkaRecord>forBoundedOutOfOrderness(Duration.ofHours(24))
    .withTimestampAssigner((event, timestamp) ->
        (long) event.getValue().get("eventTimestamp"));
```

A **bounded out-of-orderness** watermark tolerates up to 24 hours of late-arriving events before advancing event time. Timestamps are sourced from the `eventTimestamp` field in the Avro value (milliseconds since epoch). This window aligns with the per-day routing key so that late events from the same UTC date still aggregate into the correct state partition.

---

## Error Handling & DLQ

The `InboundRouter` sends records to a `dlq` side output in two situations:

1. **Deserialization errors** — key or value failed to deserialize; the `KafkaRecord` carries the error details including whether it was the key or value, the partition, offset, and error message.
2. **Tombstone records** — key is non-null but value is null.

The DLQ side output is defined but not yet wired to a sink in `TelemetryJob` (marked as `// TODO: implement DLQ`).

---

## Testing

### Unit Tests (`src/test/`)

Run with `mvn test`. E2E tests are excluded automatically.

**`SpeedInformationTest`** covers the `SpeedInformation` model in isolation:

| Test | Assertion |
|---|---|
| `testConstructor` | Instantiation does not throw |
| `testToKafkaRecord` | Default state emits all-zero values with the correct key |
| `testSpeedOnly` | A single speed reading is returned as the average |
| `testSpeedAvg` | Two identical readings produce the correct mean |

**`OutboundKeyVIDDateTest`** covers outbound key serialization:

| Test | Assertion |
|---|---|
| `testConstructor` | Instantiation does not throw |
| `testToGenericRecord` | `vehicleId` and `date` round-trip correctly through `GenericRecord` |

### End-to-End Test (`src/e2e/`)

Run with `mvn verify -Pe2e`. Docker Compose must be running first.

**`TelemetryE2ETest.testAllSpeedEvents`:**
1. Produces a `SPEED`, `ACCELERATION`, and `ODOMETER` event to the inbound topic, all sharing the same `Key`
2. Consumes 3 records from the outbound speed topic (one emitted per inbound event processed)
3. Asserts the final (3rd) record matches the exact speed value, X/Y/Z acceleration values, and a `totalKmDriven` of 0 (only one odometer reading was received, so `currentKm - startKm = 0`)

**Infrastructure helpers:**

- **`MiniCluster`** — wraps Flink's `MiniClusterWithClientResource`, runs `TelemetryJob` on a background thread, exposes REST on port `8082`, and uses Awaitility (60s timeout, 1s poll) to wait for `JobStatus.RUNNING` before tests proceed
- **`KafkaAdminHelper`** — creates topics via Kafka `AdminClient`; silently ignores already-exists errors
- **`SchemaRegistryHelper`** — registers `.avsc` files from disk against the live Schema Registry using `CachedSchemaRegistryClient`
- **`InboundProducer`** — builds complete, valid Avro records for any `TelemetryType` with randomly generated field values; supports an optional fixed `Key` for same-key test scenarios
- **`OutboundConsumer`** — polls the outbound topic until either a deadline or a target record count is reached; uses a random consumer group ID to always read from the earliest offset

---

## Sample Data

`sampleData/` contains hand-crafted JSON payloads matching the Avro schemas, useful for manual testing or seeding via the Kafka CLI.

**Example key (`sampleData/exampleKey`):**
```json
{
  "vehicleId": "vehicle-123",
  "vin": "1FTFW1E50PKE12345",
  "fleetId": "fleet-alpha"
}
```

**Example speed event (`sampleData/exampleValues/vehicle/speed`):**
```json
{
  "eventId": "evt-speed-001",
  "eventTimestamp": 1708000000000,
  "vehicleInfo": { "make": "Ford", "model": "F-150", "year": 2023, "engineType": "V6" },
  "telemetryType": "SPEED",
  "payload": {
    "com.example.avro.SpeedTelemetry": { "speedKph": 112.4 }
  }
}
```

Vehicle sample files: `speed`, `acceleration`, `battery`, `brake`, `diagnostic`, `engine`, `fuel`, `location`, `odometer`, `tirePressure`

Non-vehicle sample files: `infrastructure`, `mobileDevice`, `roadCondition`, `traffic`, `weather`

---

## Known TODOs

| Location | TODO |
|---|---|
| `TelemetryJob` | Enable `EmbeddedRocksDBStateBackend` (currently in-memory only) |
| `TelemetryJob` | Increase parallelism beyond 1 after testing |
| `TelemetryJob` | Remove no-restart strategy; configure a production restart policy |
| `TelemetryJob` | Wire DLQ side output to a Kafka sink |
| `InboundRouter` | Implement tombstone handling per event type (currently all go to DLQ) |
| `InboundRouter` | Route remaining 12 `TelemetryType` values to appropriate processors |
| `KafkaRecord` | Override `toString()` for easier debug logging |
| `SpeedInformation` | Override `toString()` for easier debug logging |
| `ProcessVehicle` | Implement the vehicle `RichMapFunction` (currently returns `null`) |
| `application.properties` | Add DLQ topic configuration |
| `OutboundKeyVIDDate` | Use Avro `timestamp-millis` logical type for the `date` field |
| `SpeedInformationTest` | Add test coverage for X, Y, Z acceleration and km driven |
| `MiniCluster` | Move `isCancellation()` helper to shared infra utility class |