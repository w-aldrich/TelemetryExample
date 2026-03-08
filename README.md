# Vehicle Telemetry Processing Pipeline

## Overview

This project is a real-time vehicle telemetry streaming pipeline built on **Apache Flink** and **Apache Kafka**. It consumes a single inbound Kafka topic carrying 15 distinct telemetry event types from vehicles and surrounding infrastructure, routes each event to the appropriate processing path via Flink side outputs, and publishes enriched aggregation records to typed outbound topics.

The current implementation delivers a **SpeedInformation** aggregation: per-vehicle, per-day rolling averages for speed and 3-axis acceleration, plus total kilometers driven. The architecture is designed so that additional telemetry processors and outbound topics can be wired in incrementally without restructuring the core pipeline.

All inbound and outbound messages are serialized with **Avro** and governed by **Confluent Schema Registry**, giving every schema a versioned, centrally managed contract.

---

## Architecture

```
                        ┌──────────────────────────────────────────────────┐
                        │                  Apache Flink Job                │
                        │                                                  │
Kafka Inbound Topic ───►│  GenericDeserialization                          │
(Avro, 15 types)        │         │                                        │
                        │         ▼                                        │
                        │   InboundRouter (ProcessFunction)                │
                        │    ├── hasDeserializationError? ──► dlq side out │──► (TODO: DLQ Kafka sink)
                        │    ├── isTombstone?             ──► dlq side out │
                        │    │                                             │
                        │    └── TelemetryType switch:                     │
                        │         ├── SPEED   ┐                            │
                        │         ├── ACCEL   ├──► speedInfo side output   │
                        │         ├── ODOM    ┘         │                  │
                        │         └── (12 other types) ─► (no output)      │
                        │                               │                  │
                        │              keyBy(vehicleId_date)               │
                        │                               │                  │
                        │                    ProcessSpeed                  │
                        │              (KeyedProcessFunction)              │
                        │              ValueState<SpeedInformation>        │
                        │              TTL: 24h OnReadAndWrite             │
                        │                               │                  │
                        │                  GenericSerialization            │
                        └───────────────────────────────┼──────────────────┘
                                                        │
                                                        ▼
                                          Kafka Outbound Speed Topic
                                          Key:   vehicleId + UTC date
                                          Value: SpeedInformation (Avro)
```

**Routing key** — `{vehicleId}_{UTC-date}` computed from `eventTimestamp` millis. This scopes all state per vehicle per calendar day, so Flink's keyed partitioning naturally separates aggregation windows without a time window operator.

**State** — `SpeedInformation` is a `ValueState` accumulator holding running sums and counts for speed, X/Y/Z acceleration, and start/current odometer readings. It is updated on every event and emits one outbound record per inbound event, making the latest aggregate always available downstream.

**Dead Letter Queue** — deserialization failures and tombstone records are routed to a `dlq` side output. Kafka sink wiring for the DLQ is a pending TODO.

---

## Project Structure

```
TelemetryExample/
├── schemas/
│   ├── inboundAvsc/
│   │   ├── key.avsc                          # Inbound key: vehicleId, vin, fleetId
│   │   └── value.avsc                        # Inbound value: union of all 15 payload types
│   └── outboundAvsc/
│       ├── key.avsc                          # Outbound key: vehicleId + date
│       └── valueSpeedInformation.avsc        # Outbound value: SpeedInformation aggregate
│
├── sampleData/
│   ├── exampleKey
│   └── exampleValues/
│       ├── vehicle/                          # speed, acceleration, odometer, engine, fuel,
│       │                                     # location, battery, brake, diagnostic, tirePressure
│       └── nonVehicleTelemetry/              # weather, traffic, roadCondition,
│                                             # infrastructure, mobileDevice
│
├── src/
│   ├── main/
│   │   ├── java/com/example/
│   │   │   ├── TelemetryJob.java             # Flink entry point — wires all operators
│   │   │   ├── config/
│   │   │   │   └── AppConfig.java            # Loads application.properties
│   │   │   ├── flink/
│   │   │   │   └── ProcessSpeed.java         # KeyedProcessFunction: speed aggregation
│   │   │   ├── model/
│   │   │   │   ├── InboundRouter.java        # Routes events to side outputs
│   │   │   │   ├── KafkaRecord.java          # Internal wrapper: key + value + routing key
│   │   │   │   ├── PartitionOffset.java
│   │   │   │   ├── outbound/
│   │   │   │   │   ├── Outbound.java         # Interface: toKafkaRecord()
│   │   │   │   │   ├── OutboundKeyVIDDate.java
│   │   │   │   │   ├── SpeedInformation.java # Accumulator state: sums, counts, km
│   │   │   │   │   └── SpeedInformationOutbound.java
│   │   │   │   └── telemetryEnums/
│   │   │   │       ├── TelemetryType.java    # 15-value enum + schema name helper
│   │   │   │       └── Status.java
│   │   │   └── util/
│   │   │       ├── deserialization/
│   │   │       │   ├── GenericDeserialization.java   # KafkaRecordDeserializationSchema
│   │   │       │   └── errors/DeserializationError.java
│   │   │       ├── serialization/
│   │   │       │   └── GenericSerialization.java     # KafkaRecordSerializationSchema
│   │   │       └── helpers/
│   │   │           └── GenericRecordHelper.java      # POJO → GenericRecord via reflection
│   │   └── resources/
│   │       └── application.properties
│   │
│   ├── test/
│   │   ├── java/com/example/
│   │   │   ├── SpeedInformationTest.java     # Unit tests: accumulator logic
│   │   │   ├── OutboundKeyVIDDateTest.java   # Unit tests: outbound key serialization
│   │   │   └── utils/
│   │   │       └── RandomGenerator.java      # Test data helpers (shared with E2E)
│   │
│   └── e2e/
│       ├── java/com/example/telemetry/
│       │   ├── TelemetryE2ETest.java         # Full pipeline E2E test suite
│       │   ├── infraSetup/
│       │   │   ├── MiniCluster.java          # Embeds Flink MiniClusterWithClientResource
│       │   │   ├── KafkaAdminHelper.java     # Topic creation
│       │   │   └── SchemaRegistryHelper.java # Schema registration
│       │   ├── telemetryEvents/              # Event POJOs for test data generation
│       │   │   ├── Key.java / BaseEvent.java / VehicleInfo.java
│       │   │   ├── vehicle/                  # Speed, Acceleration, Odometer, Engine, ...
│       │   │   └── nonVehicle/               # Weather, Traffic, RoadCondition, ...
│       │   └── utils/
│       │       ├── InboundProducer.java      # Produces typed Avro events to Kafka
│       │       └── OutboundConsumer.java     # Consumes output, filtered by vehicleId
│       └── resources/
│           └── docker-compose.yml            # Kafka + Zookeeper + Schema Registry + Control Center
│
└── pom.xml
```

---

## Tech Stack & Dependencies

| Dependency | Version | Purpose |
|---|---|---|
| Java | 17 | Language runtime |
| Apache Flink | 1.18.1 | Stream processing engine |
| flink-connector-kafka | 3.0.2-1.18 | Kafka source and sink |
| flink-statebackend-rocksdb | 1.18.1 | RocksDB state backend (configured, not yet enabled) |
| Apache Kafka clients | 3.7.0 | Producer/consumer used in E2E tests |
| Apache Avro | 1.11.3 | Schema-based serialization |
| Confluent Platform | 7.5.0 | Schema Registry client + Avro serializer/deserializer |
| Awaitility | 4.2.1 | Async condition polling in E2E tests |
| JUnit Jupiter | 5.10.2 | Unit and E2E test framework |
| flink-test-utils | 1.18.1 | `MiniClusterWithClientResource` for E2E tests |
| Log4j2 | 2.22.1 | Logging (test scope) |

All schemas are resolved at runtime from the `schemas/` directory relative to the working directory. The Confluent repository (`https://packages.confluent.io/maven/`) is required and declared in `pom.xml`.

---

## Prerequisites

- **Java 17**
- **Maven 3.8+**
- **Docker** and **Docker Compose** — required only for E2E tests (brings up Kafka, Zookeeper, Schema Registry, and Confluent Control Center)

---

## Getting Started

### 1. Clone and build

```bash
git clone <repo-url>
cd TelemetryExample
mvn clean package
```

The build excludes E2E tests by default. Unit tests run as part of `package`.

### 2. Configure

Edit `src/main/resources/application.properties` before running:

```properties
# Kafka broker
kafka.bootstrap.servers=localhost:9092

# Confluent Schema Registry
schema.registry.url=http://localhost:8081

# Topics
telemetry.inbound.topic=someTopic
telemetry.outbound.speed.topic=someOutboundTopic

# Consumer group
kafka.consumer.group.id=telemetry-flink-group

# Flink
flink.checkpoint.interval.ms=10000
flink.parallelism=1

# State TTL
state.backend=rocksdb
state.ttl.minues=10
```

> **Note:** `flink.parallelism` is set to `1` for development. Increase this when promoting to a multi-node environment. `EmbeddedRocksDBStateBackend` is declared in `TelemetryJob` but commented out — the job currently runs with in-memory state.

### 3. Register schemas and create topics

Before starting the job, register both inbound and outbound schemas with Schema Registry and ensure the configured topics exist. The E2E test helpers (`KafkaAdminHelper`, `SchemaRegistryHelper`) can serve as a reference for how to do this programmatically.

### 4. Run the job

```bash
java -jar target/vehicle-telemetry-1.0-SNAPSHOT.jar
```

> The `--add-opens` JVM flags required for Avro/Kryo serialization on Java 17 are documented inside `TelemetryJob.java` and are pre-configured in the E2E Maven profile's `argLine`. Add them to your `java` invocation if running outside Maven.

---

## Testing

### Unit tests

Unit tests live in `src/test/` and run with the default Maven lifecycle. They have no external dependencies — schemas are read directly from the `schemas/` directory at the project root.

```bash
mvn test
```

**`SpeedInformationTest`** covers the `SpeedInformation` accumulator in isolation:

- Constructor and default zero state
- Single speed reading returned directly
- Average speed across multiple readings
- Single acceleration reading (X/Y/Z) returned directly via the `count <= 1` path
- Average X, Y, Z across two readings independently
- Single odometer reading sets baseline — `totalKmDriven` is `0`
- Two odometer readings compute `totalKmDriven = currentKm - startKm`

**`OutboundKeyVIDDateTest`** covers outbound key construction and `GenericRecord` serialization.

### E2E tests

E2E tests live in `src/e2e/` and require Docker. 

```bash
docker compose -f src/e2e/resources/docker-compose.yml up -d
```

```bash
mvn verify -Pe2e
```

```bash
docker compose -f src/e2e/resources/docker-compose.yml down
```

The profile automatically injects these system properties into the test JVM:

```
kafka.bootstrap.servers=localhost:9092
schema.registry.url=http://localhost:8081
```

The Flink job runs inside a `MiniClusterWithClientResource` embedded in the test JVM. `InboundProducer` sends typed Avro events to Kafka; `OutboundConsumer` reads the outbound topic filtered by `vehicleId`, ensuring each test sees only its own records regardless of what other tests have produced.

**`TelemetryE2ETest` coverage:**

| Test | What is verified |
|---|---|
| `testSpeedEventIsRouted` | SPEED produces 1 outbound record; `averageSpeed` matches inbound |
| `testAccelerationEventIsRouted` | ACCELERATION produces 1 outbound record; X/Y/Z match inbound |
| `testOdometerEventIsRouted` | ODOMETER produces 1 outbound record; `totalKmDriven` is `0` |
| `testNonRoutedTypesProduceNoOutput` | All 12 non-routed types produce no outbound records |
| `testSingleSpeedEvent` | Single SPEED: speed field matches, all other fields are zero |
| `testMultipleSpeedEventsAverage` | Two SPEED events: `averageSpeed` equals their mean |
| `testThreeSpeedEventsAverage` | Three SPEED events: running average accumulates correctly |
| `testSingleAccelerationEvent` | Single ACCELERATION: X/Y/Z returned directly (`count <= 1` path) |
| `testMultipleAccelerationEventsAverage` | Two ACCELERATION events: X, Y, Z each averaged independently |
| `testSingleOdometerEventProducesZeroKm` | One odometer reading: `totalKmDriven` is `0` |
| `testTwoOdometerEventsTotalKmDriven` | Two odometer readings: `totalKmDriven = currentKm - startKm` |
| `testAllThreeRoutedTypesWithSameKey` | SPEED + ACCELERATION + ODOMETER same key: final record has all fields |
| `testFullHappyPath` | 2 SPEED + 1 ACCELERATION + 2 ODOMETER: all averages and km driven correct |
| `testDifferentKeysHaveIndependentState` | Two vehicle keys accumulate state independently |
| `testInterleavedKeysRemainIsolated` | Interleaved events from two keys do not bleed state between them |
| `testOneOutboundRecordPerRoutedEvent` | 5 events emit exactly 5 outbound records (one-per-event contract) |
| `testMixedRoutedAndNonRoutedTypes` | 2 routed + 3 non-routed events produce exactly 2 outbound records |

The Confluent Control Center UI is available at `http://localhost:9021` while the Docker stack is running and can be used to inspect topics and schemas during a test run.

---