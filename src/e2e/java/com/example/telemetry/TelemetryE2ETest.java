package com.example.telemetry;

import com.example.config.AppConfig;
import com.example.model.KafkaRecord;
import com.example.model.telemetryEnums.TelemetryType;
import com.example.telemetry.infraSetup.KafkaAdminHelper;
import com.example.telemetry.infraSetup.MiniCluster;
import com.example.telemetry.infraSetup.SchemaRegistryHelper;
import com.example.telemetry.telemetryEvents.Key;
import com.example.telemetry.utils.InboundProducer;
import com.example.telemetry.utils.OutboundConsumer;
import com.example.utils.RandomGenerator;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;

/**
 * Full end-to-end test suite for the Vehicle Telemetry Flink pipeline.
 *
 * Permutations covered:
 *
 *  Routing:
 *    - SPEED, ACCELERATION, ODOMETER are routed to the speed outbound topic
 *    - All other 12 TelemetryTypes (ENGINE, FUEL, LOCATION, TIRE_PRESSURE, BATTERY,
 *      BRAKE, DIAGNOSTIC, WEATHER, ROAD_CONDITION, TRAFFIC, INFRASTRUCTURE, MOBILE_DEVICE)
 *      produce no outbound record (fall-through in InboundRouter switch)
 *
 *  ProcessSpeed state accumulation:
 *    - Single SPEED event: averageSpeed == speedKph, all others zero
 *    - Multiple SPEED events: averageSpeed is the mean across all readings
 *    - Single ACCELERATION event: X/Y/Z returned directly (count <= 1 path)
 *    - Multiple ACCELERATION events: X/Y/Z are averaged correctly
 *    - Single ODOMETER event: totalKmDriven == 0 (only baseline set, no current)
 *    - Two ODOMETER events: totalKmDriven == currentKm - startKm
 *    - All three routed types together, same key: final record contains all aggregated values
 *
 *  Key isolation:
 *    - Two different keys produce independent state; one key's events do not affect the other
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TelemetryE2ETest {

    private static final AppConfig APP_CONFIG = new AppConfig();
    private static final String BOOTSTRAP = APP_CONFIG.getKafkaBootstrapServers();
    private static final String REGISTRY = APP_CONFIG.getSchemaRegistryUrl();
    private static final String INBOUND_TOPIC = APP_CONFIG.getInboundTopic();
    private static final String OUTBOUND_SPEED_TOPIC = APP_CONFIG.getOutboundSpeedTopic();
    private static final String INBOUND_AVSC_KEY = "schemas/inboundAvsc/key.avsc";
    private static final String INBOUND_AVSC_VALUE = "schemas/inboundAvsc/value.avsc";
    private static final String OUTBOUND_AVSC_KEY = "schemas/outboundAvsc/key.avsc";
    private static final String OUTBOUND_AVSC_VALUE_SPEED_INFORMATION = "schemas/outboundAvsc/valueSpeedInformation.avsc";

    // How long to wait after producing before consuming — gives Flink time to process
    private static final long PROCESSING_WAIT_MS = 1000;
    // Max time to wait when consuming an expected number of records
    private static final long CONSUME_TIMEOUT_MS = 5000;
    // Max time to wait when asserting NO records appear for a given key
    private static final long NO_RECORD_TIMEOUT_MS = 3000;

    private MiniCluster miniCluster;

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    private void createTopicsAndSchemas() {
        try {
            KafkaAdminHelper.createTopic(BOOTSTRAP, INBOUND_TOPIC);
            KafkaAdminHelper.createTopic(BOOTSTRAP, OUTBOUND_SPEED_TOPIC);
        } catch (Exception e) {
            System.out.println("[E2E] Topic create (may already exist): " + e.getMessage());
        }
        try {
            SchemaRegistryHelper.registerSchema(REGISTRY, INBOUND_TOPIC + "-key", INBOUND_AVSC_KEY);
            SchemaRegistryHelper.registerSchema(REGISTRY, INBOUND_TOPIC + "-value", INBOUND_AVSC_VALUE);
            SchemaRegistryHelper.registerSchema(REGISTRY, OUTBOUND_SPEED_TOPIC + "-key", OUTBOUND_AVSC_KEY);
            SchemaRegistryHelper.registerSchema(REGISTRY, OUTBOUND_SPEED_TOPIC + "-value", OUTBOUND_AVSC_VALUE_SPEED_INFORMATION);
        } catch (Exception e) {
            System.out.println("[E2E] Schema register (may already exist): " + e.getMessage());
        }
    }

    // !!! Must start docker-compose file first !!!
    @BeforeAll
    public void setup() throws Exception {
        createTopicsAndSchemas();
        miniCluster = new MiniCluster();
        miniCluster.waitForJobStartup();
    }

    @AfterAll
    public void tearDown() throws Exception {
        miniCluster.shutdown();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Creates a fresh producer for each test. */
    private InboundProducer producer() throws Exception {
        return new InboundProducer(BOOTSTRAP, REGISTRY, INBOUND_TOPIC, INBOUND_AVSC_KEY, INBOUND_AVSC_VALUE);
    }

    /**
     * Creates a filtered consumer that only returns records matching the given vehicleId.
     * Use this in every single-key test so records from other concurrent tests are invisible.
     */
    private OutboundConsumer consumer(String vehicleId) {
        return new OutboundConsumer(BOOTSTRAP, REGISTRY, OUTBOUND_SPEED_TOPIC, RandomGenerator.generateString(10), vehicleId);
    }

    /**
     * Creates an unfiltered consumer that returns all records regardless of key.
     * Use only in multi-key tests that need to observe output for multiple vehicle IDs.
     */
    private OutboundConsumer consumerUnfiltered() {
        return new OutboundConsumer(BOOTSTRAP, REGISTRY, OUTBOUND_SPEED_TOPIC, RandomGenerator.generateString(10));
    }

    private GenericRecord getPayload(GenericRecord gr) {
        return (GenericRecord) gr.get("payload");
    }

    // --- Inbound field extractors ---
    private double getInboundSpeed(GenericRecord gr) { return Double.parseDouble(getPayload(gr).get("speedKph").toString()); }
    private double getInboundX(GenericRecord gr)     { return Double.parseDouble(getPayload(gr).get("xAxis").toString()); }
    private double getInboundY(GenericRecord gr)     { return Double.parseDouble(getPayload(gr).get("yAxis").toString()); }
    private double getInboundZ(GenericRecord gr)     { return Double.parseDouble(getPayload(gr).get("zAxis").toString()); }
    private double getInboundKm(GenericRecord gr)    { return Double.parseDouble(getPayload(gr).get("totalKilometers").toString()); }

    // --- Outbound field extractors ---
    private double getAvgSpeed(GenericRecord gr)     { return Double.parseDouble(gr.get("averageSpeed").toString()); }
    private double getAvgX(GenericRecord gr)         { return Double.parseDouble(gr.get("averageXAcceleration").toString()); }
    private double getAvgY(GenericRecord gr)         { return Double.parseDouble(gr.get("averageYAcceleration").toString()); }
    private double getAvgZ(GenericRecord gr)         { return Double.parseDouble(gr.get("averageZAcceleration").toString()); }
    private double getTotalKm(GenericRecord gr)      { return Double.parseDouble(gr.get("totalKmDriven").toString()); }

    // -------------------------------------------------------------------------
    // Routing tests — which types reach the outbound speed topic
    // -------------------------------------------------------------------------

    /**
     * SPEED events must be routed to the speed outbound topic and produce one record.
     */
    @Test
    public void testSpeedEventIsRouted() throws Exception {
        InboundProducer p = producer();
        Key key = new Key();

        KafkaRecord inbound = p.sendInboundEvent(TelemetryType.SPEED, Optional.of(key));
        p.closeProducer();
        Thread.sleep(PROCESSING_WAIT_MS);

        OutboundConsumer c = consumer(key.getVehicleId());
        List<KafkaRecord> results = c.consume(1, CONSUME_TIMEOUT_MS);
        c.close();

        assert results.size() == 1 : "Expected 1 outbound record for SPEED, got " + results.size();
        assert getAvgSpeed(results.get(0).getValue()) == getInboundSpeed(inbound.getValue())
                : "averageSpeed should equal the single speed reading";
    }

    /**
     * ACCELERATION events must be routed to the speed outbound topic and produce one record.
     */
    @Test
    public void testAccelerationEventIsRouted() throws Exception {
        InboundProducer p = producer();
        Key key = new Key();

        KafkaRecord inbound = p.sendInboundEvent(TelemetryType.ACCELERATION, Optional.of(key));
        p.closeProducer();
        Thread.sleep(PROCESSING_WAIT_MS);

        OutboundConsumer c = consumer(key.getVehicleId());
        List<KafkaRecord> results = c.consume(1, CONSUME_TIMEOUT_MS);
        c.close();

        assert results.size() == 1 : "Expected 1 outbound record for ACCELERATION, got " + results.size();
        assert getAvgX(results.get(0).getValue()) == getInboundX(inbound.getValue())
                : "averageXAcceleration should equal the single X reading";
        assert getAvgY(results.get(0).getValue()) == getInboundY(inbound.getValue())
                : "averageYAcceleration should equal the single Y reading";
        assert getAvgZ(results.get(0).getValue()) == getInboundZ(inbound.getValue())
                : "averageZAcceleration should equal the single Z reading";
    }

    /**
     * ODOMETER events must be routed to the speed outbound topic and produce one record.
     * A single odometer reading sets only the baseline so totalKmDriven is 0.
     */
    @Test
    public void testOdometerEventIsRouted() throws Exception {
        InboundProducer p = producer();
        Key key = new Key();

        p.sendInboundEvent(TelemetryType.ODOMETER, Optional.of(key));
        p.closeProducer();
        Thread.sleep(PROCESSING_WAIT_MS);

        OutboundConsumer c = consumer(key.getVehicleId());
        List<KafkaRecord> results = c.consume(1, CONSUME_TIMEOUT_MS);
        c.close();

        assert results.size() == 1 : "Expected 1 outbound record for ODOMETER, got " + results.size();
        assert getTotalKm(results.get(0).getValue()) == 0.0
                : "totalKmDriven should be 0 with only a baseline odometer reading";
    }

    /**
     * The 12 non-routed TelemetryTypes must not produce any outbound record on the speed topic.
     * Each is sent with a unique key and the consumer is filtered to that key, so only
     * records for this test's own vehicles are checked.
     */
    @Test
    public void testNonRoutedTypesProduceNoOutput() throws Exception {
        TelemetryType[] nonRouted = {
                TelemetryType.ENGINE,
                TelemetryType.FUEL,
                TelemetryType.LOCATION,
                TelemetryType.TIRE_PRESSURE,
                TelemetryType.BATTERY,
                TelemetryType.BRAKE,
                TelemetryType.DIAGNOSTIC,
                TelemetryType.WEATHER,
                TelemetryType.ROAD_CONDITION,
                TelemetryType.TRAFFIC,
                TelemetryType.INFRASTRUCTURE,
                TelemetryType.MOBILE_DEVICE
        };

        for (TelemetryType type : nonRouted) {
            Key key = new Key();
            InboundProducer p = producer();
            p.sendInboundEvent(type, Optional.of(key));
            p.closeProducer();

            Thread.sleep(PROCESSING_WAIT_MS);

            // Filter to this specific vehicle — if anything slips through routing it will show here
            OutboundConsumer c = consumer(key.getVehicleId());
            List<KafkaRecord> results = c.consumeAvailable(NO_RECORD_TIMEOUT_MS);
            c.close();

            assert results.isEmpty()
                    : "Expected 0 outbound records for " + type + ", got " + results.size();
        }
    }

    // -------------------------------------------------------------------------
    // ProcessSpeed — SPEED accumulation
    // -------------------------------------------------------------------------

    /**
     * A single SPEED event: averageSpeed equals the single reading.
     * All other outbound fields should be zero.
     */
    @Test
    public void testSingleSpeedEvent() throws Exception {
        InboundProducer p = producer();
        Key key = new Key();

        KafkaRecord inbound = p.sendInboundEvent(TelemetryType.SPEED, Optional.of(key));
        p.closeProducer();
        Thread.sleep(PROCESSING_WAIT_MS);

        OutboundConsumer c = consumer(key.getVehicleId());
        List<KafkaRecord> results = c.consume(1, CONSUME_TIMEOUT_MS);
        c.close();

        assert results.size() == 1;
        GenericRecord out = results.get(0).getValue();
        assert getAvgSpeed(out) == getInboundSpeed(inbound.getValue())
                : "averageSpeed should match the single inbound reading";
        assert getAvgX(out) == 0.0 : "averageXAcceleration should be 0 with no acc events";
        assert getAvgY(out) == 0.0 : "averageYAcceleration should be 0 with no acc events";
        assert getAvgZ(out) == 0.0 : "averageZAcceleration should be 0 with no acc events";
        assert getTotalKm(out) == 0.0 : "totalKmDriven should be 0 with no odometer events";
    }

    /**
     * Two SPEED events with the same key: averageSpeed equals the mean of both readings.
     * Two outbound records are emitted (one per inbound event).
     * The second record carries the running average.
     */
    @Test
    public void testMultipleSpeedEventsAverage() throws Exception {
        InboundProducer p = producer();
        Key key = new Key();

        KafkaRecord inbound1 = p.sendInboundEvent(TelemetryType.SPEED, Optional.of(key));
        KafkaRecord inbound2 = p.sendInboundEvent(TelemetryType.SPEED, Optional.of(key));
        p.closeProducer();
        Thread.sleep(PROCESSING_WAIT_MS);

        OutboundConsumer c = consumer(key.getVehicleId());
        List<KafkaRecord> results = c.consume(2, CONSUME_TIMEOUT_MS);
        c.close();

        assert results.size() == 2 : "Expected 2 outbound records, got " + results.size();

        double speed1 = getInboundSpeed(inbound1.getValue());
        double speed2 = getInboundSpeed(inbound2.getValue());
        double expectedAvg = (speed1 + speed2) / 2.0;

        GenericRecord finalOut = results.get(1).getValue();
        assert getAvgSpeed(finalOut) == expectedAvg
                : "averageSpeed should be the mean of both readings. Expected: " + expectedAvg
                + " Got: " + getAvgSpeed(finalOut);
    }

    /**
     * Three SPEED events with the same key: confirms the average continues to accumulate
     * correctly beyond two readings.
     */
    @Test
    public void testThreeSpeedEventsAverage() throws Exception {
        InboundProducer p = producer();
        Key key = new Key();

        KafkaRecord inbound1 = p.sendInboundEvent(TelemetryType.SPEED, Optional.of(key));
        KafkaRecord inbound2 = p.sendInboundEvent(TelemetryType.SPEED, Optional.of(key));
        KafkaRecord inbound3 = p.sendInboundEvent(TelemetryType.SPEED, Optional.of(key));
        p.closeProducer();
        Thread.sleep(PROCESSING_WAIT_MS);

        OutboundConsumer c = consumer(key.getVehicleId());
        List<KafkaRecord> results = c.consume(3, CONSUME_TIMEOUT_MS);
        c.close();

        assert results.size() == 3 : "Expected 3 outbound records, got " + results.size();

        double speed1 = getInboundSpeed(inbound1.getValue());
        double speed2 = getInboundSpeed(inbound2.getValue());
        double speed3 = getInboundSpeed(inbound3.getValue());
        double expectedAvg = (speed1 + speed2 + speed3) / 3.0;

        GenericRecord finalOut = results.get(2).getValue();
        assert getAvgSpeed(finalOut) == expectedAvg
                : "averageSpeed should be the mean of three readings. Expected: " + expectedAvg
                + " Got: " + getAvgSpeed(finalOut);
    }

    // -------------------------------------------------------------------------
    // ProcessSpeed — ACCELERATION accumulation
    // -------------------------------------------------------------------------

    /**
     * A single ACCELERATION event: X, Y, Z are each returned directly (count <= 1 path).
     */
    @Test
    public void testSingleAccelerationEvent() throws Exception {
        InboundProducer p = producer();
        Key key = new Key();

        KafkaRecord inbound = p.sendInboundEvent(TelemetryType.ACCELERATION, Optional.of(key));
        p.closeProducer();
        Thread.sleep(PROCESSING_WAIT_MS);

        OutboundConsumer c = consumer(key.getVehicleId());
        List<KafkaRecord> results = c.consume(1, CONSUME_TIMEOUT_MS);
        c.close();

        assert results.size() == 1;
        GenericRecord out = results.get(0).getValue();
        assert getAvgX(out) == getInboundX(inbound.getValue())
                : "averageXAcceleration should equal the single X reading";
        assert getAvgY(out) == getInboundY(inbound.getValue())
                : "averageYAcceleration should equal the single Y reading";
        assert getAvgZ(out) == getInboundZ(inbound.getValue())
                : "averageZAcceleration should equal the single Z reading";
        assert getAvgSpeed(out) == 0.0 : "averageSpeed should be 0 with no speed events";
        assert getTotalKm(out) == 0.0 : "totalKmDriven should be 0 with no odometer events";
    }

    /**
     * Two ACCELERATION events with the same key: X, Y, Z are each averaged across both readings.
     */
    @Test
    public void testMultipleAccelerationEventsAverage() throws Exception {
        InboundProducer p = producer();
        Key key = new Key();

        KafkaRecord inbound1 = p.sendInboundEvent(TelemetryType.ACCELERATION, Optional.of(key));
        KafkaRecord inbound2 = p.sendInboundEvent(TelemetryType.ACCELERATION, Optional.of(key));
        p.closeProducer();
        Thread.sleep(PROCESSING_WAIT_MS);

        OutboundConsumer c = consumer(key.getVehicleId());
        List<KafkaRecord> results = c.consume(2, CONSUME_TIMEOUT_MS);
        c.close();

        assert results.size() == 2 : "Expected 2 outbound records, got " + results.size();

        double expectedX = (getInboundX(inbound1.getValue()) + getInboundX(inbound2.getValue())) / 2.0;
        double expectedY = (getInboundY(inbound1.getValue()) + getInboundY(inbound2.getValue())) / 2.0;
        double expectedZ = (getInboundZ(inbound1.getValue()) + getInboundZ(inbound2.getValue())) / 2.0;

        GenericRecord finalOut = results.get(1).getValue();
        assert getAvgX(finalOut) == expectedX
                : "averageXAcceleration should be mean of two readings. Expected: " + expectedX;
        assert getAvgY(finalOut) == expectedY
                : "averageYAcceleration should be mean of two readings. Expected: " + expectedY;
        assert getAvgZ(finalOut) == expectedZ
                : "averageZAcceleration should be mean of two readings. Expected: " + expectedZ;
    }

    // -------------------------------------------------------------------------
    // ProcessSpeed — ODOMETER accumulation
    // -------------------------------------------------------------------------

    /**
     * A single ODOMETER event sets only the startKm baseline.
     * totalKmDriven must be 0 because there is no current reading yet.
     */
    @Test
    public void testSingleOdometerEventProducesZeroKm() throws Exception {
        InboundProducer p = producer();
        Key key = new Key();

        p.sendInboundEvent(TelemetryType.ODOMETER, Optional.of(key));
        p.closeProducer();
        Thread.sleep(PROCESSING_WAIT_MS);

        OutboundConsumer c = consumer(key.getVehicleId());
        List<KafkaRecord> results = c.consume(1, CONSUME_TIMEOUT_MS);
        c.close();

        assert results.size() == 1;
        assert getTotalKm(results.get(0).getValue()) == 0.0
                : "totalKmDriven should be 0 with only a baseline odometer reading";
    }

    /**
     * Two ODOMETER events with the same key: totalKmDriven == secondReading - firstReading.
     */
    @Test
    public void testTwoOdometerEventsTotalKmDriven() throws Exception {
        InboundProducer p = producer();
        Key key = new Key();

        KafkaRecord inbound1 = p.sendInboundEvent(TelemetryType.ODOMETER, Optional.of(key));
        KafkaRecord inbound2 = p.sendInboundEvent(TelemetryType.ODOMETER, Optional.of(key));
        p.closeProducer();
        Thread.sleep(PROCESSING_WAIT_MS);

        OutboundConsumer c = consumer(key.getVehicleId());
        List<KafkaRecord> results = c.consume(2, CONSUME_TIMEOUT_MS);
        c.close();

        assert results.size() == 2 : "Expected 2 outbound records, got " + results.size();

        double startKm   = getInboundKm(inbound1.getValue());
        double currentKm = getInboundKm(inbound2.getValue());

        // Only assert non-zero total when currentKm > 1 (matches the getTotalDriven guard)
        GenericRecord finalOut = results.get(1).getValue();
        if (currentKm > 1) {
            double expectedTotal = currentKm - startKm;
            assert getTotalKm(finalOut) == expectedTotal
                    : "totalKmDriven should be currentKm - startKm. Expected: " + expectedTotal
                    + " Got: " + getTotalKm(finalOut);
        } else {
            assert getTotalKm(finalOut) == 0.0
                    : "totalKmDriven should be 0 when currentKm <= 1";
        }
    }

    // -------------------------------------------------------------------------
    // ProcessSpeed — combined events, same key
    // -------------------------------------------------------------------------

    /**
     * All three routed types sent with the same key.
     * Three outbound records emitted. The final record must contain all aggregated values:
     * averageSpeed from SPEED, X/Y/Z from ACCELERATION, totalKmDriven == 0 (baseline only).
     */
    @Test
    public void testAllThreeRoutedTypesWithSameKey() throws Exception {
        InboundProducer p = producer();
        Key key = new Key();

        KafkaRecord inboundSpeed = p.sendInboundEvent(TelemetryType.SPEED,        Optional.of(key));
        KafkaRecord inboundAcc   = p.sendInboundEvent(TelemetryType.ACCELERATION,  Optional.of(key));
        p.sendInboundEvent(TelemetryType.ODOMETER,      Optional.of(key));
        p.closeProducer();
        Thread.sleep(PROCESSING_WAIT_MS);

        OutboundConsumer c = consumer(key.getVehicleId());
        List<KafkaRecord> results = c.consume(3, CONSUME_TIMEOUT_MS);
        c.close();

        assert results.size() == 3 : "Expected 3 outbound records, got " + results.size();

        GenericRecord finalOut = results.get(2).getValue();
        assert getAvgSpeed(finalOut) == getInboundSpeed(inboundSpeed.getValue())
                : "averageSpeed should match the SPEED event";
        assert getAvgX(finalOut) == getInboundX(inboundAcc.getValue())
                : "averageXAcceleration should match the ACCELERATION event";
        assert getAvgY(finalOut) == getInboundY(inboundAcc.getValue())
                : "averageYAcceleration should match the ACCELERATION event";
        assert getAvgZ(finalOut) == getInboundZ(inboundAcc.getValue())
                : "averageZAcceleration should match the ACCELERATION event";
        assert getTotalKm(finalOut) == 0.0
                : "totalKmDriven should be 0 with only one odometer reading";
    }

    /**
     * Full happy path: 2 SPEED + 1 ACCELERATION + 2 ODOMETER events on the same key.
     * Verifies speed averaged, X/Y/Z from single acc, and km driven computed.
     */
    @Test
    public void testFullHappyPath() throws Exception {
        InboundProducer p = producer();
        Key key = new Key();

        KafkaRecord inboundSpeed1 = p.sendInboundEvent(TelemetryType.SPEED,        Optional.of(key));
        KafkaRecord inboundSpeed2 = p.sendInboundEvent(TelemetryType.SPEED,        Optional.of(key));
        KafkaRecord inboundAcc    = p.sendInboundEvent(TelemetryType.ACCELERATION,  Optional.of(key));
        KafkaRecord inboundOdom1  = p.sendInboundEvent(TelemetryType.ODOMETER,      Optional.of(key));
        KafkaRecord inboundOdom2  = p.sendInboundEvent(TelemetryType.ODOMETER,      Optional.of(key));
        p.closeProducer();
        Thread.sleep(PROCESSING_WAIT_MS);

        OutboundConsumer c = consumer(key.getVehicleId());
        List<KafkaRecord> results = c.consume(5, CONSUME_TIMEOUT_MS);
        c.close();

        assert results.size() == 5 : "Expected 5 outbound records, got " + results.size();

        GenericRecord finalOut = results.get(4).getValue();

        double expectedAvgSpeed = (getInboundSpeed(inboundSpeed1.getValue()) + getInboundSpeed(inboundSpeed2.getValue())) / 2.0;
        assert getAvgSpeed(finalOut) == expectedAvgSpeed
                : "averageSpeed should be the mean of two SPEED readings. Expected: " + expectedAvgSpeed;

        assert getAvgX(finalOut) == getInboundX(inboundAcc.getValue())
                : "averageXAcceleration should match the single ACCELERATION event";
        assert getAvgY(finalOut) == getInboundY(inboundAcc.getValue())
                : "averageYAcceleration should match the single ACCELERATION event";
        assert getAvgZ(finalOut) == getInboundZ(inboundAcc.getValue())
                : "averageZAcceleration should match the single ACCELERATION event";

        double startKm   = getInboundKm(inboundOdom1.getValue());
        double currentKm = getInboundKm(inboundOdom2.getValue());
        if (currentKm > 1) {
            assert getTotalKm(finalOut) == currentKm - startKm
                    : "totalKmDriven should be currentKm - startKm";
        } else {
            assert getTotalKm(finalOut) == 0.0;
        }
    }

    // -------------------------------------------------------------------------
    // Key isolation — independent state per vehicle
    // -------------------------------------------------------------------------

    /**
     * Two different keys produce independent state.
     * Uses consumerUnfiltered() then partitions by vehicleId since we intentionally
     * need to observe output for both keys at once.
     */
    @Test
    public void testDifferentKeysHaveIndependentState() throws Exception {
        InboundProducer p = producer();
        Key keyA = new Key();
        Key keyB = new Key();

        KafkaRecord speedA1 = p.sendInboundEvent(TelemetryType.SPEED, Optional.of(keyA));
        KafkaRecord speedA2 = p.sendInboundEvent(TelemetryType.SPEED, Optional.of(keyA));
        KafkaRecord speedB  = p.sendInboundEvent(TelemetryType.SPEED, Optional.of(keyB));
        p.closeProducer();
        Thread.sleep(PROCESSING_WAIT_MS);

        // Consume for each key independently using filtered consumers
        OutboundConsumer cA = consumer(keyA.getVehicleId());
        List<KafkaRecord> resultsA = cA.consume(2, CONSUME_TIMEOUT_MS);
        cA.close();

        OutboundConsumer cB = consumer(keyB.getVehicleId());
        List<KafkaRecord> resultsB = cB.consume(1, CONSUME_TIMEOUT_MS);
        cB.close();

        assert resultsA.size() == 2 : "Expected 2 outbound records for key A, got " + resultsA.size();
        assert resultsB.size() == 1 : "Expected 1 outbound record for key B, got " + resultsB.size();

        double expectedAvgA = (getInboundSpeed(speedA1.getValue()) + getInboundSpeed(speedA2.getValue())) / 2.0;
        assert getAvgSpeed(resultsA.get(1).getValue()) == expectedAvgA
                : "Key A averageSpeed should be the mean of its two readings. Expected: " + expectedAvgA;

        double expectedSpeedB = getInboundSpeed(speedB.getValue());
        assert getAvgSpeed(resultsB.get(0).getValue()) == expectedSpeedB
                : "Key B averageSpeed should equal its single reading. Expected: " + expectedSpeedB;
    }

    /**
     * Interleaved events from two different keys: verifies that state accumulation
     * remains isolated even when events from different keys arrive in alternating order.
     */
    @Test
    public void testInterleavedKeysRemainIsolated() throws Exception {
        InboundProducer p = producer();
        Key keyA = new Key();
        Key keyB = new Key();

        KafkaRecord accA    = p.sendInboundEvent(TelemetryType.ACCELERATION, Optional.of(keyA));
        KafkaRecord speedB1 = p.sendInboundEvent(TelemetryType.SPEED,        Optional.of(keyB));
        KafkaRecord speedA  = p.sendInboundEvent(TelemetryType.SPEED,        Optional.of(keyA));
        KafkaRecord speedB2 = p.sendInboundEvent(TelemetryType.SPEED,        Optional.of(keyB));
        p.closeProducer();
        Thread.sleep(PROCESSING_WAIT_MS);

        // Consume each key independently with filtered consumers
        OutboundConsumer cA = consumer(keyA.getVehicleId());
        List<KafkaRecord> resultsA = cA.consume(2, CONSUME_TIMEOUT_MS);
        cA.close();

        OutboundConsumer cB = consumer(keyB.getVehicleId());
        List<KafkaRecord> resultsB = cB.consume(2, CONSUME_TIMEOUT_MS);
        cB.close();

        assert resultsA.size() == 2 : "Expected 2 records for key A, got " + resultsA.size();
        assert resultsB.size() == 2 : "Expected 2 records for key B, got " + resultsB.size();

        // Key A's final record: should have the ACCELERATION values and the SPEED value
        GenericRecord finalA = resultsA.get(1).getValue();
        assert getAvgX(finalA) == getInboundX(accA.getValue())
                : "Key A: X should come only from key A's acceleration event";
        assert getAvgSpeed(finalA) == getInboundSpeed(speedA.getValue())
                : "Key A: speed should come only from key A's speed event";

        // Key B's final record: should have the average of its two speed readings, no acc
        double expectedAvgSpeedB = (getInboundSpeed(speedB1.getValue()) + getInboundSpeed(speedB2.getValue())) / 2.0;
        GenericRecord finalB = resultsB.get(1).getValue();
        assert getAvgSpeed(finalB) == expectedAvgSpeedB
                : "Key B: averageSpeed should be mean of its two readings";
        assert getAvgX(finalB) == 0.0
                : "Key B: X should be 0 — no acceleration events were sent for key B";
    }

    // -------------------------------------------------------------------------
    // Record count — one outbound record emitted per routed inbound event
    // -------------------------------------------------------------------------

    /**
     * Verifies the pipeline emits exactly one outbound record per routed inbound event.
     */
    @Test
    public void testOneOutboundRecordPerRoutedEvent() throws Exception {
        InboundProducer p = producer();
        Key key = new Key();

        int eventCount = 5;
        for (int i = 0; i < eventCount; i++) {
            p.sendInboundEvent(TelemetryType.SPEED, Optional.of(key));
        }
        p.closeProducer();
        Thread.sleep(PROCESSING_WAIT_MS);

        OutboundConsumer c = consumer(key.getVehicleId());
        List<KafkaRecord> results = c.consume(eventCount, CONSUME_TIMEOUT_MS);
        c.close();

        assert results.size() == eventCount
                : "Expected exactly " + eventCount + " outbound records, got " + results.size();
    }

    /**
     * Mixing routed and non-routed types: only the 2 routed events (SPEED, ACCELERATION)
     * should produce output. The 3 non-routed events (ENGINE, FUEL, BRAKE) must be silent.
     * The filtered consumer guarantees we only see this test's own vehicle's records.
     */
    @Test
    public void testMixedRoutedAndNonRoutedTypes() throws Exception {
        InboundProducer p = producer();
        Key key = new Key();

        p.sendInboundEvent(TelemetryType.SPEED,        Optional.of(key));
        p.sendInboundEvent(TelemetryType.ENGINE,       Optional.of(key)); // not routed
        p.sendInboundEvent(TelemetryType.ACCELERATION, Optional.of(key));
        p.sendInboundEvent(TelemetryType.FUEL,         Optional.of(key)); // not routed
        p.sendInboundEvent(TelemetryType.BRAKE,        Optional.of(key)); // not routed
        p.closeProducer();
        Thread.sleep(PROCESSING_WAIT_MS);

        OutboundConsumer c = consumer(key.getVehicleId());
        List<KafkaRecord> results = c.consume(2, CONSUME_TIMEOUT_MS);
        List<KafkaRecord> extras  = c.consumeAvailable(NO_RECORD_TIMEOUT_MS);
        c.close();

        assert results.size() == 2
                : "Expected exactly 2 outbound records (SPEED + ACCELERATION), got " + results.size();
        assert extras.isEmpty()
                : "Expected no additional records for this vehicle beyond the 2 routed events, got " + extras.size();
    }
}