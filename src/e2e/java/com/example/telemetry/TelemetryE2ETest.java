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
    private MiniCluster miniCluster;

    private void createTopicAndSchemas() {
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
        createTopicAndSchemas();
        miniCluster = new MiniCluster();
        miniCluster.waitForJobStartup();
    }

    @AfterAll
    public void tearDown() throws Exception {
        miniCluster.shutdown();
    }

    private boolean testAvgSpeed(double expectedSpeed, GenericRecord gr) {
        return Double.parseDouble(gr.get("averageSpeed").toString()) == expectedSpeed;
    }

    private boolean testAvgX(double expectedAvg, GenericRecord gr) {
        return Double.parseDouble(gr.get("averageXAcceleration").toString()) == expectedAvg;
    }

    private boolean testAvgY(double expectedAvg, GenericRecord gr) {
        return Double.parseDouble(gr.get("averageXAcceleration").toString()) == expectedAvg;
    }

    private boolean testAvgZ(double expectedAvg, GenericRecord gr) {
        return Double.parseDouble(gr.get("averageXAcceleration").toString()) == expectedAvg;
    }

    private boolean testTotalKmDriven(double expectedTotal, GenericRecord gr) {
        return Double.parseDouble(gr.get("totalKmDriven").toString()) == expectedTotal;
    }

    private GenericRecord getPayload(GenericRecord gr) {
        return (GenericRecord) gr.get("payload");
    }

    private double getInboundSpeed(GenericRecord gr) {
        return Double.parseDouble(getPayload(gr).get("speedKph").toString());
    }
    private double getInboundX(GenericRecord gr) {
        return Double.parseDouble(getPayload(gr).get("xAxis").toString());
    }
    private double getInboundY(GenericRecord gr) {
        return Double.parseDouble(getPayload(gr).get("yAxis").toString());
    }
    private double getInboundZ(GenericRecord gr) {
        return Double.parseDouble(getPayload(gr).get("zAxis").toString());
    }
    private double getInboundKm(GenericRecord gr) {
        return Double.parseDouble(getPayload(gr).get("totalKilometers").toString());
    }

    // expected 3 outbound records
    // test speed, acc, odometer events work as expected without additional logic
    @Test
    public void testAllSpeedEvents() throws Exception {
        InboundProducer inboundProducer = new InboundProducer(
                BOOTSTRAP,
                REGISTRY,
                INBOUND_TOPIC,
                INBOUND_AVSC_KEY,
                INBOUND_AVSC_VALUE
        );

        Key sameKey = new Key();
        KafkaRecord inboundSpeed = inboundProducer.sendInboundEvent(TelemetryType.SPEED, Optional.of(sameKey));
        KafkaRecord inboundAcc = inboundProducer.sendInboundEvent(TelemetryType.ACCELERATION, Optional.of(sameKey));

        // Ignore as we aren't testing this
        inboundProducer.sendInboundEvent(TelemetryType.ODOMETER, Optional.of(sameKey));

        inboundProducer.closeProducer();

        // Wait 1 second to ensure everything is processed as expected
        Thread.sleep(1000);

        OutboundConsumer consumer = new OutboundConsumer(BOOTSTRAP, REGISTRY, OUTBOUND_SPEED_TOPIC, RandomGenerator.generateString(3));
        List<KafkaRecord> ret = consumer.consume(3, 2000);

        assert(ret.size() == 3);

        consumer.close();

        // Using the same key, the last record should contain everything we are looking for
        GenericRecord expectedKmRecord = ret.get(2).getValue();

        assert( getInboundSpeed(inboundSpeed.getValue()) == (double) expectedKmRecord.get("averageSpeed"));
        assert( getInboundX(inboundAcc.getValue()) == (double) expectedKmRecord.get("averageXAcceleration") );
        assert( getInboundY(inboundAcc.getValue()) == (double) expectedKmRecord.get("averageYAcceleration") );
        assert( getInboundZ(inboundAcc.getValue()) == (double) expectedKmRecord.get("averageZAcceleration") );
        // Starting point nothing should be here
        assert( 0 == (double) expectedKmRecord.get("totalKmDriven"));
    }

}