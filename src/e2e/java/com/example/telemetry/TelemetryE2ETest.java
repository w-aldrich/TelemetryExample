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
import com.example.telemetry.utils.RandomGenerator;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
    private static final String inboundKeyAvroPath = "schemas/inboundAvsc/key.avsc";
    private static final String inboundValueAvroPath = "schemas/inboundAvsc/value.avsc";
    private static final String outboundKeyAvroPath = "schemas/outboundAvsc/key.avsc";
    private static final String outboundValueAvroPath = "schemas/outboundAvsc/valueSpeedInformation.avsc";
    private MiniCluster miniCluster;

    private void createTopicAndSchemas() {
        try {
            KafkaAdminHelper.createTopic(BOOTSTRAP, INBOUND_TOPIC);
            KafkaAdminHelper.createTopic(BOOTSTRAP, OUTBOUND_SPEED_TOPIC);
        } catch (Exception e) {
            System.out.println("[E2E] Topic create (may already exist): " + e.getMessage());
        }
        try {
            SchemaRegistryHelper.registerSchema(REGISTRY, INBOUND_TOPIC + "-key",   inboundKeyAvroPath);
            SchemaRegistryHelper.registerSchema(REGISTRY, INBOUND_TOPIC + "-value", inboundValueAvroPath);
            SchemaRegistryHelper.registerSchema(REGISTRY, OUTBOUND_SPEED_TOPIC + "-key",   outboundKeyAvroPath);   // ← add
            SchemaRegistryHelper.registerSchema(REGISTRY, OUTBOUND_SPEED_TOPIC + "-value", outboundValueAvroPath); // ← add

        } catch (Exception e) {
            System.out.println("[E2E] Schema register (may already exist): " + e.getMessage());
        }
    }

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
    @Test
    public void testAllSpeedEvents() throws Exception {
        InboundProducer inboundProducer = new InboundProducer(
                BOOTSTRAP,
                REGISTRY,
                INBOUND_TOPIC,
                inboundKeyAvroPath,
                inboundValueAvroPath
        );

        Key sameKey = new Key();
        KafkaRecord inboundSpeed = inboundProducer.sendInboundEvent(TelemetryType.SPEED, Optional.of(sameKey));
        KafkaRecord inboundAcc = inboundProducer.sendInboundEvent(TelemetryType.ACCELERATION, Optional.of(sameKey));
        KafkaRecord inboundKM = inboundProducer.sendInboundEvent(TelemetryType.ODOMETER, Optional.of(sameKey));

        inboundProducer.closeProducer();
        Thread.sleep(1000);

        OutboundConsumer consumer = new OutboundConsumer(BOOTSTRAP, REGISTRY, OUTBOUND_SPEED_TOPIC, RandomGenerator.generateString(3));
        List<KafkaRecord> ret = consumer.consume(3, 2000);

        assert(ret.size() == 3);

        consumer.close();

        GenericRecord expectedSpeedRecord = ret.get(0).getValue();
        GenericRecord expectedAccRecord = ret.get(1).getValue();
        GenericRecord expectedKmRecord = ret.get(2).getValue();

        assert( getInboundSpeed(inboundSpeed.getValue()) == (double) expectedSpeedRecord.get("averageSpeed"));
        assert( getInboundX(inboundAcc.getValue()) == (double) expectedAccRecord.get("averageXAcceleration") );
        assert( getInboundY(inboundAcc.getValue()) == (double) expectedAccRecord.get("averageYAcceleration") );
        assert( getInboundZ(inboundAcc.getValue()) == (double) expectedAccRecord.get("averageZAcceleration") );
        // Starting point nothing should be here
        assert( 0 == (double) expectedKmRecord.get("totalKmDriven"));
    }

}