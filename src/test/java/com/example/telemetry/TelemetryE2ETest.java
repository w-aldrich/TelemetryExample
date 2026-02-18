package com.example.telemetry;

import com.example.model.telemetryEnums.TelemetryType;
import com.example.telemetry.infraSetup.KafkaAdminHelper;
import com.example.telemetry.infraSetup.SchemaRegistryHelper;
import org.junit.jupiter.api.Test;

public class TelemetryE2ETest {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String REGISTRY = "http://localhost:8081";
    private static final String TOPIC = "telemetry-topic";

    @Test
    public void testEndToEnd() throws Exception {

        String inboundKeyAvroPath = "schemas/inboundAvsc/key.avsc";
        String inboundValueAvroPath = "schemas/inboundAvsc/value.avsc";

        // Create topic
        KafkaAdminHelper.createTopic(BOOTSTRAP, TOPIC);

        // Register schemas
        SchemaRegistryHelper.registerSchema(
                REGISTRY,
                TOPIC + "-key",
                inboundKeyAvroPath
        );

        SchemaRegistryHelper.registerSchema(
                REGISTRY,
                TOPIC + "-value",
                inboundValueAvroPath
        );

        TelemetryProducer producer =
                new TelemetryProducer(
                        BOOTSTRAP,
                        REGISTRY,
                        TOPIC,
                        inboundKeyAvroPath,
                        inboundValueAvroPath
                );

        producer.sendEvent(TelemetryType.SPEED);

    }
}
