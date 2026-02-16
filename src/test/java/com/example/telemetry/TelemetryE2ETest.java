package com.example.telemetry;

import com.example.telemetry.infraSetup.KafkaAdminHelper;
import com.example.telemetry.infraSetup.SchemaRegistryHelper;
import org.junit.jupiter.api.Test;

public class TelemetryE2ETest {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String REGISTRY = "http://localhost:8081";
    private static final String TOPIC = "telemetry-topic";

    @Test
    public void testEndToEnd() throws Exception {

        // Create topic
        KafkaAdminHelper.createTopic(BOOTSTRAP, TOPIC);

        // Register schemas
        SchemaRegistryHelper.registerSchema(
                REGISTRY,
                TOPIC + "-key",
                "schemas/key.avsc"
        );

        SchemaRegistryHelper.registerSchema(
                REGISTRY,
                TOPIC + "-value",
                "schemas/value.avsc"
        );

//        // Produce telemetry
//        TelemetryProducer.sendEvent(
//                BOOTSTRAP,
//                REGISTRY,
//                TOPIC,
//                "schemas/inboundAvsc/key.avsc",
//                "schemas/inboundAvsc/value.avsc"
//        );
    }
}
