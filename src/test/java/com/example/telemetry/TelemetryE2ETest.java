package com.example.telemetry;

import com.example.model.telemetryEnums.TelemetryType;
import com.example.telemetry.infraSetup.KafkaAdminHelper;
import com.example.telemetry.infraSetup.SchemaRegistryHelper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TelemetryE2ETest {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String REGISTRY = "http://localhost:8081";
    private static final String TOPIC = "telemetry-topic";
    private static final String inboundKeyAvroPath = "schemas/inboundAvsc/key.avsc";
    private static final String inboundValueAvroPath = "schemas/inboundAvsc/value.avsc";

    @BeforeAll
    public static void createTopicAndSchemas() {
        // TODO: This try catch is just so I don't have to restart docker, fix
        try {
            KafkaAdminHelper.createTopic(BOOTSTRAP, TOPIC);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        try {
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
        } catch (Exception e) {
            System.out.println("Failed to register schemas");
            System.out.println(e.getMessage());
        }

    }

    @Test
    public void testOneOfEachEvent() throws Exception {

        InboundProducer inboundProducer =
                new InboundProducer(
                        BOOTSTRAP,
                        REGISTRY,
                        TOPIC,
                        inboundKeyAvroPath,
                        inboundValueAvroPath
                );

        for(TelemetryType t: TelemetryType.values()) {
            inboundProducer.sendInboundEvent(t);
        }

        inboundProducer.closeProducer();

        //TODO: create consumer and ensure all events are consumed properly

    }
}
