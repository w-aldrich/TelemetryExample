package com.example.telemetry.infraSetup;

import org.apache.kafka.clients.admin.*;
import java.util.Collections;
import java.util.Properties;

public class KafkaAdminHelper {

    public static void createTopic(String bootstrap, String topic) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);

        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        }
    }
}
