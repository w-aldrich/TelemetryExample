package com.example.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppConfig {

    private final Properties properties = new Properties();

    public AppConfig() {
        try (InputStream input =
                     getClass().getClassLoader()
                             .getResourceAsStream("application.properties")) {

            if (input == null) {
                throw new RuntimeException("application.properties not found");
            }

            properties.load(input);

        } catch (IOException e) {
            throw new RuntimeException("Failed to load configuration", e);
        }
    }

    public String getKafkaBootstrapServers() {
        return properties.getProperty("kafka.bootstrap.servers");
    }

    public String getSchemaRegistryUrl() {
        return properties.getProperty("schema.registry.url");
    }

    public String getInboundTopic() {
        return properties.getProperty("telemetry.inbound.topic");
    }

    public String getOutboundSpeedTopic() {
        return properties.getProperty("telemetry.outbound.speed.topic");
    }

    public String getConsumerGroupId() {
        return properties.getProperty("kafka.consumer.group.id");
    }

    public long getCheckpointInterval() {
        return Long.parseLong(
                properties.getProperty("flink.checkpoint.interval.ms"));
    }

    public int getParallelism() {
        return Integer.parseInt(
                properties.getProperty("flink.parallelism"));
    }

    public long getStateTtlMinutes() {
        return Long.parseLong(
                properties.getProperty("state.ttl.minutes"));
    }
}
