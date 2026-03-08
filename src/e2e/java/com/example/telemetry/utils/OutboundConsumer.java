package com.example.telemetry.utils;

import com.example.model.KafkaRecord;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class OutboundConsumer {

    private final KafkaConsumer<GenericRecord, GenericRecord> consumer;
    private final String topic;
    private final Optional<String> vehicleIdFilter;

    /**
     * Creates a consumer that returns all records from the topic, regardless of key.
     * Useful when asserting across multiple vehicle IDs (e.g. key isolation tests).
     */
    public OutboundConsumer(String bootstrapServers, String schemaRegistryUrl, String topic, String groupId) {
        this(bootstrapServers, schemaRegistryUrl, topic, groupId, Optional.empty());
    }

    /**
     * Creates a consumer that only returns records whose outbound key matches the given vehicleId.
     * This ensures each test only sees its own records, even when the topic contains
     * records from other concurrent or prior tests.
     */
    public OutboundConsumer(String bootstrapServers, String schemaRegistryUrl, String topic, String groupId, String vehicleId) {
        this(bootstrapServers, schemaRegistryUrl, topic, groupId, Optional.of(vehicleId));
    }

    private OutboundConsumer(String bootstrapServers, String schemaRegistryUrl, String topic, String groupId, Optional<String> vehicleIdFilter) {
        this.topic = topic;
        this.vehicleIdFilter = vehicleIdFilter;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("specific.avro.reader", false);

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topic));
    }

    /**
     * Returns true if the given record passes the vehicleId filter.
     * Always returns true when no filter is set.
     */
    private boolean matchesFilter(ConsumerRecord<GenericRecord, GenericRecord> record) {
        if (vehicleIdFilter.isEmpty()) {
            return true;
        }
        if (record.key() == null) {
            return false;
        }
        Object vehicleId = record.key().get("vehicleId");
        return vehicleId != null && vehicleIdFilter.get().equals(vehicleId.toString());
    }

    /**
     * Polls until {@code expectedCount} matching records have been collected or
     * {@code timeoutMs} elapses, whichever comes first.
     * Non-matching records (filtered by vehicleId) are silently skipped.
     */
    public List<KafkaRecord> consume(int expectedCount, long timeoutMs) {
        List<KafkaRecord> collected = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline && collected.size() < expectedCount) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) break;

            ConsumerRecords<GenericRecord, GenericRecord> records =
                    consumer.poll(Duration.ofMillis(Math.min(remaining, 500)));

            for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                if (matchesFilter(record)) {
                    collected.add(new KafkaRecord(record.key(), record.value(), false));
                }
            }
        }

        return collected;
    }

    /**
     * Polls until {@code timeoutMs} elapses, collecting all matching records.
     * Useful for asserting no records are present for a given vehicleId.
     */
    public List<KafkaRecord> consumeAvailable(long timeoutMs) {
        return consume(Integer.MAX_VALUE, timeoutMs);
    }

    public void close() {
        consumer.close();
    }
}