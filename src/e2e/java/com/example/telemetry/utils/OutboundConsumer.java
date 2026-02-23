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
import java.util.Properties;

public class OutboundConsumer {

    private final KafkaConsumer<GenericRecord, GenericRecord> consumer;
    private final String topic;

    public OutboundConsumer(String bootstrapServers, String schemaRegistryUrl, String topic, String groupId) {
        this.topic = topic;

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

    // TODO: Add key value to look for to limit results
    public List<KafkaRecord> consume(
            int expectedCount, long timeoutMs) {

        List<KafkaRecord> collected = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline && collected.size() < expectedCount) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) break;

            ConsumerRecords<GenericRecord, GenericRecord> records =
                    consumer.poll(Duration.ofMillis(Math.min(remaining, 500)));

            for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                // ignore routing key
                collected.add(new KafkaRecord(record.key(), record.value(), false));
            }
        }

        return collected;
    }

    /**
     * Polls once with a short timeout — useful for asserting no records are present.
     */
    public List<KafkaRecord> consumeAvailable(long timeoutMs) {
        return consume(Integer.MAX_VALUE, timeoutMs);
    }

    public void close() {
        consumer.close();
    }
}
