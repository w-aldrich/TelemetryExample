package com.example.util.deserialization;

import com.example.util.deserialization.errors.DeserializationError;
import com.example.model.KafkaRecord;
import com.example.model.PartitionOffset;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class GenericDeserialization implements KafkaRecordDeserializationSchema<KafkaRecord> {

    private final String topic;
    private final String schemaRegistryUrl;

    private transient KafkaAvroDeserializer keyDeserializer;
    private transient KafkaAvroDeserializer valueDeserializer;

    public GenericDeserialization(String topic, String schemaRegistryUrl) {
        this.topic             = topic;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    private void initDeserializersIfNeeded() {
        if (keyDeserializer != null) {
            return;
        }

        Map<String, Object> config = Map.of(
                "schema.registry.url", schemaRegistryUrl,
                "specific.avro.reader", false
        );

        keyDeserializer = new KafkaAvroDeserializer();
        keyDeserializer.configure(config, true);

        valueDeserializer = new KafkaAvroDeserializer();
        valueDeserializer.configure(config, false);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaRecord> collector)
            throws IOException {

        initDeserializersIfNeeded();

        GenericRecord key   = null;
        GenericRecord value = null;
        Optional<DeserializationError> keyError   = Optional.empty();
        Optional<DeserializationError> valueError = Optional.empty();

        int  partition = record.partition();
        long offset    = record.offset();

        try {
            key = (GenericRecord) keyDeserializer.deserialize(topic, record.key());
        } catch (Exception e) {
            keyError = buildError(true, e.getMessage(), partition, offset);
        }

        try {
            value = (GenericRecord) valueDeserializer.deserialize(topic, record.value());
        } catch (Exception e) {
            valueError = buildError(false, e.getMessage(), partition, offset);
        }

        KafkaRecord kafkaRecord = new KafkaRecord(key, value, true);
        keyError  .ifPresent(err -> kafkaRecord.setDeserializationError(true,  Optional.of(err)));
        valueError.ifPresent(err -> kafkaRecord.setDeserializationError(false, Optional.of(err)));
        kafkaRecord.setPartitionOffset(partition, offset);

        collector.collect(kafkaRecord);
    }

    @Override
    public TypeInformation<KafkaRecord> getProducedType() {
        return TypeInformation.of(KafkaRecord.class);
    }

    private static Optional<DeserializationError> buildError(
            boolean isKey, String message, int partition, long offset) {

        DeserializationError error = new DeserializationError(isKey);
        error.setErrorInformation(message);
        error.setPartitionOffset(new PartitionOffset(partition, offset));
        return Optional.of(error);
    }
}