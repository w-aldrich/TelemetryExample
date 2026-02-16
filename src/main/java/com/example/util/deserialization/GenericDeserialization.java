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

//TODO: Add schema configs passed into class
public class GenericDeserialization implements KafkaRecordDeserializationSchema<KafkaRecord> {

    //TODO: Confirm cache exists here
    private final KafkaAvroDeserializer keyDeserializer = new KafkaAvroDeserializer();
    private final KafkaAvroDeserializer valueDeserializer = new KafkaAvroDeserializer();
    private String topic;

    public GenericDeserialization(String topic) {
        Map<String, Object> config = Map.of(
                "schema.registry.url", "http://localhost:8081",
                "specific.avro.reader", false
        );
        keyDeserializer.configure(config, true);
        valueDeserializer.configure(config, false);
        this.topic = topic;
    }

    @Override
    public TypeInformation<KafkaRecord> getProducedType() {
        // Types.POJO(KafkaRecord.class);
        return TypeInformation.of(KafkaRecord.class);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaRecord> collector) throws IOException {
        GenericRecord key;
        GenericRecord value;
        Optional<DeserializationError> keyError = Optional.empty();
        Optional<DeserializationError> valueError = Optional.empty();

        int partition = record.partition();
        long offset = record.offset();

        try {
            key = (GenericRecord) keyDeserializer.deserialize(topic, record.key());
        } catch (Exception e) {
            key = null;
            keyError = Optional.of(new DeserializationError(true));
            keyError.get().setErrorInformation(e.getMessage());
            PartitionOffset partitionOffset = new PartitionOffset(partition, offset);
            keyError.get().setPartitionOffset(partitionOffset);
        }

        try {
            value = (GenericRecord) valueDeserializer.deserialize(topic, record.value());
        } catch (Exception e) {
            value = null;
            valueError = Optional.of(new DeserializationError(true));
            valueError.get().setErrorInformation(e.getMessage());
            PartitionOffset partitionOffset = new PartitionOffset(partition, offset);
            valueError.get().setPartitionOffset(partitionOffset);
        }

        KafkaRecord kafkaRecord = new KafkaRecord(key, value);
        if(keyError.isPresent()) {
            kafkaRecord.setDeserializationError(true, keyError);
        }
        if(valueError.isPresent()) {
            kafkaRecord.setDeserializationError(false, valueError);
        }

        kafkaRecord.setPartitionOffset(partition, offset);

        collector.collect(kafkaRecord);
    }
}
