package com.example.util.serialization;

import com.example.model.KafkaRecord;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Map;

public class GenericSerialization implements KafkaSerializationSchema<KafkaRecord> {

    private String destinationTopic;

    //TODO: confirm cache
    private KafkaAvroSerializer keySerializer;
    private KafkaAvroSerializer valueSerializer;

    public GenericSerialization(String destinationTopic) {
        Map<String, Object> config = Map.of(
                "schema.registry.url", "http://localhost:8081",
                "specific.avro.reader", false
        );
        keySerializer.configure(config, true);
        valueSerializer.configure(config, false);
        this.destinationTopic = destinationTopic;
    }

    //TODO: actual exception information
    @Override
    public ProducerRecord<byte[], byte[]> serialize(KafkaRecord kafkaRecord, @Nullable Long aLong) {
       try {
           byte[] key = keySerializer.serialize(destinationTopic, kafkaRecord.getKey());
           byte[] value = valueSerializer.serialize(destinationTopic, kafkaRecord.getValue());
           ProducerRecord<byte[], byte[]> outbound = new ProducerRecord<>(destinationTopic, key, value);
           return outbound;
       } catch (Exception e) {
           throw e;
       }
    }
}
