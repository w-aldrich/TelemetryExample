package com.example.util.serialization;

import com.example.model.KafkaRecord;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Map;

public class GenericSerialization implements KafkaRecordSerializationSchema<KafkaRecord> {

    private final String destinationTopic;
    private final String schemaRegistryUrl;

    // Not final — serializers are not serializable themselves, so we init them
    // lazily in open() after the class is deserialized on the task manager.
    private transient KafkaAvroSerializer keySerializer;
    private transient KafkaAvroSerializer valueSerializer;

    public GenericSerialization(String destinationTopic, String schemaRegistryUrl) {
        this.destinationTopic  = destinationTopic;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
            throws Exception {

        Map<String, Object> config = Map.of(
                "schema.registry.url", schemaRegistryUrl,
                "specific.avro.reader", false
        );

        keySerializer = new KafkaAvroSerializer();
        keySerializer.configure(config, true);  // isKey = true

        valueSerializer = new KafkaAvroSerializer();
        valueSerializer.configure(config, false); // isKey = false
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(KafkaRecord kafkaRecord, KafkaSinkContext kafkaSinkContext, Long aLong) {
        try {

            GenericRecord keyRecord = kafkaRecord.getKey();
            byte[] key = keySerializer.serialize(destinationTopic, keyRecord);

            GenericRecord valueRecord = kafkaRecord.getValue();
            byte[] value = valueSerializer.serialize(destinationTopic, valueRecord);

            return new ProducerRecord<>(destinationTopic, key, value);

        } catch (Exception e) {
            throw new RuntimeException("Serialization failed for topic " + destinationTopic, e);
        }
    }
}
