package com.example.telemetry;

import com.example.telemetry.telemetryEvents.Key;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.*;
import org.checkerframework.checker.units.qual.K;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Properties;

public class TelemetryProducer {

    String bootstrap;
    String registry;
    String topic;
    Schema keySchema;
    Schema valueSchema;

    public TelemetryProducer(String bootstrap, String registry, String topic, String keyAvroPath, String valueAvroPath) throws IOException {
        this.bootstrap = bootstrap;
        this.registry = registry;
        this.topic = topic;
        this.keySchema = new Schema.Parser().parse(new File(keyAvroPath));
        this.valueSchema = new Schema.Parser().parse(new File(valueAvroPath));
    }

    private GenericRecord keyRecord(Schema keySchema) {
        GenericRecord record = new GenericData.Record(keySchema);
        Key randomKey = new Key();
        Field[] fields = Key.class.getDeclaredFields();
        for (Field field : fields) {
            try {
                String name = field.getName();

                // Get the field value for this specific object instance
                // For static fields, pass null or the Class object
                Object value = field.get(this);

                record.put(name, value);
            } catch (IllegalAccessException e) {
                System.out.println("Error accessing field: " + field.getName());
                e.printStackTrace();
            }
        }
        return record;
    }

    public void sendEvent() throws Exception {

        GenericRecord key = keyRecord(keySchema);
//        new GenericData.Record(keySchema);
//        key.put("vehicleId", "vehicle-123");
//        key.put("vin", "VIN123456789");
//        key.put("fleetId", "fleet-alpha");

        GenericRecord value = new GenericData.Record(valueSchema);
        value.put("eventId", "evt-1");
        value.put("eventTimestamp", System.currentTimeMillis());
        value.put("telemetryType", "SPEED");

        // Speed payload
        Schema speedSchema = valueSchema.getField("payload")
                .schema()
                .getTypes()
                .stream()
                .filter(s -> s.getName().equals("SpeedTelemetry"))
                .findFirst()
                .orElseThrow();

        GenericRecord speed = new GenericData.Record(speedSchema);
        speed.put("speedKph", 100.5);

        value.put("payload", speed);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put("schema.registry.url", registry);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        Producer<Object, Object> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<>(topic, key, value)).get();
        producer.close();
    }
}
