package com.example.telemetry;

import com.example.model.telemetryEnums.TelemetryType;
import com.example.telemetry.telemetryEvents.BaseEvent;
import com.example.telemetry.telemetryEvents.Key;
import com.example.telemetry.telemetryEvents.nonVehicle.Infrastructure;
import com.example.telemetry.telemetryEvents.nonVehicle.MobileDevice;
import com.example.telemetry.telemetryEvents.vehicle.Acceleration;
import com.example.telemetry.telemetryEvents.vehicle.Speed;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Properties;

public class TelemetryProducer {

    String bootstrap;
    String registry;
    String topic;
    Schema keySchema;
    Schema valueSchema;
    Properties props = new Properties();
    Producer<Object, Object> producer;

    public TelemetryProducer(String bootstrap, String registry, String topic, String keyAvroPath, String valueAvroPath) throws IOException {
        this.bootstrap = bootstrap;
        this.registry = registry;
        this.topic = topic;
        this.keySchema = new Schema.Parser().parse(new File(keyAvroPath));
        this.valueSchema = new Schema.Parser().parse(new File(valueAvroPath));
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put("schema.registry.url", registry);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producer = new KafkaProducer<>(props);
    }

    private GenericRecord setFieldNameAndValue(GenericRecord record, Object o, boolean ignoreBaseTypes) {
        Field[] fields = o.getClass().getDeclaredFields();
        for (Field field : fields) {
            try {
                // To access private fields, you must make them accessible
                field.setAccessible(true);

                // Get the name of the field
                String name = field.getName();
                if(ignoreBaseTypes && (name.equals("eventId") || name.equals("eventTimestamp") || name.equals("telemetryType"))) {
                    continue;
                }

                // Get the value of the field for the specific object instance
                // For static fields, pass null as the object argument
                Object value = field.get(Modifier.isStatic(field.getModifiers()) ? null : o);
                record.put(name, value);
            } catch (IllegalAccessException e) {
                System.out.println("Error accessing field: " + field.getName());
                e.printStackTrace();
            }
        }
        return record;
    }

    private GenericRecord keyRecord(Schema keySchema) {
        GenericRecord record = new GenericData.Record(keySchema);
        Key randomKey = new Key();
        return setFieldNameAndValue(record, randomKey, false);
    }

    private Schema getTypeSchema(TelemetryType t) {
        // name is all caps, convert the rest to lowercase and then add Telemetry to the end
        return
            valueSchema.getField("payload")
                .schema()
                .getTypes()
                .stream()
                .filter(s -> s.getName().equals(t.typeToTelemetryName()))
                .findFirst()
                .orElseThrow();
    }

    private GenericRecord valueRecord(TelemetryType t) throws Exception {
        GenericRecord record = new GenericData.Record(valueSchema);
        Object blah;
        Schema internalSchema;

        record.put("eventId", RandomGenerator.generateString(10));
        record.put("eventTimestamp", System.currentTimeMillis());
        record.put("telemetryType", t.name());

        switch (t) {
            case FUEL, BRAKE, ENGINE, BATTERY, TRAFFIC, WEATHER, LOCATION, ODOMETER, DIAGNOSTIC, TIRE_PRESSURE, ROAD_CONDITION -> {
                throw new Exception("Unimplemented");
            }
            case SPEED -> {
                blah = new Speed();
            }
            case ACCELERATION -> {
                blah = new Acceleration();
            }
            case MOBILE_DEVICE -> {
                blah = new MobileDevice();
            }
            case INFRASTRUCTURE -> {
                blah = new Infrastructure();
            }
            default -> throw new Exception("Unimplemented");
        }

        Schema vehicleSchema = valueSchema.getField("vehicleInfo").schema();
        GenericRecord vehicle = new GenericData.Record(vehicleSchema);
        BaseEvent b = (BaseEvent) blah;
        setFieldNameAndValue(vehicle, b.getVehicleInfo(), true);
        record.put("vehicleInfo", vehicle);

        internalSchema = getTypeSchema(t);
        GenericRecord internalRecord = new GenericData.Record(internalSchema);
        setFieldNameAndValue(internalRecord, blah, true);
        record.put("payload", internalRecord);

        //Convert Enum
        Schema enumSchema = valueSchema.getField("telemetryType").schema();
        GenericData.EnumSymbol symbol = new GenericData.EnumSymbol(enumSchema, t);
        record.put("telemetryType", symbol);

        System.out.println(record);

        return record;
    }

    public void sendEvent(TelemetryType t) throws Exception {

        GenericRecord key = keyRecord(keySchema);

        GenericRecord value = valueRecord(t);

        producer.send(new ProducerRecord<>(topic, key, value)).get();
        producer.close();
    }
}
