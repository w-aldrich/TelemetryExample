package com.example.telemetry;

import com.example.model.telemetryEnums.TelemetryType;
import com.example.telemetry.telemetryEvents.BaseEvent;
import com.example.telemetry.telemetryEvents.Key;
import com.example.telemetry.telemetryEvents.nonVehicle.*;
import com.example.telemetry.telemetryEvents.vehicle.*;
import com.example.util.helpers.GenericRecordHelper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.*;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class InboundProducer {

    String bootstrap;
    String registry;
    String topic;
    Schema keySchema;
    Schema valueSchema;
    Properties props = new Properties();
    Producer<Object, Object> producer;

    public InboundProducer(String bootstrap, String registry, String topic, String keyAvroPath, String valueAvroPath) throws IOException {
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

    private GenericRecord keyRecord(Schema keySchema) {
        return GenericRecordHelper.fromObjectToGenericRecord(new Key(), keySchema);
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
        BaseEvent event;
        Schema internalSchema;

        record.put("eventId", RandomGenerator.generateString(10));
        record.put("eventTimestamp", System.currentTimeMillis());
        record.put("telemetryType", t.name());

        switch (t) {
            case SPEED -> event = new Speed();
            case ENGINE -> event = new Engine();
            case FUEL -> event = new Fuel();
            case LOCATION -> event = new Location();
            case TIRE_PRESSURE -> event = new TirePressure();
            case BATTERY -> event = new Battery();
            case BRAKE -> event = new Brake();
            case ACCELERATION -> event = new Acceleration();
            case ODOMETER -> event = new Odometer();
            case DIAGNOSTIC -> event = new Diagnostic();
            case WEATHER -> event = new Weather();
            case ROAD_CONDITION -> event = new RoadCondition();
            case TRAFFIC -> event = new Traffic();
            case INFRASTRUCTURE -> event = new Infrastructure();
            case MOBILE_DEVICE -> event = new MobileDevice();
            default -> throw new Exception("Unimplemented");
        }

        Schema vehicleSchema = valueSchema.getField("vehicleInfo").schema();
        GenericRecord vehicle = GenericRecordHelper.fromObjectToGenericRecord(event.getVehicleInfo(), vehicleSchema);
        record.put("vehicleInfo", vehicle);

        internalSchema = getTypeSchema(t);
        GenericRecord internalRecord = GenericRecordHelper.fromObjectToGenericRecord(event, internalSchema);
        record.put("payload", internalRecord);

        //Convert Enum
        Schema enumSchema = valueSchema.getField("telemetryType").schema();
        GenericData.EnumSymbol symbol = new GenericData.EnumSymbol(enumSchema, t);
        record.put("telemetryType", symbol);

        System.out.println(record);

        return record;
    }

    public void sendInboundEvent(TelemetryType t) throws Exception {

        System.out.println("Starting to send Type: " + t.name());
        GenericRecord key = keyRecord(keySchema);

        GenericRecord value = valueRecord(t);

        producer.send(new ProducerRecord<>(topic, key, value)).get();
        System.out.println("Completed sending Type: " + t.name());
    }

    public void closeProducer() {
        producer.close();
    }
}
