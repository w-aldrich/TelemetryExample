package com.example.model;

import com.example.util.deserialization.errors.DeserializationError;
import org.apache.avro.generic.GenericRecord;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;

public class KafkaRecord implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private String routingKey = "";
    private GenericRecord key;
    private GenericRecord value;
    private int partition = 0;
    private long offset = 0;
    private Optional<DeserializationError> keyDeserializationError = Optional.empty();
    private Optional<DeserializationError> valueDeserializationError = Optional.empty();

    public KafkaRecord() {};

    public KafkaRecord(GenericRecord key, GenericRecord value, boolean isInbound) {
        this.key = key;
        this.value = value;
        if(isInbound) {
            computeRoutingKey();
        }
    }

    private void computeRoutingKey() {
        this.routingKey = key
                .get("vehicleId") + "_" +
                Instant.ofEpochMilli(
                                (long) value.get("eventTimestamp")
                        )
                        .atZone(ZoneId.of("UTC"))
                        .toLocalDate()
                        .toString();

    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setKey(GenericRecord key) {
        this.key = key;
    }
    public GenericRecord getKey() {
        return key;
    }

    public void setValue(GenericRecord value) {
        this.value = value;
    }

    public GenericRecord getValue() {
        return value;
    }

    public void setPartitionOffset(int partition, long offset) {
       this.partition = partition;
       this.offset = offset;
    }

    public PartitionOffset getPartitionOffset() {
        return new PartitionOffset(partition, offset);
    }

    public void setDeserializationError(boolean iskey, Optional<DeserializationError> error) {
        if(iskey) {
            keyDeserializationError = error;
        } else {
            valueDeserializationError = error;
        }
    }

    public boolean hasDeserializationError() {
        return keyDeserializationError.isPresent() || valueDeserializationError.isPresent();
    }

    public boolean isTombstoneRecord() {
        return key != null && value == null;
    }

}
