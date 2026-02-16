package com.example.model;

import com.example.util.Deserialization.Errors.DeserializationError;
import org.apache.avro.generic.GenericRecord;

import java.util.Optional;

public class KafkaRecord {
    private GenericRecord key;
    private GenericRecord value;
    private Optional<PartitionOffset> partitionOffset = Optional.empty();
    private Optional<DeserializationError> keyDeserializationError = Optional.empty();
    private Optional<DeserializationError> valueDeserializationError = Optional.empty();

    public KafkaRecord(GenericRecord key, GenericRecord value) {
        this.key = key;
        this.value = value;
    }

    public GenericRecord getKey() {
        return key;
    }

    public GenericRecord getValue() {
        return value;
    }

    public void setPartitionOffset(int partition, long offset) {
        partitionOffset = Optional.of(new PartitionOffset(partition, offset));
    }

    public void setDeserializationError(boolean iskey, Optional<DeserializationError> error) {
        if(iskey) {
            keyDeserializationError = error;
        } else {
            valueDeserializationError = error;
        }
    }

    public Optional<PartitionOffset> getPartitionOffset() {
        return partitionOffset;
    }

    public boolean hasDeserializationError() {
        return keyDeserializationError.isPresent() || valueDeserializationError.isPresent();
    }

    public boolean isTombstoneRecord() {
        return key != null && value == null;
    }

}
