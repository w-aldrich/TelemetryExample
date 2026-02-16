package com.example.util.deserialization.errors;

import com.example.model.PartitionOffset;

import java.util.Optional;

public class DeserializationError extends Exception {

    boolean isKey = false;
    Optional<PartitionOffset> partitionOffset = Optional.empty();
    Optional<String> errorInformation = Optional.empty();

    public DeserializationError(boolean isKey) {
        this.isKey = isKey;
    }

    public void setPartitionOffset(PartitionOffset partitionOffset) {
        this.partitionOffset = Optional.of(partitionOffset);
    }

    public void setErrorInformation(String errorInformation) {
        this.errorInformation = Optional.of(errorInformation);
    }

    @Override
    public String getMessage() {
        StringBuilder b = new StringBuilder();
        if(isKey) {
            b.append("The Key had a deserialization issue");
        } else {
            b.append("The Value had a deserialization issue");
        }

        if(partitionOffset.isPresent()) {
            b.append(" at ");
            b.append(partitionOffset);
        }

        if(errorInformation.isPresent()) {
            b.append(". Additional Information: ");
            b.append(errorInformation.get());
        }

        return super.getMessage();
    }
}
