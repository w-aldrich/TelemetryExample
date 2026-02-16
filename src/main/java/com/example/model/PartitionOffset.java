package com.example.model;

public record PartitionOffset(int partition, long offset) {

    @Override
    public String toString() {
        return "Partition: " + partition + " Offset: " + offset;
    }
}
