package com.example.model.outbound;

import com.example.model.KafkaRecord;

public interface Outbound {
    public KafkaRecord toKafkaRecord();
}
