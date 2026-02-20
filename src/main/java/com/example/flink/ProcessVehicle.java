package com.example.flink;

import com.example.model.KafkaRecord;
import org.apache.flink.api.common.functions.RichMapFunction;

public class ProcessVehicle extends RichMapFunction<KafkaRecord, KafkaRecord> {
    @Override
    public KafkaRecord map(KafkaRecord kafkaRecord) throws Exception {
        return null;
    }
}
