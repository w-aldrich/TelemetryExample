package com.example.model;

import com.example.model.telemetryEnums.TelemetryType;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class InboundRouter extends ProcessFunction<KafkaRecord, KafkaRecord> {

    private final OutputTag<KafkaRecord> speedInfoOutputTag = new OutputTag<>("speedInfo"){};
    private final OutputTag<KafkaRecord> dlqOutputTag = new OutputTag<>("dlq"){};

    public OutputTag<KafkaRecord> getSpeedInfoOutputTag() {
        return speedInfoOutputTag;
    }

    public OutputTag<KafkaRecord> getDlqOutputTag() {
        return dlqOutputTag;
    }

    @Override
    public void processElement(KafkaRecord kafkaRecord, ProcessFunction<KafkaRecord, KafkaRecord>.Context context, Collector<KafkaRecord> collector) {
        if(kafkaRecord.hasDeserializationError()) {
            context.output(dlqOutputTag, kafkaRecord);
        } else if (kafkaRecord.isTombstoneRecord()) {
            //TODO implement tombstone, sending to DLQ for now
            // This may change based off of needs as well, some types may need tombstone records, others may not
            context.output(dlqOutputTag, kafkaRecord);
        } else {
            TelemetryType t = TelemetryType.fromGRToType(kafkaRecord.getValue());
            switch (t) {
                case SPEED, ACCELERATION, ODOMETER -> context.output(speedInfoOutputTag, kafkaRecord);
            }
        }
    }
}
