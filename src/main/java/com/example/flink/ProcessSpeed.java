package com.example.flink;

import com.example.model.KafkaRecord;
import com.example.model.outbound.SpeedInformation;
import com.example.model.telemetryEnums.TelemetryType;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessSpeed extends KeyedProcessFunction<String, KafkaRecord, SpeedInformation> {

    private transient ValueState<SpeedInformation> state;

    @Override
    public void open(Configuration parameters) {

        ValueStateDescriptor<SpeedInformation> aggDesc =
            new ValueStateDescriptor<SpeedInformation>("telemetry-agg", SpeedInformation.class);

        state = getRuntimeContext().getState(aggDesc);
    }

    @Override
    public void processElement(
            KafkaRecord kafkaRecord,
            KeyedProcessFunction<String, KafkaRecord, SpeedInformation>.Context context,
            Collector<SpeedInformation> collector) throws Exception {

        SpeedInformation inState = state.value();

        if(inState == null) {
            inState =
                new SpeedInformation(
                    kafkaRecord.getKey().getSchema(),
                    kafkaRecord.getValue().getSchema(),
                    (String) kafkaRecord.getKey().get("vehicleId"),
                    (long) kafkaRecord.getValue().get("eventTimestamp"));
        }

        TelemetryType t = TelemetryType.valueOf((String)kafkaRecord.getValue().get("TelemetryType"));
        GenericRecord payload = (GenericRecord) kafkaRecord.getValue().get("payload");
        GenericRecord internal = (GenericRecord) payload.get(t.typeToTelemetryName());

        switch (t) {
            case SPEED -> inState.setSpeed((double) internal.get("speedKph"));
            case ACCELERATION -> {
                double x = (double) internal.get("xAxis");
                double y = (double) internal.get("yAxis");
                double z = (double) internal.get("zAxis");
                inState.setAcc(x, y, z);
            }
            case ODOMETER -> inState.setKmDriven((double)internal.get("totalKilometers"));
        }
    }

}
