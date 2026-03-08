package com.example.flink;

import com.example.model.KafkaRecord;
import com.example.model.outbound.SpeedInformation;
import com.example.model.telemetryEnums.TelemetryType;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessSpeed extends KeyedProcessFunction<String, KafkaRecord, KafkaRecord> {

    private transient ValueState<SpeedInformation> state;

    @Override
    public void open(Configuration parameters) {
        try {
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.hours(24))
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .cleanupFullSnapshot()
                    .build();

            ValueStateDescriptor<SpeedInformation> aggDesc =
                    new ValueStateDescriptor<SpeedInformation>("telemetry-agg", SpeedInformation.class);

            aggDesc.enableTimeToLive(ttlConfig);

            state = getRuntimeContext().getState(aggDesc);
        } catch (Exception e) {
            System.out.printf("[PROCESS_SPEED] ERROR %s", e.getMessage());
        }
    }

    @Override
    public void processElement(
            KafkaRecord kafkaRecord,
            KeyedProcessFunction<String, KafkaRecord, KafkaRecord>.Context context,
            Collector<KafkaRecord> collector) throws Exception {

        SpeedInformation inState = state.value();

        if (inState == null) {
            inState = new SpeedInformation(
                    (String) kafkaRecord.getKey().get("vehicleId").toString(),
                    (long) kafkaRecord.getValue().get("eventTimestamp"));
        }

        TelemetryType t = TelemetryType.valueOf((String)kafkaRecord.getValue().get("TelemetryType").toString());
        GenericRecord payload = (GenericRecord) kafkaRecord.getValue().get("payload");

        switch (t) {
            case SPEED -> inState.setSpeed((double) payload.get("speedKph"));
            case ACCELERATION -> {
                double x = (double) payload.get("xAxis");
                double y = (double) payload.get("yAxis");
                double z = (double) payload.get("zAxis");
                inState.setAcc(x, y, z);
            }
            case ODOMETER -> inState.setKmDriven((double) payload.get("totalKilometers"));
        }

        state.update(inState);
        collector.collect(inState.toKafkaRecord());
    }
}
