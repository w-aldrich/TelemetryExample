package com.example;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TelemetryJob {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);

        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        // Kafka source and sink will be added here
        // Transformations placeholder

        env.execute("Vehicle Telemetry Processing Job");
    }
}
