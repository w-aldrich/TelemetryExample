package com.example;

import com.example.config.AppConfig;
import com.example.flink.ProcessSpeed;
import com.example.flink.ProcessVehicle;
import com.example.model.KafkaRecord;
import com.example.model.outbound.SpeedInformation;
import com.example.model.telemetryEnums.TelemetryType;
import com.example.util.deserialization.GenericDeserialization;
import com.example.util.serialization.GenericSerialization;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.checkerframework.checker.units.qual.K;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Properties;

public class TelemetryJob {

    private static final OutputTag<KafkaRecord> speedInfo = new OutputTag<>("speedInfo");
    private static final OutputTag<KafkaRecord> vehicleInfo = new OutputTag<>("vehicleInfo");
    private static final OutputTag<KafkaRecord> dlq = new OutputTag<>("dlq");

    private Properties getProperties(AppConfig appConfig) {
        Properties props = new Properties();
        props.put("bootstrap.servers", appConfig.getKafkaBootstrapServers());
        props.put("schema.registry.url", appConfig.getSchemaRegistryUrl());
        props.put("group.id", appConfig.getConsumerGroupId());
        return props;
    }

    private static String getKey(KafkaRecord record) {
        return record.getKey()
                .get("vehicleId") + "_" +
                Instant.ofEpochMilli(
                                (long) record.getValue().get("eventTimestamp")
                        )
                        .atZone(ZoneId.of("UTC"))
                        .toLocalDate()
                        .toString();
    }

    public static void main(String[] args) throws Exception {

        AppConfig appConfig = new AppConfig();

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(appConfig.getCheckpointInterval(), CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.setParallelism(appConfig.getParallelism());

        KafkaSource<KafkaRecord> source =
                KafkaSource.<KafkaRecord>builder()
                .setBootstrapServers(appConfig.getKafkaBootstrapServers())
                .setTopics(appConfig.getInboundTopic())
                .setGroupId(appConfig.getConsumerGroupId())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new GenericDeserialization(appConfig.getInboundTopic()))
                .build();

        KafkaSink<KafkaRecord> sink =
                KafkaSink.<KafkaRecord>builder()
                        .setBootstrapServers(appConfig.getKafkaBootstrapServers())
                        .setRecordSerializer(
                            new GenericSerialization(
                                    appConfig.getProcessedTopic(),
                                    appConfig.getSchemaRegistryUrl()
                            )
                        ).build();

        // TODO: Add watermarks
        SingleOutputStreamOperator<KafkaRecord> router =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), appConfig.getInboundTopic())
                .process(new ProcessFunction<KafkaRecord, KafkaRecord>() {
                    @Override
                    public void processElement(KafkaRecord kafkaRecord, ProcessFunction<KafkaRecord, KafkaRecord>.Context context, Collector<KafkaRecord> collector) throws Exception {
                        TelemetryType t = TelemetryType.fromGRToType(kafkaRecord.getValue());
                        switch (t) {
                            case SPEED, ACCELERATION, ODOMETER -> context.output(speedInfo, kafkaRecord);
                        }
                    }
                });


        router.getSideOutput(speedInfo)
            .keyBy(TelemetryJob::getKey)
            .process(new ProcessSpeed())
            .map(SpeedInformation::toKafkaRecord)
            .sinkTo(sink);



        env.execute("Vehicle Telemetry Processing Job");
    }
}
