package com.example;

import com.example.config.AppConfig;
import com.example.flink.ProcessSpeed;
import com.example.model.KafkaRecord;
import com.example.model.outbound.SpeedInformation;
import com.example.model.telemetryEnums.TelemetryType;
import com.example.util.deserialization.GenericDeserialization;
import com.example.util.serialization.GenericSerialization;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Properties;

public class TelemetryJob {

    private static final OutputTag<KafkaRecord> speedInfo = new OutputTag<KafkaRecord>("speedInfo"){};
    private static final OutputTag<KafkaRecord> vehicleInfo = new OutputTag<KafkaRecord>("vehicleInfo"){};
    private static final OutputTag<KafkaRecord> dlq = new OutputTag<KafkaRecord>("dlq"){};

    public static void main(String[] args) throws Exception {
        run(StreamExecutionEnvironment.getExecutionEnvironment());
    }

    public static void run(StreamExecutionEnvironment env) throws Exception {

        System.out.println("**** Starting Flink Application ****");

        AppConfig appConfig = new AppConfig();

        env.enableCheckpointing(appConfig.getCheckpointInterval(), CheckpointingMode.EXACTLY_ONCE);
//        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.setParallelism(1);
        env.setRestartStrategy(
                org.apache.flink.api.common.restartstrategy.RestartStrategies.noRestart()
        );
        AvroKryoSerializerUtils avroKryoSerializerUtils = new AvroKryoSerializerUtils();
        avroKryoSerializerUtils.addAvroSerializersIfRequired(env.getConfig(), GenericData.Record.class);

        KafkaSource<KafkaRecord> source =
                KafkaSource.<KafkaRecord>builder()
                .setBootstrapServers(appConfig.getKafkaBootstrapServers())
                .setTopics(appConfig.getInboundTopic())
                .setGroupId(appConfig.getConsumerGroupId())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new GenericDeserialization(
                        appConfig.getInboundTopic(),
                        appConfig.getSchemaRegistryUrl()
                ))
                .build();

        KafkaSink<KafkaRecord> sink =
                KafkaSink.<KafkaRecord>builder()
                        .setBootstrapServers(appConfig.getKafkaBootstrapServers())
                        .setRecordSerializer(new GenericSerialization(
                            appConfig.getOutboundSpeedTopic(),
                            appConfig.getSchemaRegistryUrl()
                        ))
                        .build();

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
            .keyBy(KafkaRecord::getRoutingKey)
            .process(new ProcessSpeed())
            .sinkTo(sink);


        env.execute("Vehicle Telemetry Processing Job");
    }
}
