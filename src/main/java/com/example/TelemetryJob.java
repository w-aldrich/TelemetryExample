package com.example;

import com.example.config.AppConfig;
import com.example.flink.ProcessSpeed;
import com.example.model.KafkaRecord;
import com.example.model.telemetryEnums.TelemetryType;
import com.example.util.deserialization.GenericDeserialization;
import com.example.util.serialization.GenericSerialization;
import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class TelemetryJob {

    private static final OutputTag<KafkaRecord> speedInfo = new OutputTag<KafkaRecord>("speedInfo"){};
    private static final OutputTag<KafkaRecord> dlq = new OutputTag<KafkaRecord>("dlq"){};

    public static void main(String[] args) throws Exception {
        run(StreamExecutionEnvironment.getExecutionEnvironment());
    }

    public static void run(StreamExecutionEnvironment env) throws Exception {

        System.out.println("**** Starting Flink Application ****");

        AppConfig appConfig = new AppConfig();

        env.enableCheckpointing(appConfig.getCheckpointInterval(), CheckpointingMode.EXACTLY_ONCE);
        // TODO: Add backend
//        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.setParallelism(1);

        // TODO: Remove once application is ready
        env.setRestartStrategy(
                org.apache.flink.api.common.restartstrategy.RestartStrategies.noRestart()
        );

        // Needed for GenericRecord deserialization
        AvroKryoSerializerUtils avroKryoSerializerUtils = new AvroKryoSerializerUtils();
        avroKryoSerializerUtils.addAvroSerializersIfRequired(env.getConfig(), GenericData.Record.class);

        String bootstrapServers = appConfig.getKafkaBootstrapServers();
        String schemaRegUrl = appConfig.getSchemaRegistryUrl();
        String inboundTopic = appConfig.getInboundTopic();

        KafkaSource<KafkaRecord> source =
                KafkaSource.<KafkaRecord>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inboundTopic)
                .setGroupId(appConfig.getConsumerGroupId())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new GenericDeserialization(
                        inboundTopic,
                        schemaRegUrl
                ))
                .build();

        KafkaSink<KafkaRecord> speedSink =
                KafkaSink.<KafkaRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setRecordSerializer(new GenericSerialization(
                            appConfig.getOutboundSpeedTopic(),
                            schemaRegUrl
                        ))
                        .build();

        // TODO: Add watermarks
        SingleOutputStreamOperator<KafkaRecord> router =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), inboundTopic)
                .process(new ProcessFunction<KafkaRecord, KafkaRecord>() {
                    @Override
                    public void processElement(KafkaRecord kafkaRecord, ProcessFunction<KafkaRecord, KafkaRecord>.Context context, Collector<KafkaRecord> collector) throws Exception {
                        if(kafkaRecord.hasDeserializationError()) {
                            context.output(dlq, kafkaRecord);
                        } else if (kafkaRecord.isTombstoneRecord()) {
                            //TODO implement tombstone, sending to DLQ for now
                            context.output(dlq, kafkaRecord);
                        } else {
                            TelemetryType t = TelemetryType.fromGRToType(kafkaRecord.getValue());
                            switch (t) {
                                case SPEED, ACCELERATION, ODOMETER -> context.output(speedInfo, kafkaRecord);
                            }
                        }
                    }
                });


        router.getSideOutput(speedInfo)
            .keyBy(KafkaRecord::getRoutingKey)
            .process(new ProcessSpeed())
            .sinkTo(speedSink);

        // TODO: implement DLQ
        // router.getSideOutput(dlq);

        env.execute("Vehicle Telemetry Processing Job");
    }
}
