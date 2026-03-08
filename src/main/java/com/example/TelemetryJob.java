package com.example;

import com.example.config.AppConfig;
import com.example.flink.ProcessSpeed;
import com.example.model.InboundRouter;
import com.example.model.KafkaRecord;
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

import java.time.Duration;


public class TelemetryJob {

    public static void main(String[] args) throws Exception {
        run(StreamExecutionEnvironment.getExecutionEnvironment());
    }

    public static void run(StreamExecutionEnvironment env) throws Exception {

        System.out.println("**** Starting Flink Application ****");

        AppConfig appConfig = new AppConfig();

        env.enableCheckpointing(appConfig.getCheckpointInterval(), CheckpointingMode.EXACTLY_ONCE);

        // TODO: Add backend, everything is in memory right now
//        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        // TODO: Update once application is ready. Should have higher than 1 parallelism.
        env.setParallelism(1);

        // TODO: Remove once application is ready. This allows for complete failure if something goes wrong.
        env.setRestartStrategy(
                org.apache.flink.api.common.restartstrategy.RestartStrategies.noRestart()
        );

        /*
        The following are needed for GenericRecord Deserialization.
        Also ensure you have the following in your pom.xml file
        --add-opens java.base/java.util=ALL-UNNAMED
        --add-opens java.base/java.lang=ALL-UNNAMED
        --add-opens java.base/java.lang.reflect=ALL-UNNAMED
        --add-opens java.base/java.io=ALL-UNNAMED
        --add-opens java.base/java.net=ALL-UNNAMED
        --add-opens java.base/java.nio=ALL-UNNAMED
        --add-opens java.base/sun.nio.ch=ALL-UNNAMED
        These are needed due to some issues with serialization/deserialization
         */
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

        // Outbound topic for `SpeedInformation` class
        KafkaSink<KafkaRecord> speedSink =
            KafkaSink.<KafkaRecord>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new GenericSerialization(
                    appConfig.getOutboundSpeedTopic(),
                    schemaRegUrl
                ))
                .build();

        /*
        Split the single telemetry topic into discrete units to work against.
        This allows us to break up the work and potentially have many outbound topics from the single inbound.
        This would be useful if downstream teams needed any specific information broken out. It takes the work off of those teams,
        and allows it to be real time coming through Flink.
         */
        InboundRouter inboundRouter = new InboundRouter();

        // TODO: Check this is the watermark you want
        // Allow for 5 minute delay
        WatermarkStrategy<KafkaRecord> watermarkStrategy =
                WatermarkStrategy.<KafkaRecord>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                        .withTimestampAssigner((event, timestamp) -> (long) event.getValue().get("eventTimestamp"));

        // TODO: Ensure that old events don't create new state
        SingleOutputStreamOperator<KafkaRecord> router =
            env.fromSource(source, watermarkStrategy, inboundTopic)
                .process(inboundRouter);

        router.getSideOutput(inboundRouter.getSpeedInfoOutputTag())
            .keyBy(KafkaRecord::getRoutingKey)
            .process(new ProcessSpeed())
            .sinkTo(speedSink);

        // TODO: implement DLQ
//         router.getSideOutput(inboundRouter.getDlqOutputTag());

        env.execute("Vehicle Telemetry Processing Job");
    }
}
