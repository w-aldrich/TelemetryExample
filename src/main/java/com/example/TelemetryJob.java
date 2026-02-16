package com.example;

import com.example.config.AppConfig;
import com.example.model.KafkaRecord;
import com.example.util.deserialization.GenericDeserialization;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class TelemetryJob {

    private Properties getProperties(AppConfig appConfig) {
        Properties props = new Properties();
        props.put("bootstrap.servers", appConfig.getKafkaBootstrapServers());
        props.put("schema.registry.url", appConfig.getSchemaRegistryUrl());
        props.put("group.id", appConfig.getConsumerGroupId());
        return props;
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

        DataStream<KafkaRecord> dataStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), appConfig.getInboundTopic());

        KeyedStream<KafkaRecord, String> keyedStream = dataStream.keyBy(record ->
                    record
                    .getKey()
                    .get("vehicleId")
                    .toString());

        keyedStream.map(record -> {
                    GenericRecord value = record.getValue();
                    return record;
                }
                );


        // Kafka source and sink will be added here
        // Transformations placeholder

        env.execute("Vehicle Telemetry Processing Job");
    }
}
