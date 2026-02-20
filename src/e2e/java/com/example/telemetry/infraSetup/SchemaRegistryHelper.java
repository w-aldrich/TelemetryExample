package com.example.telemetry.infraSetup;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.Schema;

import java.io.File;
import java.nio.file.Files;

public class SchemaRegistryHelper {

    public static void registerSchema(
            String registryUrl,
            String subject,
            String schemaPath
    ) throws Exception {

        String schemaStr = Files.readString(new File(schemaPath).toPath());
        Schema schema = new Schema.Parser().parse(schemaStr);

        CachedSchemaRegistryClient client =
                new CachedSchemaRegistryClient(registryUrl, 10);

        client.register(subject, schema);
    }
}
