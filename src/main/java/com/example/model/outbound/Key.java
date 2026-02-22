package com.example.model.outbound;

import com.example.util.helpers.GenericRecordHelper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class Key {

    String keySchema = new Schema.Parser().parse(Files.readString(new File("schemas/outboundAvsc/key.avsc").toPath())).toString();
    String vehicleId;
    long date;

    public Key(String vehicleId, long date) throws IOException {
        this.keySchema = keySchema.toString();
        this.vehicleId = vehicleId;
        this.date = date;
    }

    public GenericRecord toGenericRecord() {
        return GenericRecordHelper.fromObjectToGenericRecord(this, new Schema.Parser().parse(keySchema));
    }
}
