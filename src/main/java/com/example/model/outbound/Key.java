package com.example.model.outbound;

import com.example.util.helpers.GenericRecordHelper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class Key {

    Schema keySchema;
    String vehicleId;
    long date;

    public Key(String vehicleId, long date, Schema keySchema) {
        this.keySchema = keySchema;
        this.vehicleId = vehicleId;
        this.date = date;
    }

    public GenericRecord toGenericRecord() {
        return GenericRecordHelper.fromObjectToGenericRecord(this, keySchema);
    }
}
