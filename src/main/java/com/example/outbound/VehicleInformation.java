package com.example.outbound;

import com.example.model.KafkaRecord;
import com.example.util.helpers.GenericRecordHelper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class VehicleInformation extends Key {

    Key key;
    Schema valueSchema;

    public VehicleInformation(Schema keySchema, Schema valueSchema, String vehicleId, long date) {
        this.key = new Key(vehicleId, date, keySchema);
        this.valueSchema = valueSchema;
    }

    public KafkaRecord toKafkaRecord() {
        GenericRecord keyRecord = key.toGenericRecord();
        GenericRecord valueRecord = GenericRecordHelper.fromObjectToGenericRecord(this, valueSchema);

        return new KafkaRecord(keyRecord, valueRecord);
    }
}
