package com.example.model.outbound;

import com.example.model.KafkaRecord;
import com.example.util.helpers.GenericRecordHelper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class VehicleInformation implements Outbound {

    Key key;
    Schema valueSchema;

    public VehicleInformation(Schema keySchema, Schema valueSchema, String vehicleId, long date) {
        key = new Key(vehicleId, date, keySchema);
        this.valueSchema = valueSchema;
    }

    @Override
    public KafkaRecord toKafkaRecord() {
        GenericRecord keyRecord = key.toGenericRecord();
        GenericRecord valueRecord = GenericRecordHelper.fromObjectToGenericRecord(this, valueSchema);

        return new KafkaRecord(keyRecord, valueRecord);
    }
}
