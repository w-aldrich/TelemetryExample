package com.example.outbound;

import com.example.model.KafkaRecord;
import com.example.util.helpers.GenericRecordHelper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class SpeedInformation extends Key {
    Key key;
    double averageXAcceleration;
    double averageYAcceleration;
    double averageZAcceleration;
    double totalKmDriven;
    Schema valueSchema;

    public SpeedInformation(Schema keySchema, Schema valueSchema, String vehicleId, long date) {
        key = new Key(vehicleId, date, keySchema);
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    public KafkaRecord toKafkaRecord() {
        GenericRecord keyRecord = key.toGenericRecord();
        GenericRecord valueRecord = GenericRecordHelper.fromObjectToGenericRecord(this, valueSchema);

        return new KafkaRecord(keyRecord, valueRecord);
    }


}
