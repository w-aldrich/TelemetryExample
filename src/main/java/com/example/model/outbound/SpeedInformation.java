package com.example.model.outbound;

import com.example.model.KafkaRecord;
import com.example.util.helpers.GenericRecordHelper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class SpeedInformation implements Outbound {

    //TODO: Override toString for debugging purposes

    private OutboundKeyVIDDate outboundKeyVIDDate;
    private double sumX = 0.0;
    private double countX = 0.0;
    private double sumY = 0.0;
    private double countY = 0.0;
    private double sumZ = 0.0;
    private double countZ = 0.0;
    private double sumSpeed = 0.0;
    private double countSpeed = 0.0;
    private double startKm = 0.0;
    private double currentKm = 0.0;
    private final String valueSchemaString = new Schema.Parser().parse(new Schema.Parser().parse(Files.readString(new File("schemas/outboundAvsc/valueSpeedInformation.avsc").toPath())).toString()).toString();

    public SpeedInformation(String vehicleId, long date) throws IOException {
        outboundKeyVIDDate = new OutboundKeyVIDDate(vehicleId, date);
    }

    public void setSpeed(double speed) {
        sumSpeed += speed;
        countSpeed++;
    }

    public void setAcc(double x, double y, double z) {
        sumX += x;
        sumY += y;
        sumZ += z;
        countX++;
        countY++;
        countZ++;
    }

    public void setKmDriven(double km) {
        if(startKm == 0) {
            startKm = km;
        } else {
            currentKm = km;
        }
    }

    private double getXAvg() {
        return countX <= 1 ? sumX : sumX/countX;
    }
    private double getYAvg() {
        return countY <= 1 ? sumY : sumY/countY;
    }
    private double getZAvg() {
        return countZ <= 1 ? sumZ : sumZ/countZ;
    }

    private double getSpeedAvg() {
        return countSpeed <= 1 ? sumSpeed : sumSpeed/countSpeed;
    }

    private double getTotalDriven() {
        return currentKm <= 1 ? 0 : currentKm - startKm;
    }


    @Override
    public KafkaRecord toKafkaRecord() {
        GenericRecord keyRecord = outboundKeyVIDDate.toGenericRecord();
        SpeedInformationOutbound a = new SpeedInformationOutbound(getXAvg(), getYAvg(), getZAvg(), getSpeedAvg(), getTotalDriven());
        GenericRecord valueRecord = GenericRecordHelper.fromObjectToGenericRecord(a, new Schema.Parser().parse(valueSchemaString));

        return new KafkaRecord(keyRecord, valueRecord, false);
    }

}
