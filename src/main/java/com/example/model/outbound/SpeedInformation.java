package com.example.model.outbound;

import com.example.model.KafkaRecord;
import com.example.util.helpers.GenericRecordHelper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class SpeedInformation implements Outbound {

    private static class ActualOutbound {
        double averageXAcceleration;
        double averageYAcceleration;
        double averageZAcceleration;
        double averageSpeed;
        double totalKmDriven;

        private ActualOutbound(double x, double y, double z, double s, double t) {
            averageXAcceleration = x;
            averageYAcceleration = y;
            averageZAcceleration = z;
            averageSpeed = s;
            totalKmDriven = t;
        }
    }

    private OutboundKey outboundKey;
    private int sumX = 0;
    private int countX = 0;
    private int sumY = 0;
    private int countY = 0;
    private int sumZ = 0;
    private int countZ = 0;
    private int sumSpeed = 0;
    private int countSpeed = 0;
    private double startKm = 0;
    private double currentKm = 0;
    private final String valueSchemaString = new Schema.Parser().parse(new Schema.Parser().parse(Files.readString(new File("schemas/outboundAvsc/valueSpeedInformation.avsc").toPath())).toString()).toString();

    public SpeedInformation(String vehicleId, long date) throws IOException {
        outboundKey = new OutboundKey(vehicleId, date);
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
        return countX <= 1 ? sumX : (double) sumX/countX;
    }
    private double getYAvg() {
        return countY <= 1 ? sumY : (double) sumY/countY;
    }
    private double getZAvg() {
        return countZ <= 1 ? sumZ : (double) sumZ/countZ;
    }

    private double getSpeedAvg() {
        return countSpeed <= 1 ? sumSpeed : (double) sumSpeed/countSpeed;
    }

    private double getTotalDriven() {
        return currentKm <= 1 ? 0 : currentKm - startKm;
    }


    @Override
    public KafkaRecord toKafkaRecord() {
        GenericRecord keyRecord = outboundKey.toGenericRecord();
        ActualOutbound a = new ActualOutbound(getXAvg(), getYAvg(), getZAvg(), getSpeedAvg(), getTotalDriven());
        GenericRecord valueRecord = GenericRecordHelper.fromObjectToGenericRecord(a, new Schema.Parser().parse(valueSchemaString));

        return new KafkaRecord(keyRecord, valueRecord, false);
    }

}
