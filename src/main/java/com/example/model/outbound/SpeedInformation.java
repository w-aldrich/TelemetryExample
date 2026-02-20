package com.example.model.outbound;

import com.example.model.KafkaRecord;
import com.example.util.helpers.GenericRecordHelper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

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

    Key key;
    int sumX = 0;
    int countX = 0;
    int sumY = 0;
    int countY = 0;
    int sumZ = 0;
    int countZ = 0;
    int sumSpeed = 0;
    int countSpeed = 0;
    double startKm = 0;
    double currentKm = 0;
    Schema valueSchema;

    public SpeedInformation(Schema keySchema, Schema valueSchema, String vehicleId, long date) {
        key = new Key(vehicleId, date, keySchema);
        this.valueSchema = valueSchema;
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
        return countX == 0 ? 0 : (double) sumX/countX;
    }
    private double getYAvg() {
        return countY == 0 ? 0 : (double) sumY/countY;
    }
    private double getZAvg() {
        return countZ == 0 ? 0 : (double) sumZ/countZ;
    }

    private double getSpeedAvg() {
        return countSpeed == 0 ? 0 : (double) sumSpeed/countSpeed;
    }

    private double getTotalDriven() {
        return currentKm == 0 ? 0 : currentKm - startKm;
    }


    @Override
    public KafkaRecord toKafkaRecord() {
        GenericRecord keyRecord = key.toGenericRecord();
        ActualOutbound a = new ActualOutbound(getXAvg(), getYAvg(), getZAvg(), getSpeedAvg(), getTotalDriven());
        GenericRecord valueRecord = GenericRecordHelper.fromObjectToGenericRecord(a, valueSchema);

        return new KafkaRecord(keyRecord, valueRecord);
    }

}
