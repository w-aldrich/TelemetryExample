package com.example;

import com.example.model.KafkaRecord;
import com.example.model.outbound.SpeedInformation;
import com.example.utils.RandomGenerator;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class SpeedInformationTest {

    @Test
    public void testConstructor() {
        SpeedInformation speedInformation = null;
        try {
            speedInformation = new SpeedInformation("1", 1L);
        } catch (IOException e) {
            assert (false);
        }
        assert (true);
    }

    private boolean testKey(String vehicleId, long date, GenericRecord gr) {
        return gr.get("vehicleId").toString().equals(vehicleId) &&
                Long.parseLong(gr.get("date").toString()) == date;
    }

    private boolean testAvgSpeed(double expectedSpeed, GenericRecord gr) {
        return Double.parseDouble(gr.get("averageSpeed").toString()) == expectedSpeed;
    }

    private boolean testAvgX(double expectedAvg, GenericRecord gr) {
        return Double.parseDouble(gr.get("averageXAcceleration").toString()) == expectedAvg;
    }

    private boolean testAvgY(double expectedAvg, GenericRecord gr) {
        return Double.parseDouble(gr.get("averageYAcceleration").toString()) == expectedAvg;
    }

    private boolean testAvgZ(double expectedAvg, GenericRecord gr) {
        return Double.parseDouble(gr.get("averageZAcceleration").toString()) == expectedAvg;
    }

    private boolean testTotalKmDriven(double expectedTotal, GenericRecord gr) {
        return Double.parseDouble(gr.get("totalKmDriven").toString()) == expectedTotal;
    }

    private SpeedInformation setup(String id, long date) {
        SpeedInformation speedInformation = null;
        try {
            speedInformation = new SpeedInformation(id, date);
        } catch (IOException e) {
            assert (false);
        }
        return speedInformation;
    }

    // Test default kafka record
    @Test
    public void testToKafkaRecord() {
        String id = RandomGenerator.generateString(10);
        long date = (long) RandomGenerator.generateInt(50000);

        SpeedInformation speedInformation = setup(id, date);

        KafkaRecord kr = speedInformation.toKafkaRecord();
        GenericRecord value = kr.getValue();
        assert(testKey(id, date, kr.getKey()));

        assert(testAvgX(0, value));
        assert(testAvgY(0, value));
        assert(testAvgZ(0, value));
        assert(testAvgSpeed(0, value));
        assert(testTotalKmDriven(0, value));
    }

    // Should return 1st speed passed in
    @Test
    public void testSpeedOnly() {
        String id = RandomGenerator.generateString(10);
        long date = (long) RandomGenerator.generateInt(50000);
        double speed = (double) RandomGenerator.generateInt(100);

        SpeedInformation speedInformation = setup(id, date);
        speedInformation.setSpeed(speed);

        KafkaRecord kr = speedInformation.toKafkaRecord();
        GenericRecord value = kr.getValue();
        assert(testKey(id, date, kr.getKey()));
        assert(testAvgSpeed(speed, value));
    }

    // Should return avg speed
    @Test
    public void testSpeedAvg() {
        String id = RandomGenerator.generateString(10);
        long date = (long) RandomGenerator.generateInt(50000);
        double speed = (double) RandomGenerator.generateInt(100);

        SpeedInformation speedInformation = setup(id, date);
        speedInformation.setSpeed(speed);
        speedInformation.setSpeed(speed);

        double expectedAverage = (speed + speed) / 2;

        KafkaRecord kr = speedInformation.toKafkaRecord();
        GenericRecord value = kr.getValue();
        assert(testKey(id, date, kr.getKey()));
        assert(testAvgSpeed(expectedAverage, value));
    }

    // Should return the first X/Y/Z values passed in (count <= 1 returns sum directly)
    @Test
    public void testAccelerationOnly() {
        String id = RandomGenerator.generateString(10);
        long date = (long) RandomGenerator.generateInt(50000);
        double x = RandomGenerator.generateInt(100);
        double y = RandomGenerator.generateInt(100);
        double z = RandomGenerator.generateInt(100);

        SpeedInformation speedInformation = setup(id, date);
        speedInformation.setAcc(x, y, z);

        KafkaRecord kr = speedInformation.toKafkaRecord();
        GenericRecord value = kr.getValue();
        assert(testKey(id, date, kr.getKey()));
        assert(testAvgX(x, value));
        assert(testAvgY(y, value));
        assert(testAvgZ(z, value));
    }

    // Should return average X across two readings
    @Test
    public void testAccelerationXAvg() {
        String id = RandomGenerator.generateString(10);
        long date = (long) RandomGenerator.generateInt(50000);
        double x1 = RandomGenerator.generateInt(100);
        double x2 = RandomGenerator.generateInt(100);

        SpeedInformation speedInformation = setup(id, date);
        speedInformation.setAcc(x1, 0, 0);
        speedInformation.setAcc(x2, 0, 0);

        double expectedAvg = (x1 + x2) / 2;

        KafkaRecord kr = speedInformation.toKafkaRecord();
        GenericRecord value = kr.getValue();
        assert(testKey(id, date, kr.getKey()));
        assert(testAvgX(expectedAvg, value));
    }

    // Should return average Y across two readings
    @Test
    public void testAccelerationYAvg() {
        String id = RandomGenerator.generateString(10);
        long date = (long) RandomGenerator.generateInt(50000);
        double y1 = RandomGenerator.generateInt(100);
        double y2 = RandomGenerator.generateInt(100);

        SpeedInformation speedInformation = setup(id, date);
        speedInformation.setAcc(0, y1, 0);
        speedInformation.setAcc(0, y2, 0);

        double expectedAvg = (y1 + y2) / 2;

        KafkaRecord kr = speedInformation.toKafkaRecord();
        GenericRecord value = kr.getValue();
        assert(testKey(id, date, kr.getKey()));
        assert(testAvgY(expectedAvg, value));
    }

    // Should return average Z across two readings
    @Test
    public void testAccelerationZAvg() {
        String id = RandomGenerator.generateString(10);
        long date = (long) RandomGenerator.generateInt(50000);
        double z1 = RandomGenerator.generateInt(100);
        double z2 = RandomGenerator.generateInt(100);

        SpeedInformation speedInformation = setup(id, date);
        speedInformation.setAcc(0, 0, z1);
        speedInformation.setAcc(0, 0, z2);

        double expectedAvg = (z1 + z2) / 2;

        KafkaRecord kr = speedInformation.toKafkaRecord();
        GenericRecord value = kr.getValue();
        assert(testKey(id, date, kr.getKey()));
        assert(testAvgZ(expectedAvg, value));
    }

    // First odometer reading sets the baseline — totalKmDriven should be 0
    @Test
    public void testKmDrivenFirstReadingOnly() {
        String id = RandomGenerator.generateString(10);
        long date = (long) RandomGenerator.generateInt(50000);
        double startKm = RandomGenerator.generateInt(200000);

        SpeedInformation speedInformation = setup(id, date);
        speedInformation.setKmDriven(startKm);

        KafkaRecord kr = speedInformation.toKafkaRecord();
        GenericRecord value = kr.getValue();
        assert(testKey(id, date, kr.getKey()));
        assert(testTotalKmDriven(0, value));
    }

    // Second odometer reading should produce totalKmDriven = currentKm - startKm
    @Test
    public void testKmDrivenTwoReadings() {
        String id = RandomGenerator.generateString(10);
        long date = (long) RandomGenerator.generateInt(50000);
        double startKm = RandomGenerator.generateInt(100000) + 2; // ensure > 1 so getTotalDriven doesn't zero out
        double endKm = startKm + RandomGenerator.generateInt(500) + 1;

        SpeedInformation speedInformation = setup(id, date);
        speedInformation.setKmDriven(startKm);
        speedInformation.setKmDriven(endKm);

        double expectedTotal = endKm - startKm;

        KafkaRecord kr = speedInformation.toKafkaRecord();
        GenericRecord value = kr.getValue();
        assert(testKey(id, date, kr.getKey()));
        assert(testTotalKmDriven(expectedTotal, value));
    }
}