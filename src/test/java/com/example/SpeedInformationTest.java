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
        return Double.parseDouble(gr.get("averageXAcceleration").toString()) == expectedAvg;
    }

    private boolean testAvgZ(double expectedAvg, GenericRecord gr) {
        return Double.parseDouble(gr.get("averageXAcceleration").toString()) == expectedAvg;
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

    // TODO: Test X
    // TODO: Test Y
    // TODO: Test Z
    // TODO: Test KM Driven
}
