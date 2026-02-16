package com.example.telemetry.telemetryEvents;

import com.example.telemetry.RandomGenerator;

public class VehicleInfo {
    private String make = RandomGenerator.generateString(10);
    private String model = RandomGenerator.generateString(10);
    private int year = RandomGenerator.generateInt(2026);
    private String engineType = RandomGenerator.generateString(10);

    public void setMake(String make) {
        this.make = make;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public void setEngineType(String engineType) {
        this.engineType = engineType;
    }

    public String getMake() {
        return make;
    }

    public String getModel() {
        return model;
    }

    public int getYear() {
        return year;
    }

    public String getEngineType() {
        return engineType;
    }
}
