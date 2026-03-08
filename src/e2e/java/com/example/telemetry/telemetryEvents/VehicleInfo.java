package com.example.telemetry.telemetryEvents;

import com.example.utils.RandomGenerator;

public class VehicleInfo {
    private final String make = RandomGenerator.generateString(10);
    private final String model = RandomGenerator.generateString(10);
    private final int year = RandomGenerator.generateInt(2026);
    private final String engineType = RandomGenerator.generateString(10);
}
