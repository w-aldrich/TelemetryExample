package com.example.telemetry.telemetryEvents;

import com.example.telemetry.RandomGenerator;

public class Key {
    String vehicleId = RandomGenerator.generateString(10);
    String vin = RandomGenerator.generateString(10);
    String fleetId = RandomGenerator.generateString(5);
}
