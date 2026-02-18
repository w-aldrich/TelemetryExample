package com.example.telemetry.telemetryEvents.nonVehicle;

import com.example.telemetry.RandomGenerator;

public class MobileDevice {
    String deviceId = RandomGenerator.generateString(10);
    double batteryLevelPercent = (double) RandomGenerator.generateInt(100);
    String networkType = RandomGenerator.generateString(5);
    String appVersion = RandomGenerator.generateString(3);
}
