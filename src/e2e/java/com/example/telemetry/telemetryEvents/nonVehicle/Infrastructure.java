package com.example.telemetry.telemetryEvents.nonVehicle;

import com.example.telemetry.utils.RandomGenerator;
import com.example.telemetry.telemetryEvents.BaseEvent;

public class Infrastructure extends BaseEvent {
    String sensorId = RandomGenerator.generateString(10);
    double signalStrength = (double) RandomGenerator.generateInt(10);
    String status = RandomGenerator.generateStatus().toString();
}
