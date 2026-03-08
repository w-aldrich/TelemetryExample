package com.example.telemetry.telemetryEvents.nonVehicle;

import com.example.utils.RandomGenerator;
import com.example.telemetry.telemetryEvents.BaseEvent;

public class Traffic extends BaseEvent {
    int congestionLevel = RandomGenerator.generateInt(10);
    double averageSpeedKph = (double) RandomGenerator.generateInt(100);
    boolean incidentReported = RandomGenerator.generateBoolean();
}
