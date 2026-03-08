package com.example.telemetry.telemetryEvents.nonVehicle;

import com.example.utils.RandomGenerator;
import com.example.telemetry.telemetryEvents.BaseEvent;

public class RoadCondition extends BaseEvent {
    String surfaceType = RandomGenerator.generateString(3);
    double surfaceTemperatureC = (double) RandomGenerator.generateInt(100);
    boolean hazardDetected = RandomGenerator.generateBoolean();
}
