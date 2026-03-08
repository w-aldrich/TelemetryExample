package com.example.telemetry.telemetryEvents.vehicle;

import com.example.utils.RandomGenerator;
import com.example.telemetry.telemetryEvents.BaseEvent;

public class Fuel extends BaseEvent {
    double fuelLevelPercent = (double) RandomGenerator.generateInt(100);
}
