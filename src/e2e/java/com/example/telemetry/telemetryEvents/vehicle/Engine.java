package com.example.telemetry.telemetryEvents.vehicle;

import com.example.telemetry.RandomGenerator;
import com.example.telemetry.telemetryEvents.BaseEvent;

public class Engine extends BaseEvent {
    int rpm = RandomGenerator.generateInt(1000);
    double engineTempC = (double) RandomGenerator.generateInt(100);
}
