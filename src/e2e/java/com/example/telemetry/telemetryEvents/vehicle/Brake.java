package com.example.telemetry.telemetryEvents.vehicle;

import com.example.telemetry.utils.RandomGenerator;
import com.example.telemetry.telemetryEvents.BaseEvent;

public class Brake extends BaseEvent {
    double brakePadWearPercent = (double) RandomGenerator.generateInt(100);
}
