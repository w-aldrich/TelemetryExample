package com.example.telemetry.telemetryEvents.vehicle;

import com.example.telemetry.RandomGenerator;
import com.example.telemetry.telemetryEvents.BaseEvent;

public class Odometer extends BaseEvent {
    double totalKilometers = (double) RandomGenerator.generateInt(100);
}
