package com.example.telemetry.telemetryEvents.vehicle;

import com.example.utils.RandomGenerator;
import com.example.telemetry.telemetryEvents.BaseEvent;

public class Odometer extends BaseEvent {
    double totalKilometers = (double) RandomGenerator.generateInt(100);
}
