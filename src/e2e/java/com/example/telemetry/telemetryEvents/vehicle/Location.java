package com.example.telemetry.telemetryEvents.vehicle;

import com.example.telemetry.utils.RandomGenerator;
import com.example.telemetry.telemetryEvents.BaseEvent;

public class Location extends BaseEvent {
    double latitude = (double) RandomGenerator.generateInt(10000);
    double longitude = (double) RandomGenerator.generateInt(10000);
    double altitudeMeters = (double) RandomGenerator.generateInt(10000);
}
