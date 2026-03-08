package com.example.telemetry.telemetryEvents.vehicle;

import com.example.utils.RandomGenerator;
import com.example.telemetry.telemetryEvents.BaseEvent;

public class Acceleration extends BaseEvent {

    double xAxis = (double) RandomGenerator.generateInt(100);
    double yAxis = (double) RandomGenerator.generateInt(100);
    double zAxis = (double) RandomGenerator.generateInt(100);

}
