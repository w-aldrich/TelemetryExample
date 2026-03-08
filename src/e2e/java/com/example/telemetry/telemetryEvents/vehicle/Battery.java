package com.example.telemetry.telemetryEvents.vehicle;

import com.example.utils.RandomGenerator;
import com.example.telemetry.telemetryEvents.BaseEvent;

public class Battery extends BaseEvent {
    double voltage = (double) RandomGenerator.generateInt(100);
    double currentAmps = (double) RandomGenerator.generateInt(100);

}
