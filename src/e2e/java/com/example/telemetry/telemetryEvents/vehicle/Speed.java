package com.example.telemetry.telemetryEvents.vehicle;

import com.example.telemetry.RandomGenerator;
import com.example.telemetry.telemetryEvents.BaseEvent;

public class Speed extends BaseEvent {
    double speedKph = (double)RandomGenerator.generateInt(100);
}
