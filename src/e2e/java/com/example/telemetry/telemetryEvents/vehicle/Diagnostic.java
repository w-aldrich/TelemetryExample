package com.example.telemetry.telemetryEvents.vehicle;

import com.example.telemetry.utils.RandomGenerator;
import com.example.telemetry.telemetryEvents.BaseEvent;

public class Diagnostic extends BaseEvent {
    String errorCode = RandomGenerator.generateString(5);
    String severity = RandomGenerator.generateString(3);
}
