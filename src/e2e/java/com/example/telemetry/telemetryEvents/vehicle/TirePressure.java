package com.example.telemetry.telemetryEvents.vehicle;

import com.example.telemetry.RandomGenerator;
import com.example.telemetry.telemetryEvents.BaseEvent;

public class TirePressure extends BaseEvent {
    double frontLeftPsi = (double) RandomGenerator.generateInt(100);
    double frontRightPsi = (double) RandomGenerator.generateInt(100);
    double rearLeftPsi = (double) RandomGenerator.generateInt(100);
    double rearRightPsi = (double) RandomGenerator.generateInt(100);

}
