package com.example.telemetry.telemetryEvents.nonVehicle;

import com.example.utils.RandomGenerator;
import com.example.telemetry.telemetryEvents.BaseEvent;

public class Weather extends BaseEvent {
    double temperatureC = (double) RandomGenerator.generateInt(100);
    double humidityPercent = (double) RandomGenerator.generateInt(100);
    double windSpeedKph = (double) RandomGenerator.generateInt(100);
    double precipitationMm = (double) RandomGenerator.generateInt(100);
}
