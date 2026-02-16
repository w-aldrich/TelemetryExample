package com.example.telemetry.telemetryEvents;

import com.example.model.telemetryEnums.TelemetryType;
import com.example.telemetry.RandomGenerator;

public class BaseEvent {
    String eventId = RandomGenerator.generateString(10);
    VehicleInfo vehicleInfo = new VehicleInfo();
    TelemetryType type;

    public BaseEvent() {
        type = RandomGenerator.generateType();
    }

    public BaseEvent(TelemetryType type) {
        this.type = type;
    }

}
