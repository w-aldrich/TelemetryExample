package com.example.telemetry.telemetryEvents;

import com.example.model.telemetryEnums.TelemetryType;
import com.example.telemetry.utils.RandomGenerator;

public class BaseEvent {
    String eventId;
    VehicleInfo vehicleInfo;
    TelemetryType type;

    public BaseEvent() {
        eventId = RandomGenerator.generateString(10);
        vehicleInfo = new VehicleInfo();
        type = RandomGenerator.generateType();
    }

    public VehicleInfo getVehicleInfo() {
        return vehicleInfo;
    }

    public BaseEvent(TelemetryType type) {
        this.type = type;
    }

}
