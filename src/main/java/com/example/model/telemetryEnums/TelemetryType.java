package com.example.model.telemetryEnums;

public enum TelemetryType {
    SPEED,
    ENGINE,
    FUEL,
    LOCATION,
    TIRE_PRESSURE,
    BATTERY,
    BRAKE,
    ACCELERATION,
    ODOMETER,
    DIAGNOSTIC,
    WEATHER,
    ROAD_CONDITION,
    TRAFFIC,
    INFRASTRUCTURE,
    MOBILE_DEVICE;

    public String typeToTelemetryName() {
        String name = this.name();
        return
                name.charAt(0) +
                    name.substring(1)
                        .toLowerCase() + "Telemetry";
    }
}
