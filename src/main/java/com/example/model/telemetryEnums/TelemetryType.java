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
        String[] split = name.split("_");
        StringBuilder sb = new StringBuilder();
        for(String s: split) {
            sb.append(s.charAt(0) + s.substring(1).toLowerCase());
        }
        sb.append("Telemetry");
        return sb.toString();
    }
}
