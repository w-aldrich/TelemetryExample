package com.example.model.telemetryEnums;

import org.apache.avro.generic.GenericRecord;

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

    public static TelemetryType fromGRToType(GenericRecord gr) {
        return TelemetryType.valueOf(gr.get("TelemetryType").toString());
    }

    // Cap first letter, rest is lowercase + "Telemetry"
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
