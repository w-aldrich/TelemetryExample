package com.example.model.outbound;

class SpeedInformationOutbound {

    double averageXAcceleration;
    double averageYAcceleration;
    double averageZAcceleration;
    double averageSpeed;
    double totalKmDriven;

    protected SpeedInformationOutbound(double x, double y, double z, double s, double t) {
        averageXAcceleration = x;
        averageYAcceleration = y;
        averageZAcceleration = z;
        averageSpeed = s;
        totalKmDriven = t;
    }

}
