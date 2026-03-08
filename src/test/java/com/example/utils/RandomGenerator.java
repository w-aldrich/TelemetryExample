package com.example.utils;

import com.example.model.telemetryEnums.Status;
import com.example.model.telemetryEnums.TelemetryType;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class RandomGenerator {
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final SecureRandom RANDOM = new SecureRandom();

    public static String generateString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int index = RANDOM.nextInt(CHARACTERS.length());
            sb.append(CHARACTERS.charAt(index));
        }
        return sb.toString();
    }

    public static int generateInt(int bound) {
        return RANDOM.nextInt(bound);
    }

    public static TelemetryType generateType() {
        TelemetryType[] vals = TelemetryType.values();
        int randomIndex = generateInt(vals.length);
        return vals[randomIndex];
    }

    public static Status generateStatus() {
        Status[] vals = Status.values();
        int randomIndex = generateInt(vals.length);
        return vals[randomIndex];
    }

    public static boolean generateBoolean() {
        return RANDOM.nextBoolean();
    }
}