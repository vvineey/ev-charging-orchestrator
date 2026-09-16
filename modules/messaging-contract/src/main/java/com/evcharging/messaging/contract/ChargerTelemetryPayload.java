package com.evcharging.messaging.contract;

import java.math.BigDecimal;
import java.util.Objects;

public record ChargerTelemetryPayload(
        long timestamp,
        boolean charging,
        BigDecimal power,
        BigDecimal voltage,
        BigDecimal current
) {

    public ChargerTelemetryPayload {
        if (timestamp < 0) {
            throw new IllegalArgumentException("timestamp must not be negative");
        }
        Objects.requireNonNull(power, "power must not be null");
        Objects.requireNonNull(voltage, "voltage must not be null");
        Objects.requireNonNull(current, "current must not be null");
    }
}
