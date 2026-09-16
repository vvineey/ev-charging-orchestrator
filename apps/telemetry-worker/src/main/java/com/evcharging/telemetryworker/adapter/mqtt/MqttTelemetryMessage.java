package com.evcharging.telemetryworker.adapter.mqtt;

import java.math.BigDecimal;

public record MqttTelemetryMessage(
        String stationId,
        int chargerNo,
        boolean charging,
        BigDecimal power,
        long timestamp,
        BigDecimal voltage,
        BigDecimal current
) {
}
