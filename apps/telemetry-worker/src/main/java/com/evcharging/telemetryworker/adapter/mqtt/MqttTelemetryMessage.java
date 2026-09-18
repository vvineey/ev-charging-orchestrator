package com.evcharging.telemetryworker.adapter.mqtt;

import java.math.BigDecimal;

public record MqttTelemetryMessage(
        String stationId,
        Integer chargerNo,
        Boolean charging,
        BigDecimal power,
        Long timestamp,
        BigDecimal voltage,
        BigDecimal current
) {
}
