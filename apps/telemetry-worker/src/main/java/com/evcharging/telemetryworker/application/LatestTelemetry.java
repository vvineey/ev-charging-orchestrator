package com.evcharging.telemetryworker.application;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record LatestTelemetry(
        String stationId,
        int evseId,
        boolean charging,
        BigDecimal power,
        BigDecimal voltage,
        BigDecimal current,
        Instant occurredAt,
        Instant receivedAt,
        UUID eventId
) {
}
