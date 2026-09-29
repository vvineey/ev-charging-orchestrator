package com.evcharging.telemetryworker.application;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Last committed observation; the timestamps do not imply device connectivity. */
public record TelemetryObservation(
        String stationId, int evseId, boolean charging,
        BigDecimal power, BigDecimal voltage, BigDecimal current,
        Instant occurredAt, Instant receivedAt, Instant updatedAt, UUID lastEventId
) {
}
