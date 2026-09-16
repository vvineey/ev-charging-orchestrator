package com.evcharging.messaging.contract;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record DomainEventEnvelope<T>(
        UUID eventId,
        String eventType,
        int schemaVersion,
        Instant occurredAt,
        Instant receivedAt,
        String stationId,
        int evseId,
        UUID requestId,
        UUID correlationId,
        T payload
) {

    public DomainEventEnvelope {
        Objects.requireNonNull(eventId, "eventId must not be null");
        if (eventType == null || eventType.isBlank()) {
            throw new IllegalArgumentException("eventType must not be blank");
        }
        if (schemaVersion <= 0) {
            throw new IllegalArgumentException("schemaVersion must be positive");
        }
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        Objects.requireNonNull(receivedAt, "receivedAt must not be null");
        if (stationId == null || stationId.isBlank()) {
            throw new IllegalArgumentException("stationId must not be blank");
        }
        if (evseId <= 0) {
            throw new IllegalArgumentException("evseId must be positive");
        }
        Objects.requireNonNull(correlationId, "correlationId must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
    }
}
