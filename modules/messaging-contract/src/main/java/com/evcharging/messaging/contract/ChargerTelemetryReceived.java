package com.evcharging.messaging.contract;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public final class ChargerTelemetryReceived {

    public static final String EVENT_TYPE = "ChargerTelemetryReceived";
    public static final int SCHEMA_VERSION = 1;

    private ChargerTelemetryReceived() {
    }

    public static DomainEventEnvelope<ChargerTelemetryPayload> create(
            UUID eventId,
            Instant occurredAt,
            Instant receivedAt,
            String stationId,
            EvseId evseId,
            UUID requestId,
            ChargerTelemetryPayload payload
    ) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        Objects.requireNonNull(receivedAt, "receivedAt must not be null");
        Objects.requireNonNull(evseId, "evseId must not be null");
        Objects.requireNonNull(payload, "payload must not be null");

        if (payload.timestamp() != occurredAt.toEpochMilli()) {
            throw new IllegalArgumentException("payload.timestamp must match occurredAt");
        }

        UUID correlationId = requestId == null ? eventId : requestId;
        return new DomainEventEnvelope<>(
                eventId,
                EVENT_TYPE,
                SCHEMA_VERSION,
                occurredAt,
                receivedAt,
                stationId,
                evseId.value(),
                requestId,
                correlationId,
                payload
        );
    }
}
