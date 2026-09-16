package com.evcharging.messaging.contract;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class ChargerTelemetryReceivedTest {

    private static final UUID EVENT_ID = UUID.randomUUID();
    private static final Instant OCCURRED_AT = Instant.parse("2024-01-01T00:00:00Z");
    private static final Instant RECEIVED_AT = OCCURRED_AT.plusMillis(123);

    @Test
    void createsUnsolicitedTelemetryWithEventIdAsCorrelationId() {
        ChargerTelemetryPayload payload = payloadAt(OCCURRED_AT);

        DomainEventEnvelope<ChargerTelemetryPayload> event = ChargerTelemetryReceived.create(
                EVENT_ID,
                OCCURRED_AT,
                RECEIVED_AT,
                "EV001",
                new EvseId(1),
                null,
                payload
        );

        assertThat(event.eventId()).isEqualTo(EVENT_ID);
        assertThat(event.eventType()).isEqualTo(ChargerTelemetryReceived.EVENT_TYPE);
        assertThat(event.schemaVersion()).isEqualTo(ChargerTelemetryReceived.SCHEMA_VERSION);
        assertThat(event.requestId()).isNull();
        assertThat(event.correlationId()).isEqualTo(EVENT_ID);
        assertThat(event.evseId()).isEqualTo(1);
        assertThat(event.payload()).isEqualTo(payload);
    }

    @Test
    void usesRequestIdAsCorrelationIdWhenTelemetryComesFromARequest() {
        UUID requestId = UUID.randomUUID();

        DomainEventEnvelope<ChargerTelemetryPayload> event = ChargerTelemetryReceived.create(
                EVENT_ID,
                OCCURRED_AT,
                RECEIVED_AT,
                "EV001",
                new EvseId(1),
                requestId,
                payloadAt(OCCURRED_AT)
        );

        assertThat(event.requestId()).isEqualTo(requestId);
        assertThat(event.correlationId()).isEqualTo(requestId);
    }

    @Test
    void rejectsMismatchedPayloadTimestamp() {
        ChargerTelemetryPayload payload = payloadAt(OCCURRED_AT.plusSeconds(1));

        assertThatIllegalArgumentException()
                .isThrownBy(() -> ChargerTelemetryReceived.create(
                        EVENT_ID,
                        OCCURRED_AT,
                        RECEIVED_AT,
                        "EV001",
                        new EvseId(1),
                        null,
                        payload
                ))
                .withMessage("payload.timestamp must match occurredAt");
    }

    private static ChargerTelemetryPayload payloadAt(Instant occurredAt) {
        return new ChargerTelemetryPayload(
                occurredAt.toEpochMilli(),
                true,
                new BigDecimal("120.0"),
                new BigDecimal("24.0"),
                new BigDecimal("5.0")
        );
    }
}
