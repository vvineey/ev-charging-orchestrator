package com.evcharging.messaging.contract;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class DomainEventEnvelopeTest {

    private static final UUID EVENT_ID = UUID.randomUUID();
    private static final Instant EVENT_TIME = Instant.parse("2024-01-01T00:00:00Z");

    @Test
    void allowsNullRequestIdForUnsolicitedTelemetry() {
        DomainEventEnvelope<String> envelope = new DomainEventEnvelope<>(
                EVENT_ID,
                "TestEvent",
                1,
                EVENT_TIME,
                EVENT_TIME.plusMillis(100),
                "EV001",
                1,
                null,
                EVENT_ID,
                "payload"
        );

        org.assertj.core.api.Assertions.assertThat(envelope.requestId()).isNull();
    }

    @Test
    void rejectsBlankEventType() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new DomainEventEnvelope<>(
                        EVENT_ID,
                        " ",
                        1,
                        EVENT_TIME,
                        EVENT_TIME,
                        "EV001",
                        1,
                        null,
                        EVENT_ID,
                        "payload"
                ))
                .withMessage("eventType must not be blank");
    }

    @Test
    void rejectsMissingCorrelationId() {
        assertThatNullPointerException()
                .isThrownBy(() -> new DomainEventEnvelope<>(
                        EVENT_ID,
                        "TestEvent",
                        1,
                        EVENT_TIME,
                        EVENT_TIME,
                        "EV001",
                        1,
                        null,
                        null,
                        "payload"
                ))
                .withMessage("correlationId must not be null");
    }
}
