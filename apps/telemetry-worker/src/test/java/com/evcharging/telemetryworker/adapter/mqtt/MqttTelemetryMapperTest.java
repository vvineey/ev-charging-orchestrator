package com.evcharging.telemetryworker.adapter.mqtt;

import com.evcharging.messaging.contract.ChargerTelemetryPayload;
import com.evcharging.messaging.contract.ChargerTelemetryReceived;
import com.evcharging.messaging.contract.DomainEventEnvelope;
import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class MqttTelemetryMapperTest {

    private static final UUID EVENT_ID = UUID.fromString("7a75c31d-34ae-4cb3-8c80-a606ed9d2b51");
    private static final Instant RECEIVED_AT = Instant.parse("2024-01-01T00:00:01Z");
    private static final MqttTelemetryMapper MAPPER = new MqttTelemetryMapper(
            new ObjectMapper(),
            Clock.fixed(RECEIVED_AT, ZoneOffset.UTC),
            () -> EVENT_ID
    );

    @Test
    void mapsMqttPayloadToTelemetryEvent() {
        String rawPayload = """
                {
                  "stationId": "EV001",
                  "chargerNo": 3,
                  "charging": true,
                  "power": 120.0,
                  "timestamp": 1704067200000,
                  "voltage": 24.0,
                  "current": 5.0
                }
                """;

        DomainEventEnvelope<ChargerTelemetryPayload> event = MAPPER.toEvent(rawPayload);

        assertThat(event.eventId()).isEqualTo(EVENT_ID);
        assertThat(event.eventType()).isEqualTo(ChargerTelemetryReceived.EVENT_TYPE);
        assertThat(event.stationId()).isEqualTo("EV001");
        assertThat(event.evseId()).isEqualTo(3);
        assertThat(event.occurredAt()).isEqualTo(Instant.ofEpochMilli(1704067200000L));
        assertThat(event.receivedAt()).isEqualTo(RECEIVED_AT);
        assertThat(event.payload().charging()).isTrue();
        assertThat(event.payload().power()).isEqualByComparingTo("120.0");
    }

    @Test
    void rejectsInvalidJson() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MAPPER.toEvent("not-json"))
                .withMessage("invalid MQTT telemetry payload");
    }

    @Test
    void rejectsChargerNumberOutsideFivePortRange() {
        String rawPayload = """
                {
                  "stationId": "EV001",
                  "chargerNo": 6,
                  "charging": false,
                  "power": 0.0,
                  "timestamp": 1704067200000,
                  "voltage": 0.0,
                  "current": 0.0
                }
                """;

        assertThatIllegalArgumentException()
                .isThrownBy(() -> MAPPER.toEvent(rawPayload))
                .withMessage("chargerNo must be between 1 and 5");
    }

    @Test
    void rejectsBlankPayload() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MAPPER.toEvent("  "))
                .withMessage("rawPayload must not be blank");
    }
}
