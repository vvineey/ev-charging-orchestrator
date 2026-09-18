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
        DomainEventEnvelope<ChargerTelemetryPayload> event = MAPPER.toEvent(validPayload(3));

        assertThat(event.eventId()).isEqualTo(EVENT_ID);
        assertThat(event.eventType()).isEqualTo(ChargerTelemetryReceived.EVENT_TYPE);
        assertThat(event.schemaVersion()).isEqualTo(ChargerTelemetryReceived.SCHEMA_VERSION);
        assertThat(event.stationId()).isEqualTo("EV001");
        assertThat(event.evseId()).isEqualTo(3);
        assertThat(event.occurredAt()).isEqualTo(Instant.ofEpochMilli(1704067200000L));
        assertThat(event.receivedAt()).isEqualTo(RECEIVED_AT);
        assertThat(event.requestId()).isNull();
        assertThat(event.correlationId()).isEqualTo(EVENT_ID);
        assertThat(event.payload().timestamp()).isEqualTo(1704067200000L);
        assertThat(event.payload().charging()).isTrue();
        assertThat(event.payload().power()).isEqualByComparingTo("120.0");
        assertThat(event.payload().voltage()).isEqualByComparingTo("24.0");
        assertThat(event.payload().current()).isEqualByComparingTo("5.0");
    }

    @Test
    void mapsBoundaryChargerNumbersToEvseIds() {
        assertThat(MAPPER.toEvent(validPayload(1)).evseId()).isEqualTo(1);
        assertThat(MAPPER.toEvent(validPayload(5)).evseId()).isEqualTo(5);
    }

    @Test
    void rejectsInvalidJson() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MAPPER.toEvent("not-json"))
                .withMessage("invalid MQTT telemetry payload");
    }

    @Test
    void rejectsNullJsonPayload() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MAPPER.toEvent("null"))
                .withMessage("invalid MQTT telemetry payload");
    }

    @Test
    void rejectsChargerNumberOutsideFivePortRange() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MAPPER.toEvent(validPayload(6)))
                .withMessage("chargerNo must be between 1 and 5");
    }

    @Test
    void rejectsBlankPayload() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MAPPER.toEvent("  "))
                .withMessage("rawPayload must not be blank");
    }

    @Test
    void rejectsMissingStationId() {
        String rawPayload = """
                {
                  "chargerNo": 3,
                  "charging": true,
                  "power": 120.0,
                  "timestamp": 1704067200000,
                  "voltage": 24.0,
                  "current": 5.0
                }
                """;

        assertThatIllegalArgumentException()
                .isThrownBy(() -> MAPPER.toEvent(rawPayload))
                .withMessage("stationId must not be blank");
    }

    @Test
    void rejectsMissingChargingState() {
        String rawPayload = """
                {
                  "stationId": "EV001",
                  "chargerNo": 3,
                  "power": 120.0,
                  "timestamp": 1704067200000,
                  "voltage": 24.0,
                  "current": 5.0
                }
                """;

        assertThatIllegalArgumentException()
                .isThrownBy(() -> MAPPER.toEvent(rawPayload))
                .withMessage("charging must not be null");
    }

    @Test
    void rejectsMissingTimestamp() {
        String rawPayload = """
                {
                  "stationId": "EV001",
                  "chargerNo": 3,
                  "charging": true,
                  "power": 120.0,
                  "voltage": 24.0,
                  "current": 5.0
                }
                """;

        assertThatIllegalArgumentException()
                .isThrownBy(() -> MAPPER.toEvent(rawPayload))
                .withMessage("timestamp must not be null");
    }

    @Test
    void rejectsNegativeTimestamp() {
        String rawPayload = """
                {
                  "stationId": "EV001",
                  "chargerNo": 3,
                  "charging": true,
                  "power": 120.0,
                  "timestamp": -1,
                  "voltage": 24.0,
                  "current": 5.0
                }
                """;

        assertThatIllegalArgumentException()
                .isThrownBy(() -> MAPPER.toEvent(rawPayload))
                .withMessage("timestamp must not be negative");
    }

    @Test
    void rejectsMissingTelemetryMeasurements() {
        String rawPayload = """
                {
                  "stationId": "EV001",
                  "chargerNo": 3,
                  "charging": true,
                  "timestamp": 1704067200000,
                  "voltage": 24.0,
                  "current": 5.0
                }
                """;

        assertThatIllegalArgumentException()
                .isThrownBy(() -> MAPPER.toEvent(rawPayload))
                .withMessage("power must not be null");
    }

    private static String validPayload(int chargerNo) {
        return """
                {
                  "stationId": "EV001",
                  "chargerNo": %d,
                  "charging": true,
                  "power": 120.0,
                  "timestamp": 1704067200000,
                  "voltage": 24.0,
                  "current": 5.0
                }
                """.formatted(chargerNo);
    }
}
