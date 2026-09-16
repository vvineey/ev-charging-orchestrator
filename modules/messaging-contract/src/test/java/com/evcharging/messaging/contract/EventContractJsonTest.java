package com.evcharging.messaging.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class EventContractJsonTest {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Test
    void preservesTheApprovedJsonContract() throws Exception {
        UUID eventId = UUID.fromString("4f36d9c0-b30a-4aa2-a981-4c7ee58371c4");
        Instant occurredAt = Instant.parse("2024-01-01T00:00:00Z");
        DomainEventEnvelope<ChargerTelemetryPayload> event = ChargerTelemetryReceived.create(
                eventId,
                occurredAt,
                occurredAt.plusMillis(123),
                "EV001",
                new EvseId(1),
                null,
                new ChargerTelemetryPayload(
                        1704067200000L,
                        true,
                        new BigDecimal("120.0"),
                        new BigDecimal("24.0"),
                        new BigDecimal("5.0")
                )
        );

        String json = objectMapper.writeValueAsString(event);
        JsonNode jsonNode = objectMapper.readTree(json);

        assertThat(jsonNode.get("eventId").asText()).isEqualTo(eventId.toString());
        assertThat(jsonNode.get("eventType").asText()).isEqualTo("ChargerTelemetryReceived");
        assertThat(jsonNode.get("schemaVersion").asInt()).isEqualTo(1);
        assertThat(jsonNode.get("evseId").asInt()).isEqualTo(1);
        assertThat(jsonNode.get("requestId").isNull()).isTrue();
        assertThat(jsonNode.get("payload").get("timestamp").asLong()).isEqualTo(1704067200000L);
        assertThat(jsonNode.get("payload").get("power").asText()).isEqualTo("120.0");
    }

    @Test
    void supportsRoundTripDeserialization() throws Exception {
        Instant occurredAt = Instant.parse("2024-01-01T00:00:00Z");
        DomainEventEnvelope<ChargerTelemetryPayload> original = ChargerTelemetryReceived.create(
                UUID.randomUUID(),
                occurredAt,
                occurredAt.plusMillis(123),
                "EV001",
                new EvseId(1),
                null,
                new ChargerTelemetryPayload(
                        occurredAt.toEpochMilli(),
                        false,
                        new BigDecimal("0.0"),
                        new BigDecimal("24.0"),
                        new BigDecimal("0.0")
                )
        );

        String json = objectMapper.writeValueAsString(original);
        DomainEventEnvelope<ChargerTelemetryPayload> restored = objectMapper.readValue(
                json,
                objectMapper.getTypeFactory().constructParametricType(
                        DomainEventEnvelope.class,
                        ChargerTelemetryPayload.class
                )
        );

        assertThat(restored).isEqualTo(original);
    }
}
