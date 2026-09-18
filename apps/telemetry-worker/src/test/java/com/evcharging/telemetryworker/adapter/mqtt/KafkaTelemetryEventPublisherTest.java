package com.evcharging.telemetryworker.adapter.mqtt;

import com.evcharging.messaging.contract.ChargerTelemetryPayload;
import com.evcharging.messaging.contract.ChargerTelemetryReceived;
import com.evcharging.messaging.contract.DomainEventEnvelope;
import com.evcharging.messaging.contract.EvseId;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaTelemetryEventPublisherTest {

    @Test
    void sendsSerializedEventWithStationIdAsKafkaKey() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        ObjectMapper objectMapper = mock(ObjectMapper.class);
        DomainEventEnvelope<ChargerTelemetryPayload> event = telemetryEvent();

        when(objectMapper.writeValueAsString(event)).thenReturn("{\"eventType\":\"ChargerTelemetryReceived\"}");

        KafkaTelemetryEventPublisher publisher = new KafkaTelemetryEventPublisher(
                kafkaTemplate,
                objectMapper,
                "charger.telemetry"
        );
        publisher.publish(event);

        verify(kafkaTemplate).send(
                "charger.telemetry",
                "EV001",
                "{\"eventType\":\"ChargerTelemetryReceived\"}"
        );
    }

    private static DomainEventEnvelope<ChargerTelemetryPayload> telemetryEvent() {
        Instant occurredAt = Instant.parse("2024-01-01T00:00:00Z");
        return ChargerTelemetryReceived.create(
                UUID.fromString("7a75c31d-34ae-4cb3-8c80-a606ed9d2b51"),
                occurredAt,
                occurredAt.plusMillis(123),
                "EV001",
                new EvseId(1),
                null,
                new ChargerTelemetryPayload(
                        occurredAt.toEpochMilli(),
                        true,
                        new BigDecimal("120.0"),
                        new BigDecimal("24.0"),
                        new BigDecimal("5.0")
                )
        );
    }
}
