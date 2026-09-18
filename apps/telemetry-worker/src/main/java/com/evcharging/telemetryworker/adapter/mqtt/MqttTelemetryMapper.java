package com.evcharging.telemetryworker.adapter.mqtt;

import com.evcharging.messaging.contract.ChargerTelemetryPayload;
import com.evcharging.messaging.contract.ChargerTelemetryReceived;
import com.evcharging.messaging.contract.DomainEventEnvelope;
import com.evcharging.messaging.contract.EvseId;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

@Component
public class MqttTelemetryMapper {

    private static final int MIN_CHARGER_NO = 1;
    private static final int MAX_CHARGER_NO = 5;

    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final Supplier<UUID> eventIdSupplier;

    @Autowired
    public MqttTelemetryMapper(ObjectMapper objectMapper) {
        this(objectMapper, Clock.systemUTC(), UUID::randomUUID);
    }

    MqttTelemetryMapper(ObjectMapper objectMapper, Clock clock, Supplier<UUID> eventIdSupplier) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.eventIdSupplier = Objects.requireNonNull(eventIdSupplier, "eventIdSupplier must not be null");
    }

    public DomainEventEnvelope<ChargerTelemetryPayload> toEvent(String rawPayload) {
        if (rawPayload == null || rawPayload.isBlank()) {
            throw new IllegalArgumentException("rawPayload must not be blank");
        }

        MqttTelemetryMessage message = readMessage(rawPayload);
        validateMessage(message);

        Instant occurredAt = Instant.ofEpochMilli(message.timestamp());
        ChargerTelemetryPayload payload = new ChargerTelemetryPayload(
                message.timestamp(),
                message.charging(),
                message.power(),
                message.voltage(),
                message.current()
        );

        return ChargerTelemetryReceived.create(
                eventIdSupplier.get(),
                occurredAt,
                clock.instant(),
                message.stationId(),
                new EvseId(message.chargerNo()),
                null,
                payload
        );
    }

    private MqttTelemetryMessage readMessage(String rawPayload) {
        try {
            return objectMapper.readValue(rawPayload, MqttTelemetryMessage.class);
        } catch (JacksonException exception) {
            throw new IllegalArgumentException("invalid MQTT telemetry payload", exception);
        }
    }

    private void validateMessage(MqttTelemetryMessage message) {
        if (message == null) {
            throw new IllegalArgumentException("invalid MQTT telemetry payload");
        }
        if (message.stationId() == null || message.stationId().isBlank()) {
            throw new IllegalArgumentException("stationId must not be blank");
        }
        if (message.chargerNo() == null) {
            throw new IllegalArgumentException("chargerNo must not be null");
        }
        if (message.charging() == null) {
            throw new IllegalArgumentException("charging must not be null");
        }
        if (message.timestamp() == null) {
            throw new IllegalArgumentException("timestamp must not be null");
        }
        if (message.timestamp() < 0) {
            throw new IllegalArgumentException("timestamp must not be negative");
        }
        if (message.power() == null) {
            throw new IllegalArgumentException("power must not be null");
        }
        if (message.voltage() == null) {
            throw new IllegalArgumentException("voltage must not be null");
        }
        if (message.current() == null) {
            throw new IllegalArgumentException("current must not be null");
        }
        validateChargerNo(message.chargerNo());
    }

    private void validateChargerNo(int chargerNo) {
        if (chargerNo < MIN_CHARGER_NO || chargerNo > MAX_CHARGER_NO) {
            throw new IllegalArgumentException("chargerNo must be between 1 and 5");
        }
    }
}
