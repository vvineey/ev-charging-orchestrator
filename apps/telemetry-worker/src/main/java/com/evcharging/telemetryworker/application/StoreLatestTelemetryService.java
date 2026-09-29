package com.evcharging.telemetryworker.application;

import com.evcharging.messaging.contract.ChargerTelemetryPayload;
import com.evcharging.messaging.contract.ChargerTelemetryReceived;
import com.evcharging.messaging.contract.DomainEventEnvelope;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.temporal.ChronoUnit;
import java.util.Objects;

@Service
public class StoreLatestTelemetryService {

    private final LatestTelemetryRepository repository;

    public StoreLatestTelemetryService(LatestTelemetryRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public boolean store(DomainEventEnvelope<ChargerTelemetryPayload> event) {
        Objects.requireNonNull(event, "event must not be null");
        if (!ChargerTelemetryReceived.EVENT_TYPE.equals(event.eventType())
                || event.schemaVersion() != ChargerTelemetryReceived.SCHEMA_VERSION) {
            throw new IllegalArgumentException("expected ChargerTelemetryReceived envelope v1");
        }
        var payload = event.payload();
        if (payload.timestamp() != event.occurredAt().toEpochMilli()) {
            throw new IllegalArgumentException("payload.timestamp must match occurredAt");
        }
        return repository.upsert(new LatestTelemetry(
                event.stationId(), event.evseId(), payload.charging(),
                payload.power(), payload.voltage(), payload.current(),
                event.occurredAt().truncatedTo(ChronoUnit.MICROS),
                event.receivedAt().truncatedTo(ChronoUnit.MICROS), event.eventId()
        ));
    }
}
