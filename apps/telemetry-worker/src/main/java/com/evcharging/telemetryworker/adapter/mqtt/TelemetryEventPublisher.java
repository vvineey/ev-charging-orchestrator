package com.evcharging.telemetryworker.adapter.mqtt;

import com.evcharging.messaging.contract.ChargerTelemetryPayload;
import com.evcharging.messaging.contract.DomainEventEnvelope;

public interface TelemetryEventPublisher {

    void publish(DomainEventEnvelope<ChargerTelemetryPayload> event);
}
