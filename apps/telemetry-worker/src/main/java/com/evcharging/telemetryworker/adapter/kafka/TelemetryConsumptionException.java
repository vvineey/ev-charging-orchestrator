package com.evcharging.telemetryworker.adapter.kafka;

import java.util.UUID;

/** Carries safe metadata only; parser/SQL exception messages can contain input. */
public class TelemetryConsumptionException extends RuntimeException {
    private final String stage;
    private final String reason;
    private final UUID eventId;

    public TelemetryConsumptionException(String stage, String reason, UUID eventId) {
        super("telemetry " + stage + " failure: " + reason);
        this.stage = stage;
        this.reason = reason;
        this.eventId = eventId;
    }

    public String stage() { return stage; }
    public String reason() { return reason; }
    public UUID eventId() { return eventId; }
}
