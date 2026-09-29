package com.evcharging.telemetryworker.adapter.kafka;

import com.evcharging.messaging.contract.ChargerTelemetryPayload;
import com.evcharging.messaging.contract.ChargerTelemetryReceived;
import com.evcharging.messaging.contract.DomainEventEnvelope;
import com.evcharging.messaging.contract.EvseId;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

final class TelemetryFixtures {
    static final String STATION = "consumer-fixture";
    static final Instant OBSERVED = Instant.parse("2026-09-29T00:00:00.123456789Z");
    static final Instant RECEIVED = Instant.parse("2026-09-29T00:00:00.234567891Z");

    static DomainEventEnvelope<ChargerTelemetryPayload> first() {
        return event(1, 1, OBSERVED, RECEIVED, false, "120.12345678901234567890123");
    }

    static DomainEventEnvelope<ChargerTelemetryPayload> latest() {
        return event(2, 1, Instant.parse("2026-09-29T00:00:01Z"), Instant.parse("2026-09-29T00:00:02Z"), true, "99.875");
    }

    static DomainEventEnvelope<ChargerTelemetryPayload> event(int id, int evse, Instant observed, Instant received,
                                                              boolean charging, String power) {
        return ChargerTelemetryReceived.create(new UUID(0, id), observed, received, STATION, new EvseId(evse), null,
                new ChargerTelemetryPayload(observed.toEpochMilli(), charging, new BigDecimal(power),
                        new BigDecimal("230.125"), new BigDecimal("0.875")));
    }
}
