package com.evcharging.telemetryworker.adapter.persistence;

import com.evcharging.telemetryworker.application.LatestTelemetry;
import com.evcharging.telemetryworker.application.LatestTelemetryRepository;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.ZoneOffset;

@Repository
public class JdbcLatestTelemetryRepository implements LatestTelemetryRepository {

    private static final String UPSERT = """
            INSERT INTO evse_telemetry_latest (
                station_id, evse_id, charging, power, voltage, current,
                occurred_at, received_at, last_event_id
            ) VALUES (
                :stationId, :evseId, :charging, :power, :voltage, :current,
                :occurredAt, :receivedAt, :eventId
            )
            ON CONFLICT (station_id, evse_id) DO UPDATE
            SET charging = EXCLUDED.charging,
                power = EXCLUDED.power,
                voltage = EXCLUDED.voltage,
                current = EXCLUDED.current,
                occurred_at = EXCLUDED.occurred_at,
                received_at = EXCLUDED.received_at,
                last_event_id = EXCLUDED.last_event_id,
                updated_at = clock_timestamp()
            WHERE EXCLUDED.occurred_at > evse_telemetry_latest.occurred_at
               OR (EXCLUDED.occurred_at = evse_telemetry_latest.occurred_at
                   AND EXCLUDED.received_at > evse_telemetry_latest.received_at)
            """;

    private final JdbcClient jdbc;

    public JdbcLatestTelemetryRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public boolean upsert(LatestTelemetry telemetry) {
        return jdbc.sql(UPSERT)
                .param("stationId", telemetry.stationId())
                .param("evseId", telemetry.evseId())
                .param("charging", telemetry.charging())
                .param("power", telemetry.power())
                .param("voltage", telemetry.voltage())
                .param("current", telemetry.current())
                .param("occurredAt", telemetry.occurredAt().atOffset(ZoneOffset.UTC))
                .param("receivedAt", telemetry.receivedAt().atOffset(ZoneOffset.UTC))
                .param("eventId", telemetry.eventId())
                .update() == 1;
    }
}
