package com.evcharging.telemetryworker.adapter.persistence;

import com.evcharging.telemetryworker.application.TelemetryObservation;
import com.evcharging.telemetryworker.application.TelemetryQueryRepository;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JdbcTelemetryQueryRepository implements TelemetryQueryRepository {
    private static final String SELECT = """
            SELECT station_id, evse_id, charging, power, voltage, current,
                   occurred_at, received_at, updated_at, last_event_id
            FROM evse_telemetry_latest
            WHERE station_id = :stationId
            """;

    private static final RowMapper<TelemetryObservation> OBSERVATION = (row, index) ->
            new TelemetryObservation(
                    row.getString("station_id"), row.getInt("evse_id"), row.getBoolean("charging"),
                    row.getBigDecimal("power"), row.getBigDecimal("voltage"), row.getBigDecimal("current"),
                    row.getObject("occurred_at", OffsetDateTime.class).toInstant(),
                    row.getObject("received_at", OffsetDateTime.class).toInstant(),
                    row.getObject("updated_at", OffsetDateTime.class).toInstant(),
                    row.getObject("last_event_id", UUID.class));

    private final JdbcClient jdbc;

    public JdbcTelemetryQueryRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public List<TelemetryObservation> findByStation(String stationId) {
        return jdbc.sql(SELECT + " ORDER BY evse_id")
                .param("stationId", stationId).query(OBSERVATION).list();
    }

    @Override
    public Optional<TelemetryObservation> findByEvse(String stationId, int evseId) {
        return jdbc.sql(SELECT + " AND evse_id = :evseId")
                .param("stationId", stationId).param("evseId", evseId).query(OBSERVATION).optional();
    }
}
