package com.evcharging.controlplane.transaction;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;

@Repository
class JdbcTransactionInbox {
    record State(String stationId, String status) { }

    private final JdbcClient jdbc;
    private final ObjectMapper mapper;

    JdbcTransactionInbox(JdbcClient jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    State ensureAndLock(ObservedTransactionRecord event) {
        jdbc.sql("""
                INSERT INTO transaction_inbox_state (charging_station_id, transaction_id, station_id)
                VALUES (:chargingStationId, :transactionId, :stationId)
                ON CONFLICT DO NOTHING
                """)
                .param("chargingStationId", event.chargingStationId())
                .param("transactionId", event.transactionId())
                .param("stationId", event.stationId()).update();
        return jdbc.sql("""
                SELECT station_id, status FROM transaction_inbox_state
                WHERE charging_station_id = :chargingStationId AND transaction_id = :transactionId
                FOR UPDATE
                """)
                .param("chargingStationId", event.chargingStationId())
                .param("transactionId", event.transactionId())
                .query((rs, rowNum) -> new State(rs.getString("station_id"), rs.getString("status"))).single();
    }

    boolean insertEvent(ObservedTransactionRecord event) {
        return jdbc.sql("""
                INSERT INTO transaction_inbox_event
                  (charging_station_id, transaction_id, seq_no, event_type, occurred_at,
                   first_received_at, first_event_id, source_document)
                VALUES (:chargingStationId, :transactionId, :seqNo, :eventType, :occurredAt,
                        :receivedAt, :eventId, CAST(:source AS jsonb))
                ON CONFLICT DO NOTHING
                """)
                .param("chargingStationId", event.chargingStationId())
                .param("transactionId", event.transactionId())
                .param("seqNo", event.seqNo())
                .param("eventType", event.eventType())
                .param("occurredAt", OffsetDateTime.ofInstant(event.occurredAt(), java.time.ZoneOffset.UTC))
                .param("receivedAt", OffsetDateTime.ofInstant(event.receivedAt(), java.time.ZoneOffset.UTC))
                .param("eventId", event.eventId())
                .param("source", event.sourceJson()).update() == 1;
    }

    boolean sameSource(ObservedTransactionRecord event) {
        return jdbc.sql("""
                SELECT source_document = CAST(:source AS jsonb)
                FROM transaction_inbox_event
                WHERE charging_station_id = :chargingStationId AND transaction_id = :transactionId
                  AND seq_no = :seqNo
                """)
                .param("source", event.sourceJson())
                .param("chargingStationId", event.chargingStationId())
                .param("transactionId", event.transactionId())
                .param("seqNo", event.seqNo()).query(Boolean.class).single();
    }

    void preserveConflict(ObservedTransactionRecord event) {
        jdbc.sql("""
                INSERT INTO transaction_inbox_conflict
                  (charging_station_id, transaction_id, seq_no, event_id, received_at, source_document)
                SELECT :chargingStationId, :transactionId, :seqNo, :eventId, :receivedAt, CAST(:source AS jsonb)
                WHERE NOT EXISTS (
                  SELECT 1 FROM transaction_inbox_conflict
                  WHERE charging_station_id = :chargingStationId AND transaction_id = :transactionId
                    AND seq_no = :seqNo AND source_document = CAST(:source AS jsonb))
                """)
                .param("chargingStationId", event.chargingStationId())
                .param("transactionId", event.transactionId())
                .param("seqNo", event.seqNo())
                .param("eventId", event.eventId())
                .param("receivedAt", OffsetDateTime.ofInstant(event.receivedAt(), java.time.ZoneOffset.UTC))
                .param("source", event.sourceJson()).update();
    }

    List<TransactionRecoveryRules.StoredEvent> events(ObservedTransactionRecord event) {
        return jdbc.sql("""
                SELECT seq_no, event_type, occurred_at, source_document::text AS source_text
                FROM transaction_inbox_event
                WHERE charging_station_id = :chargingStationId AND transaction_id = :transactionId
                ORDER BY seq_no
                """)
                .param("chargingStationId", event.chargingStationId())
                .param("transactionId", event.transactionId())
                .query(this::mapEvent).list();
    }

    private TransactionRecoveryRules.StoredEvent mapEvent(ResultSet rs, int rowNum) throws SQLException {
        try {
            JsonNode source = mapper.readTree(rs.getString("source_text"));
            Instant occurredAt = rs.getObject("occurred_at", OffsetDateTime.class).toInstant();
            return new TransactionRecoveryRules.StoredEvent(rs.getInt("seq_no"), rs.getString("event_type"),
                    occurredAt, source);
        } catch (JacksonException exception) {
            throw new IllegalStateException("stored transaction document is invalid");
        }
    }

    void updateState(ObservedTransactionRecord event, String status, String reason, Integer evseId) {
        jdbc.sql("""
                UPDATE transaction_inbox_state
                SET status = :status, hold_reason = :reason, evse_id = :evseId, updated_at = clock_timestamp()
                WHERE charging_station_id = :chargingStationId AND transaction_id = :transactionId
                  AND (status IS DISTINCT FROM :status OR hold_reason IS DISTINCT FROM CAST(:reason AS text)
                       OR evse_id IS DISTINCT FROM CAST(:evseId AS integer))
                """)
                .param("status", status).param("reason", reason).param("evseId", evseId)
                .param("chargingStationId", event.chargingStationId())
                .param("transactionId", event.transactionId()).update();
    }

    void insertSession(ObservedTransactionRecord event, TransactionRecoveryRules.Outcome result) {
        jdbc.sql("""
                INSERT INTO recovered_transaction_session
                  (charging_station_id, transaction_id, station_id, evse_id, started_at, ended_at, energy_wh)
                VALUES (:chargingStationId, :transactionId, :stationId, :evseId, :startedAt, :endedAt, :energyWh)
                ON CONFLICT DO NOTHING
                """)
                .param("chargingStationId", event.chargingStationId())
                .param("transactionId", event.transactionId())
                .param("stationId", event.stationId())
                .param("evseId", result.evseId())
                .param("startedAt", OffsetDateTime.ofInstant(result.startedAt(), java.time.ZoneOffset.UTC))
                .param("endedAt", OffsetDateTime.ofInstant(result.endedAt(), java.time.ZoneOffset.UTC))
                .param("energyWh", result.energyWh()).update();
    }
}
