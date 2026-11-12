package com.evcharging.controlplane.transaction;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Repository
class JdbcTransactionUsageOutbox {
    record ClaimedEvent(UUID eventId, String topic, String messageKey, String payload, UUID claimToken) { }

    private final JdbcClient jdbc;
    private final ObjectMapper mapper;
    private final String topic;

    JdbcTransactionUsageOutbox(JdbcClient jdbc, ObjectMapper mapper,
            @Value("${transaction.outbox.topic:charging.transaction.usage-provisional.v1}") String topic) {
        if (topic.isBlank()) throw new IllegalArgumentException("outbox topic must not be blank");
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.topic = topic;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void appendCandidate(ObservedTransactionRecord source) {
        var candidate = jdbc.sql("""
                UPDATE transaction_session_candidate SET aggregate_version = aggregate_version + 1
                WHERE charging_station_id = :chargingStationId AND transaction_id = :transactionId
                RETURNING station_id, evse_id, started_at, ended_at, energy_wh, candidate_status,
                          calculated_at, invalidated_at, invalidation_reason, aggregate_version
                """)
                .param("chargingStationId", source.chargingStationId()).param("transactionId", source.transactionId())
                .query((rs, rowNum) -> {
                    var snapshot = mapper.createObjectNode();
                    snapshot.put("transactionId", source.transactionId());
                    snapshot.put("evseId", rs.getInt("evse_id"));
                    snapshot.put("startedAt", instant(rs, "started_at"));
                    snapshot.put("endedAt", instant(rs, "ended_at"));
                    snapshot.put("energyWh", rs.getBigDecimal("energy_wh"));
                    snapshot.put("candidateStatus", rs.getString("candidate_status"));
                    snapshot.put("calculatedAt", instant(rs, "calculated_at"));
                    snapshot.put("invalidatedAt", instant(rs, "invalidated_at"));
                    snapshot.put("invalidationReason", rs.getString("invalidation_reason"));
                    snapshot.put("aggregateVersion", rs.getLong("aggregate_version"));
                    return new CandidateSnapshot(rs.getString("station_id"), snapshot);
                }).single();
        UUID eventId = UUID.randomUUID();
        var snapshot = candidate.payload();
        boolean invalidated = "INVALIDATED".equals(snapshot.get("candidateStatus").asString());
        String eventType = invalidated ? "UsageProvisionalInvalidated" : "UsageProvisionalCalculated";
        var envelope = mapper.createObjectNode();
        envelope.put("eventId", eventId.toString());
        envelope.put("eventType", eventType);
        envelope.put("schemaVersion", 1);
        envelope.put("occurredAt", snapshot.get(invalidated ? "invalidatedAt" : "calculatedAt").asString());
        envelope.put("receivedAt", source.receivedAt().toString());
        envelope.put("stationId", candidate.stationId());
        envelope.put("evseId", snapshot.get("evseId").asInt());
        envelope.putNull("requestId");
        envelope.put("correlationId", source.eventId().toString());
        envelope.put("chargingStationId", source.chargingStationId());
        envelope.set("payload", snapshot);
        String messageKey = candidate.stationId();
        jdbc.sql("""
                INSERT INTO transaction_usage_outbox
                  (event_id, charging_station_id, transaction_id, aggregate_version, event_type, topic, message_key, payload)
                VALUES (:eventId, :chargingStationId, :transactionId, :version, :eventType, :topic, :messageKey, CAST(:payload AS jsonb))
                """)
                .param("eventId", eventId).param("chargingStationId", source.chargingStationId())
                .param("transactionId", source.transactionId()).param("version", snapshot.get("aggregateVersion").asLong())
                .param("eventType", eventType).param("topic", topic).param("messageKey", messageKey)
                .param("payload", mapper.writeValueAsString(envelope)).update();
        jdbc.sql("INSERT INTO transaction_usage_outbox_delivery (event_id) VALUES (:eventId)")
                .param("eventId", eventId).update();
    }

    List<ClaimedEvent> claim(Duration lease, int batchSize) {
        if (lease.isNegative() || lease.toMillis() < 1 || batchSize < 1) {
            throw new IllegalArgumentException("positive lease and batch size required");
        }
        UUID token = UUID.randomUUID();
        return jdbc.sql("""
                WITH eligible AS (
                    SELECT d.event_id FROM transaction_usage_outbox_delivery d
                    JOIN transaction_usage_outbox o USING (event_id)
                    WHERE (d.status = 'PENDING' OR (d.status = 'PROCESSING' AND d.claim_until <= clock_timestamp()))
                      AND NOT EXISTS (
                        SELECT 1 FROM transaction_usage_outbox previous
                        JOIN transaction_usage_outbox_delivery delivery USING (event_id)
                        WHERE previous.charging_station_id = o.charging_station_id
                          AND previous.transaction_id = o.transaction_id
                          AND previous.aggregate_version < o.aggregate_version AND delivery.status <> 'PUBLISHED')
                    ORDER BY o.created_at, o.event_id
                    FOR UPDATE OF d SKIP LOCKED LIMIT :batchSize
                ), claimed AS (
                    UPDATE transaction_usage_outbox_delivery d
                    SET status = 'PROCESSING', claim_token = :token, claimed_at = clock_timestamp(),
                        claim_until = clock_timestamp() + :leaseMillis * interval '1 millisecond'
                    FROM eligible WHERE d.event_id = eligible.event_id
                    RETURNING d.event_id, d.claim_token
                )
                SELECT o.event_id, o.topic, o.message_key, o.payload::text AS payload, c.claim_token
                FROM claimed c JOIN transaction_usage_outbox o USING (event_id)
                """)
                .param("batchSize", batchSize).param("token", token).param("leaseMillis", lease.toMillis())
                .query((rs, rowNum) -> new ClaimedEvent(rs.getObject("event_id", UUID.class), rs.getString("topic"),
                        rs.getString("message_key"), rs.getString("payload"), rs.getObject("claim_token", UUID.class))).list();
    }

    boolean markPublished(ClaimedEvent event) {
        return jdbc.sql("""
                UPDATE transaction_usage_outbox_delivery
                SET status = 'PUBLISHED', published_at = clock_timestamp(),
                    claim_token = NULL, claimed_at = NULL, claim_until = NULL
                WHERE event_id = :eventId AND status = 'PROCESSING' AND claim_token = :token
                  AND claim_until > clock_timestamp()
                """)
                .param("eventId", event.eventId()).param("token", event.claimToken()).update() == 1;
    }

    boolean block(ClaimedEvent event, String errorType) {
        return jdbc.sql("""
                UPDATE transaction_usage_outbox_delivery
                SET status = 'BLOCKED', last_error_type = :errorType,
                    claim_token = NULL, claimed_at = NULL, claim_until = NULL
                WHERE event_id = :eventId AND status = 'PROCESSING' AND claim_token = :token
                  AND claim_until > clock_timestamp()
                """)
                .param("eventId", event.eventId()).param("token", event.claimToken())
                .param("errorType", errorType).update() == 1;
    }

    private String instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant().toString();
    }

    private record CandidateSnapshot(String stationId, tools.jackson.databind.node.ObjectNode payload) { }
}
