package com.evcharging.controlplane.transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Isolated test-only direct publisher. The crash mode halts after DB commit and before Kafka send. */
public final class Exp0004DirectPublishProcess {
    private Exp0004DirectPublishProcess() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 6) throw new IllegalArgumentException("mode, jdbcUrl, brokers, topic, transactionId, expectedWh");
        String mode = args[0];
        String transactionId = args[4];
        UUID eventId = commitTestCloseout(args[1], transactionId, new BigDecimal(args[5]));
        if ("after-commit-before-send".equals(mode)) {
            System.out.println("COMMITTED_BEFORE_SEND " + eventId);
            System.out.flush();
            Runtime.getRuntime().halt(137);
        }
        if (!"normal".equals(mode)) throw new IllegalArgumentException("unknown mode " + mode);
        String payload = eventPayload(eventId, transactionId, args[5]);
        try (var producer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[2],
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"))) {
            producer.send(new ProducerRecord<>(args[3], "ST-TEST", payload)).get(10, TimeUnit.SECONDS);
        }
        System.out.println("PUBLISHED " + eventId);
    }

    private static UUID commitTestCloseout(String jdbcUrl, String transactionId, BigDecimal expectedWh)
            throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, "postgres", "postgres")) {
            connection.setAutoCommit(false);
            try {
                UUID proposedId = insertTestCloseout(connection, transactionId, expectedWh);
                connection.commit();
                return proposedId;
            } catch (SQLException | RuntimeException failure) {
                connection.rollback();
                throw failure;
            }
        }
    }

    static String eventPayload(UUID eventId, String transactionId, String expectedWh) {
        return "{\"eventId\":\"" + eventId
                + "\",\"eventType\":\"TransactionCloseoutSimulated\",\"schemaVersion\":1,"
                + "\"stationId\":\"ST-TEST\",\"payload\":{\"transactionId\":\"" + transactionId
                + "\",\"energyWh\":" + expectedWh + "}}";
    }

    static UUID insertTestCloseout(Connection connection, String transactionId, BigDecimal expectedWh)
            throws SQLException {
        UUID proposedId = UUID.nameUUIDFromBytes(
                ("exp0004-closeout:" + transactionId).getBytes(StandardCharsets.UTF_8));
        int inserted;
        try (var statement = connection.prepareStatement("""
                        INSERT INTO exp0004_closeout
                            (charging_station_id, transaction_id, station_id, energy_wh, event_id)
                        SELECT c.charging_station_id, c.transaction_id, c.station_id, c.energy_wh, ?
                        FROM transaction_session_candidate c
                        JOIN transaction_inbox_state s USING (charging_station_id, transaction_id)
                        WHERE c.charging_station_id = 'CS-TEST' AND c.transaction_id = ?
                          AND c.candidate_status = 'PROVISIONAL' AND s.status = 'PROVISIONAL'
                          AND c.energy_wh = ?
                          AND NOT EXISTS (
                              SELECT 1 FROM transaction_inbox_conflict conflict
                              WHERE conflict.charging_station_id = c.charging_station_id
                                AND conflict.transaction_id = c.transaction_id)
                          AND (SELECT count(*) FROM transaction_inbox_event e
                               WHERE e.charging_station_id = c.charging_station_id
                                 AND e.transaction_id = c.transaction_id) = 2
                          AND EXISTS (
                              SELECT 1 FROM transaction_inbox_event e
                              WHERE e.charging_station_id = c.charging_station_id
                                AND e.transaction_id = c.transaction_id AND e.seq_no = 0
                                AND e.event_type = 'Started')
                          AND EXISTS (
                              SELECT 1 FROM transaction_inbox_event e
                              WHERE e.charging_station_id = c.charging_station_id
                                AND e.transaction_id = c.transaction_id AND e.seq_no = 1
                                AND e.event_type = 'Ended')
                        ON CONFLICT (charging_station_id, transaction_id) DO NOTHING
                """)) {
            statement.setObject(1, proposedId);
            statement.setString(2, transactionId);
            statement.setBigDecimal(3, expectedWh);
            inserted = statement.executeUpdate();
        }
        if (inserted != 1) throw new SQLException("test closeout precondition failed or duplicate");
        return proposedId;
    }
}
