package com.evcharging.controlplane.transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** One-iteration test-only Outbox writer or Polling relay in a separate JVM. */
public final class Exp0004OutboxPollingProcess {
    private Exp0004OutboxPollingProcess() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 6) throw new IllegalArgumentException("mode, jdbcUrl, brokers, topic, transactionId, expectedWh");
        switch (args[0]) {
            case "write" -> write(args, false, false);
            case "write-then-halt" -> write(args, true, false);
            case "write-rollback-before-outbox" -> write(args, false, true);
            case "relay" -> relay(args, false);
            case "relay-ack-then-halt" -> relay(args, true);
            default -> throw new IllegalArgumentException("unknown mode " + args[0]);
        }
    }

    private static void write(String[] args, boolean halt, boolean rollbackBeforeOutbox) throws SQLException {
        try (var connection = DriverManager.getConnection(args[1], "postgres", "postgres")) {
            connection.setAutoCommit(false);
            try {
                UUID eventId = Exp0004DirectPublishProcess.insertTestCloseout(
                        connection, args[4], new BigDecimal(args[5]));
                if (rollbackBeforeOutbox) throw new SQLException("injected rollback before Outbox insert");
                try (var statement = connection.prepareStatement("""
                        INSERT INTO exp0004_outbox (event_id, transaction_id, station_id, payload)
                        VALUES (?, ?, 'ST-TEST', ?)
                        """)) {
                    statement.setObject(1, eventId);
                    statement.setString(2, args[4]);
                    statement.setString(3, Exp0004DirectPublishProcess.eventPayload(eventId, args[4], args[5]));
                    statement.executeUpdate();
                }
                connection.commit();
                System.out.println("OUTBOX_COMMITTED_BEFORE_RELAY " + eventId);
                System.out.flush();
                if (halt) Runtime.getRuntime().halt(137);
            } catch (SQLException | RuntimeException failure) {
                connection.rollback();
                throw failure;
            }
        }
    }

    private static void relay(String[] args, boolean haltAfterAck) throws Exception {
        try (var connection = DriverManager.getConnection(args[1], "postgres", "postgres")) {
            UUID eventId;
            String stationId;
            String payload;
            try (var statement = connection.prepareStatement("""
                    SELECT event_id, station_id, payload FROM exp0004_outbox
                    WHERE transaction_id = ? AND published_at IS NULL
                    """)) {
                statement.setString(1, args[4]);
                try (var row = statement.executeQuery()) {
                    if (!row.next()) throw new IllegalStateException("no pending Outbox row");
                    eventId = row.getObject("event_id", UUID.class);
                    stationId = row.getString("station_id");
                    payload = row.getString("payload");
                }
            }
            var producerProperties = new HashMap<String, Object>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[2],
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.ACKS_CONFIG, "all"));
            if (Boolean.parseBoolean(System.getenv("EVC_WSS_OUTBOX_QUORUM_TEST"))) {
                producerProperties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5000);
                producerProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
                producerProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
            }
            try (var producer = new KafkaProducer<String, String>(producerProperties)) {
                producer.send(new ProducerRecord<>(args[3], stationId, payload)).get(10, TimeUnit.SECONDS);
            }
            if (haltAfterAck) {
                System.out.println("ACKED_BEFORE_MARK " + eventId);
                System.out.flush();
                Runtime.getRuntime().halt(137);
            }
            try (var statement = connection.prepareStatement("""
                    UPDATE exp0004_outbox SET published_at = now()
                    WHERE event_id = ? AND published_at IS NULL
                    """)) {
                statement.setObject(1, eventId);
                if (statement.executeUpdate() != 1) throw new IllegalStateException("Outbox completion lost");
            }
            System.out.println("RELAY_PUBLISHED " + eventId);
        }
    }
}
