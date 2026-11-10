package com.evcharging.controlplane.transaction;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Test-only consumer: a second PostgreSQL instance owns one effect per closeout event. */
public final class Exp0004IndependentConsumerProcess {
    private Exp0004IndependentConsumerProcess() {}

    static void createTables(String jdbcUrl) throws SQLException {
        try (var connection = DriverManager.getConnection(jdbcUrl, "postgres", "postgres");
             var statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE exp0004_effect (
                        event_id UUID PRIMARY KEY,
                        transaction_id TEXT NOT NULL UNIQUE,
                        energy_wh NUMERIC NOT NULL
                    )
                    """);
            statement.execute("""
                    CREATE TABLE exp0004_effect_conflict (
                        event_id UUID NOT NULL REFERENCES exp0004_effect(event_id),
                        received_energy_wh NUMERIC NOT NULL,
                        PRIMARY KEY (event_id, received_energy_wh)
                    )
                    """);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 6) throw new IllegalArgumentException("mode, brokers, topic, jdbcUrl, groupId, expectedRecords");
        String mode = args[0];
        if (!"normal".equals(mode) && !"crash-after-db".equals(mode)) {
            throw new IllegalArgumentException("unknown mode " + mode);
        }
        int expectedRecords = Integer.parseInt(args[5]);
        var mapper = new ObjectMapper();
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args[1],
                ConsumerConfig.GROUP_ID_CONFIG, args[4],
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1",
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000"));
             var connection = DriverManager.getConnection(args[3], "postgres", "postgres")) {
            connection.setAutoCommit(false);
            consumer.subscribe(java.util.List.of(args[2]));
            int seen = 0;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
            while (seen < expectedRecords && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    var body = mapper.readTree(record.value());
                    UUID eventId = UUID.fromString(body.path("eventId").asText());
                    var payload = body.path("payload");
                    var energy = payload.path("energyWh");
                    if (!energy.isNumber()) throw new IllegalArgumentException("energyWh must be numeric");
                    storeEffect(connection, eventId, payload.path("transactionId").asText(), energy.decimalValue());
                    connection.commit();
                    System.out.println("EFFECT_COMMITTED offset=" + record.offset()
                            + " eventId=" + eventId);
                    System.out.flush();
                    if ("crash-after-db".equals(mode)) Runtime.getRuntime().halt(137);
                    consumer.commitSync();
                    seen++;
                }
            }
            if (seen != expectedRecords) throw new IllegalStateException("consumed " + seen + " of " + expectedRecords);
            System.out.println("CONSUMED " + seen);
        }
    }

    private static void storeEffect(Connection connection, UUID eventId, String transactionId, BigDecimal energyWh)
            throws SQLException {
        try (var statement = connection.prepareStatement("""
                INSERT INTO exp0004_effect (event_id, transaction_id, energy_wh)
                VALUES (?, ?, ?) ON CONFLICT DO NOTHING
                """)) {
            statement.setObject(1, eventId);
            statement.setString(2, transactionId);
            statement.setBigDecimal(3, energyWh);
            if (statement.executeUpdate() == 1) return;
        }
        try (var statement = connection.prepareStatement("""
                INSERT INTO exp0004_effect_conflict (event_id, received_energy_wh)
                SELECT event_id, ? FROM exp0004_effect WHERE event_id = ? AND energy_wh <> ?
                ON CONFLICT DO NOTHING
                """)) {
            statement.setBigDecimal(1, energyWh);
            statement.setObject(2, eventId);
            statement.setBigDecimal(3, energyWh);
            statement.executeUpdate();
        }
    }
}
