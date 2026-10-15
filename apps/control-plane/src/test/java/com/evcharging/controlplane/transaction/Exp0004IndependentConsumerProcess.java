package com.evcharging.controlplane.transaction;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import tools.jackson.databind.ObjectMapper;

import java.sql.DriverManager;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Test-only consumer: a second PostgreSQL instance owns one effect per closeout event. */
public final class Exp0004IndependentConsumerProcess {
    private Exp0004IndependentConsumerProcess() {}

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
            consumer.subscribe(java.util.List.of(args[2]));
            int seen = 0;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
            while (seen < expectedRecords && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    var body = mapper.readTree(record.value());
                    UUID eventId = UUID.fromString(body.path("eventId").asText());
                    String transactionId = body.path("payload").path("transactionId").asText();
                    try (var statement = connection.prepareStatement("""
                            INSERT INTO exp0004_effect (event_id, transaction_id)
                            VALUES (?, ?) ON CONFLICT DO NOTHING
                            """)) {
                        statement.setObject(1, eventId);
                        statement.setString(2, transactionId);
                        statement.executeUpdate();
                    }
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
}
