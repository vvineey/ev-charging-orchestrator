package com.evcharging.controlplane.transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Isolated synthetic contract shared by the Polling and CDC experiments. */
final class Exp0003OutboxFixture {
    private Exp0003OutboxFixture() {}

    interface TimingSink {
        void record(int events, long selectNs, long kafkaAckNs, long deliveryCommitNs);
    }

    static void createSchema(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS exp0003_work (
                        work_key TEXT PRIMARY KEY,
                        status TEXT NOT NULL CHECK (status = 'TEST_COMMITTED')
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS exp0003_outbox (
                        id UUID PRIMARY KEY,
                        aggregatetype TEXT NOT NULL,
                        aggregateid TEXT NOT NULL UNIQUE REFERENCES exp0003_work(work_key),
                        type TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS exp0003_delivery (
                        outbox_id UUID PRIMARY KEY REFERENCES exp0003_outbox(id),
                        published_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS exp0003_effect (
                        event_id UUID PRIMARY KEY,
                        work_key TEXT NOT NULL UNIQUE,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                    )
                    """);
        }
    }

    static void clear(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("TRUNCATE exp0003_effect, exp0003_delivery, exp0003_outbox, exp0003_work");
        }
    }

    static UUID commitWork(DataSource dataSource, String workKey, boolean rollback) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                int inserted;
                try (PreparedStatement work = connection.prepareStatement("""
                        INSERT INTO exp0003_work(work_key, status) VALUES (?, 'TEST_COMMITTED')
                        ON CONFLICT (work_key) DO NOTHING
                        """)) {
                    work.setString(1, workKey);
                    inserted = work.executeUpdate();
                }
                UUID id;
                if (inserted == 1) {
                    id = UUID.nameUUIDFromBytes(("exp0003:" + workKey).getBytes(StandardCharsets.UTF_8));
                    String payload = "{\"eventId\":\"" + id + "\",\"workKey\":\"" + workKey
                            + "\",\"version\":1}";
                    try (PreparedStatement outbox = connection.prepareStatement("""
                            INSERT INTO exp0003_outbox(id, aggregatetype, aggregateid, type, payload)
                            VALUES (?, 'exp0003-work', ?, 'TestCommitted', CAST(? AS jsonb))
                            """)) {
                        outbox.setObject(1, id);
                        outbox.setString(2, workKey);
                        outbox.setString(3, payload);
                        outbox.executeUpdate();
                    }
                } else {
                    try (PreparedStatement existing = connection.prepareStatement(
                            "SELECT id FROM exp0003_outbox WHERE aggregateid = ?")) {
                        existing.setString(1, workKey);
                        try (ResultSet rows = existing.executeQuery()) {
                            if (!rows.next()) throw new SQLException("Missing outbox event for existing work " + workKey);
                            id = rows.getObject(1, UUID.class);
                        }
                    }
                }
                if (rollback) connection.rollback();
                else connection.commit();
                return id;
            } catch (SQLException | RuntimeException failure) {
                connection.rollback();
                throw failure;
            }
        }
    }

    static long count(DataSource dataSource, String table) throws SQLException {
        if (!table.matches("exp0003_(work|outbox|delivery|effect)")) throw new IllegalArgumentException(table);
        try (Connection connection = dataSource.getConnection();
                var statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("SELECT count(*) FROM " + table)) {
            rows.next();
            return rows.getLong(1);
        }
    }

    static long missingEffects(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection();
                var statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("""
                        SELECT count(*) FROM exp0003_work w
                        LEFT JOIN exp0003_effect e ON e.work_key = w.work_key
                        WHERE e.work_key IS NULL
                        """)) {
            rows.next();
            return rows.getLong(1);
        }
    }

    static long ghostEffects(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection();
                var statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("""
                        SELECT count(*) FROM exp0003_effect e
                        LEFT JOIN exp0003_work w ON w.work_key = e.work_key
                        WHERE w.work_key IS NULL
                        """)) {
            rows.next();
            return rows.getLong(1);
        }
    }

    static void applyEffect(DataSource dataSource, UUID eventId, String workKey) throws SQLException {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement("""
                        INSERT INTO exp0003_effect(event_id, work_key) VALUES (?, ?)
                        ON CONFLICT (event_id) DO NOTHING
                        """)) {
            statement.setObject(1, eventId);
            statement.setString(2, workKey);
            statement.executeUpdate();
        }
    }

    static void applyEffects(DataSource dataSource, Map<UUID, String> effects) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement("""
                    INSERT INTO exp0003_effect(event_id, work_key) VALUES (?, ?)
                    ON CONFLICT (event_id) DO NOTHING
                    """)) {
                int pending = 0;
                for (var effect : effects.entrySet()) {
                    statement.setObject(1, effect.getKey());
                    statement.setString(2, effect.getValue());
                    statement.addBatch();
                    if (++pending == 500) {
                        statement.executeBatch();
                        pending = 0;
                    }
                }
                if (pending > 0) statement.executeBatch();
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                connection.rollback();
                throw failure;
            }
        }
    }

    /** Holds row locks until Kafka ACK, then records delivery in a separate table. */
    static boolean pollOnce(DataSource dataSource, KafkaProducer<String, String> producer,
            String topic, Runnable afterAck) throws Exception {
        return pollBatch(dataSource, producer, topic, 1, afterAck) > 0;
    }

    static int pollBatch(DataSource dataSource, KafkaProducer<String, String> producer,
            String topic, int batchSize, Runnable afterAck) throws Exception {
        return pollBatch(dataSource, producer, topic, batchSize, afterAck,
                (events, selectNs, kafkaAckNs, deliveryCommitNs) -> {});
    }

    static int pollBatch(DataSource dataSource, KafkaProducer<String, String> producer,
            String topic, int batchSize, Runnable afterAck, TimingSink timing) throws Exception {
        if (batchSize < 1) throw new IllegalArgumentException("batchSize must be positive");
        long selectedFrom = System.nanoTime();
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement select = connection.prepareStatement("""
                    SELECT o.id, o.aggregateid, o.payload::text
                    FROM exp0003_outbox o
                    LEFT JOIN exp0003_delivery d ON d.outbox_id = o.id
                    WHERE d.outbox_id IS NULL
                    ORDER BY o.created_at, o.id
                    FOR UPDATE OF o SKIP LOCKED
                    LIMIT ?
                    """)) {
                select.setInt(1, batchSize);
                record OutboxRow(UUID id, String workKey, String payload) {}
                List<OutboxRow> events = new ArrayList<>();
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) {
                        events.add(new OutboxRow(rows.getObject(1, UUID.class),
                                rows.getString(2), rows.getString(3)));
                    }
                }
                if (events.isEmpty()) {
                    connection.rollback();
                    return 0;
                }
                long selectedAt = System.nanoTime();
                var acknowledgements = new ArrayList<java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata>>();
                for (OutboxRow event : events) {
                    var record = new ProducerRecord<String, String>(topic, event.workKey(), event.payload());
                    record.headers().add("id", event.id().toString().getBytes(StandardCharsets.UTF_8));
                    acknowledgements.add(producer.send(record));
                }
                for (var acknowledgement : acknowledgements) acknowledgement.get(10, TimeUnit.SECONDS);
                long acknowledgedAt = System.nanoTime();
                afterAck.run();
                try (PreparedStatement complete = connection.prepareStatement(
                        "INSERT INTO exp0003_delivery(outbox_id) VALUES (?)")) {
                    for (OutboxRow event : events) {
                        complete.setObject(1, event.id());
                        complete.addBatch();
                    }
                    complete.executeBatch();
                }
                connection.commit();
                timing.record(events.size(), selectedAt - selectedFrom,
                        acknowledgedAt - selectedAt, System.nanoTime() - acknowledgedAt);
                return events.size();
            } catch (Exception failure) {
                connection.rollback();
                throw failure;
            }
        }
    }
}
