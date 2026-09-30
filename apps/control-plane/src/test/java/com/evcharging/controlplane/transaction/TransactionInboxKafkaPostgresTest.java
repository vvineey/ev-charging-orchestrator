package com.evcharging.controlplane.transaction;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"transaction.ingress.enabled=true", "spring.kafka.listener.auto-startup=false",
                "transaction.recovery.experimental-session-confirmation-enabled=true"})
@EmbeddedKafka(partitions = 1, topics = "transaction-test-bootstrap",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        brokerProperties = "offsets.topic.num.partitions=1")
class TransactionInboxKafkaPostgresTest {
    private static final EmbeddedPostgres POSTGRES;
    static {
        try {
            POSTGRES = EmbeddedPostgres.builder().start();
        } catch (IOException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> POSTGRES.getJdbcUrl("postgres", "postgres"));
        registry.add("spring.datasource.username", () -> "postgres");
        registry.add("spring.datasource.password", () -> "postgres");
    }

    @AfterAll
    static void closePostgres() throws IOException {
        POSTGRES.close();
    }

    @Autowired EmbeddedKafkaBroker broker;
    @Autowired JdbcClient jdbc;
    @Autowired ObjectMapper mapper;
    @Autowired TransactionRecordListener listener;
    @Autowired @Qualifier("transactionKafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<String, String> factory;
    @TempDir Path directory;

    private String topic;
    private AdminClient admin;
    private KafkaProducer<String, String> producer;
    private ConcurrentMessageListenerContainer<String, String> container;
    private String activeGroupId;

    @BeforeEach
    void prepare() throws Exception {
        jdbc.sql("TRUNCATE transaction_inbox_state CASCADE").update();
        topic = "transaction-inbox-test-" + UUID.randomUUID();
        admin = AdminClient.create(Map.of("bootstrap.servers", broker.getBrokersAsString()));
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"));
    }

    @AfterEach
    void close() {
        if (container != null) container.stop();
        if (producer != null) producer.close(Duration.ofSeconds(5));
        if (admin != null) admin.close(Duration.ofSeconds(5));
    }

    @Test
    void reverseAndRedeliveryConvergeOnOneStoredSession() throws Exception {
        String transactionId = "TX-REVERSE";
        send(transactionId, 2, "Ended", 12500, false, false);
        send(transactionId, 0, "Started", 10000, true, false);
        send(transactionId, 1, "Updated", 11500, false, false);
        send(transactionId, 1, "Updated", 11500, false, false);
        start();
        waitForOffset(4);
        awaitState(transactionId, "FINALIZED");
        assertThat(eventCount(transactionId)).isEqualTo(3);
        assertThat(sessionCount(transactionId)).isEqualTo(1);
        assertThat(energy(transactionId)).isEqualByComparingTo("2500");
        assertThat(jdbc.sql("""
                SELECT evse_id FROM recovered_transaction_session
                WHERE charging_station_id = 'CS-TEST' AND transaction_id = :id
                """).param("id", transactionId).query(Integer.class).single()).isEqualTo(1);

        // A new group replays the same Kafka topic from offset zero after DB commit.
        container.stop();
        container = null;
        start();
        waitForOffset(4);
        assertThat(eventCount(transactionId)).isEqualTo(3);
        assertThat(sessionCount(transactionId)).isEqualTo(1);
    }

    @Test
    void missingSequenceHoldsUntilItArrivesAndConflictNeverAutoFinalizes() throws Exception {
        String missing = "TX-MISSING";
        send(missing, 0, "Started", 10000, true, false);
        send(missing, 2, "Ended", 12500, false, false);
        start();
        awaitState(missing, "HOLD");
        assertThat(reason(missing)).isEqualTo("MISSING_SEQUENCE");
        assertThat(sessionCount(missing)).isZero();
        send(missing, 1, "Updated", 11500, false, false);
        awaitState(missing, "FINALIZED");
        assertThat(energy(missing)).isEqualByComparingTo("2500");

        String conflict = "TX-CONFLICT";
        send(conflict, 0, "Started", 10000, true, false);
        send(conflict, 1, "Updated", 11500, false, false);
        send(conflict, 1, "Updated", 11700, false, false);
        send(conflict, 2, "Ended", 12500, false, false);
        awaitState(conflict, "HOLD_CONFLICT");
        assertThat(sessionCount(conflict)).isZero();
        assertThat(jdbc.sql("""
                SELECT count(*) FROM transaction_inbox_conflict
                WHERE charging_station_id = 'CS-TEST' AND transaction_id = :id
                """).param("id", conflict).query(Long.class).single()).isEqualTo(1);
    }

    @Test
    void actualMultipleMeterGroupsRemainPreservedButUnconfirmed() throws Exception {
        String transactionId = "TX-MULTI-METER";
        send(transactionId, 0, "Started", 10000, true, false);
        send(transactionId, 1, "Ended", 12500, false, true);
        start();
        awaitState(transactionId, "HOLD");
        assertThat(reason(transactionId)).isEqualTo("INVALID_OR_AMBIGUOUS_METER");
        assertThat(sessionCount(transactionId)).isZero();
        assertThat(jdbc.sql("""
                SELECT jsonb_array_length(source_document->'payload'->'meterValue')
                FROM transaction_inbox_event
                WHERE charging_station_id = 'CS-TEST' AND transaction_id = :id AND seq_no = 1
                """).param("id", transactionId).query(Integer.class).single()).isEqualTo(3);
    }

    @Test
    void committedDatabaseRecordReplaysAfterOffsetCommitIsInterrupted() throws Exception {
        String transactionId = "TX-COMMIT-GAP";
        send(transactionId, 0, "Started", 10000, true, false);
        send(transactionId, 1, "Ended", 12500, false, false);
        String groupId = "transaction-test-" + UUID.randomUUID();
        AtomicBoolean injected = new AtomicBoolean();
        start(groupId, record -> {
            listener.consume(record);
            if (record.offset() == 1 && injected.compareAndSet(false, true)) {
                throw new IllegalStateException("injected_after_database_commit_before_offset_commit");
            }
        });
        awaitState(transactionId, "FINALIZED");
        await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            assertThat(injected.get()).isTrue();
            assertThat(offset()).isEqualTo(1);
            assertThat(container.isRunning()).isFalse();
        });
        assertThat(sessionCount(transactionId)).isEqualTo(1);

        container.stop();
        container = null;
        start(groupId, listener::consume);
        waitForOffset(2);
        assertThat(eventCount(transactionId)).isEqualTo(2);
        assertThat(sessionCount(transactionId)).isEqualTo(1);
        assertThat(energy(transactionId)).isEqualByComparingTo("2500");
    }

    @Test
    void actualJvmHaltAfterDbCommitReplaysWithoutDuplicatingSession() throws Exception {
        String transactionId = "TX-JVM-HALT";
        String groupId = "transaction-test-" + UUID.randomUUID();
        send(transactionId, 0, "Started", 10000, true, false);
        start(groupId, listener::consume);
        waitForOffset(1);
        container.stop();
        container = null;

        send(transactionId, 1, "Ended", 12500, false, false);
        var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("transaction.test.runtime-classpath"),
                CrashAfterTransactionCommitProcess.class.getName(),
                "--spring.kafka.listener.auto-startup=false",
                "--transaction.ingress.enabled=true",
                "--transaction.recovery.experimental-session-confirmation-enabled=true",
                "--transaction.kafka-topic=" + topic,
                "--transaction.test.group-id=" + groupId,
                "--spring.kafka.consumer.properties[session.timeout.ms]=6000",
                "--spring.kafka.consumer.properties[heartbeat.interval.ms]=1000");
        builder.environment().put("SPRING_DATASOURCE_URL", POSTGRES.getJdbcUrl("postgres", "postgres"));
        builder.environment().put("SPRING_DATASOURCE_USERNAME", "postgres");
        builder.environment().put("SPRING_DATASOURCE_PASSWORD", "postgres");
        builder.environment().put("SPRING_KAFKA_BOOTSTRAPSERVERS", broker.getBrokersAsString());
        builder.redirectErrorStream(true).redirectOutput(directory.resolve("transaction-crash-process.log").toFile());
        Process process = builder.start();
        try {
            assertThat(process.waitFor(45, TimeUnit.SECONDS))
                    .as("child JVM must commit the session and halt before offset commit").isTrue();
            assertThat(process.exitValue()).isEqualTo(137);
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(10, TimeUnit.SECONDS);
            }
        }
        awaitState(transactionId, "FINALIZED");
        assertThat(sessionCount(transactionId)).isEqualTo(1);
        assertThat(offset()).isEqualTo(1);

        start(groupId, listener::consume);
        waitForOffset(2);
        assertThat(eventCount(transactionId)).isEqualTo(2);
        assertThat(sessionCount(transactionId)).isEqualTo(1);
        assertThat(energy(transactionId)).isEqualByComparingTo("2500");
    }

    private void start() {
        start("transaction-test-" + UUID.randomUUID(), listener::consume);
    }

    private void start(String groupId, MessageListener<String, String> messageListener) {
        container = factory.createContainer(topic);
        activeGroupId = groupId;
        container.getContainerProperties().setGroupId(activeGroupId);
        container.getContainerProperties().setPollTimeout(100);
        container.getContainerProperties().setMessageListener(messageListener);
        container.start();
    }

    private void waitForOffset(long expected) {
        await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> assertThat(offset()).isEqualTo(expected));
    }

    private long offset() throws Exception {
        try {
            var value = admin.listConsumerGroupOffsets(activeGroupId).partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS).get(new TopicPartition(topic, 0));
            return value == null ? 0 : value.offset();
        } catch (ExecutionException exception) {
            if (exception.getCause() instanceof GroupIdNotFoundException) return 0;
            throw exception;
        }
    }

    private void send(String transactionId, int seq, String stage, Integer wh,
                      boolean evse, boolean extraMeter) throws Exception {
        producer.send(new ProducerRecord<>(topic, 0, "ST-TEST", observed(transactionId, seq, stage, wh, evse, extraMeter)))
                .get(10, TimeUnit.SECONDS);
    }

    private String observed(String transactionId, int seq, String stage, Integer wh,
                            boolean evse, boolean extraMeter) {
        String time = seq == 0 ? "2026-01-01T10:00:00Z"
                : "Ended".equals(stage) ? "2026-01-01T10:20:00Z" : "2026-01-01T10:10:00Z";
        ObjectNode root = mapper.createObjectNode();
        root.put("eventId", UUID.randomUUID().toString());
        root.put("eventType", "OcppTransactionEventObserved");
        root.put("schemaVersion", 1);
        root.put("occurredAt", time);
        root.put("receivedAt", java.time.Instant.now().toString());
        root.put("stationId", "ST-TEST");
        root.put("chargingStationId", "CS-TEST");
        ObjectNode payload = root.putObject("payload");
        payload.put("transactionId", transactionId);
        payload.put("seqNo", seq);
        payload.put("eventType", stage);
        payload.put("triggerReason", "TEST");
        payload.putObject("transactionInfo").put("transactionId", transactionId);
        if (evse) payload.putObject("evse").put("id", 1);
        if (wh != null) {
            var meters = payload.putArray("meterValue");
            addMeter(meters.addObject(), time, stage, wh);
            if (extraMeter) {
                addMeter(meters.addObject(), time, stage, wh);
                addMeter(meters.addObject(), time, stage, wh);
            }
        }
        return mapper.writeValueAsString(root);
    }

    private void addMeter(ObjectNode meter, String time, String stage, int wh) {
        meter.put("timestamp", time);
        ObjectNode sample = meter.putArray("sampledValue").addObject();
        sample.put("value", wh);
        sample.put("context", "Started".equals(stage) ? "Transaction.Begin"
                : "Ended".equals(stage) ? "Transaction.End" : "Sample.Periodic");
        sample.put("measurand", "Energy.Active.Import.Register");
        sample.putObject("unitOfMeasure").put("unit", "Wh").put("multiplier", 0);
    }

    private void awaitState(String transactionId, String expected) {
        await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> assertThat(state(transactionId)).isEqualTo(expected));
    }

    private String state(String transactionId) {
        return jdbc.sql("SELECT status FROM transaction_inbox_state WHERE charging_station_id = 'CS-TEST' AND transaction_id = :id")
                .param("id", transactionId).query(String.class).optional().orElse(null);
    }

    private String reason(String transactionId) {
        return jdbc.sql("SELECT hold_reason FROM transaction_inbox_state WHERE charging_station_id = 'CS-TEST' AND transaction_id = :id")
                .param("id", transactionId).query(String.class).single();
    }

    private long eventCount(String transactionId) {
        return jdbc.sql("SELECT count(*) FROM transaction_inbox_event WHERE charging_station_id = 'CS-TEST' AND transaction_id = :id")
                .param("id", transactionId).query(Long.class).single();
    }

    private long sessionCount(String transactionId) {
        return jdbc.sql("SELECT count(*) FROM recovered_transaction_session WHERE charging_station_id = 'CS-TEST' AND transaction_id = :id")
                .param("id", transactionId).query(Long.class).single();
    }

    private BigDecimal energy(String transactionId) {
        return jdbc.sql("SELECT energy_wh FROM recovered_transaction_session WHERE charging_station_id = 'CS-TEST' AND transaction_id = :id")
                .param("id", transactionId).query(BigDecimal.class).single();
    }
}
