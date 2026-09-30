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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Local E5 pilot: all modes consume the same Kafka record shape and preserve source events in the
 * same PostgreSQL tables. A/B are test-only, safety-equalized adapters, not production services.
 * B uses a real 300-second wait; no logical-time value is reported as wall-clock latency.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"transaction.ingress.enabled=true", "spring.kafka.listener.auto-startup=false",
                "transaction.recovery.experimental-session-confirmation-enabled=true"})
@EmbeddedKafka(partitions = 1, topics = "transaction-performance-bootstrap",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        brokerProperties = "offsets.topic.num.partitions=1")
@EnabledIfSystemProperty(named = "transaction.test.performance-enabled", matches = "true")
class TransactionRecoveryOnlinePerformanceTest {
    private static final int TRANSACTIONS_PER_MODE = 120;
    private static final int WINDOW_SECONDS = Integer.getInteger("transaction.test.window-seconds", 300);
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
    @Autowired TransactionRecordDecoder decoder;
    @Autowired TransactionRecordListener productionListener;
    @Autowired JdbcTransactionInbox inbox;
    @Autowired TransactionRecoveryRules rules;
    @Autowired TransactionTemplate transactions;
    @Autowired @Qualifier("transactionKafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<String, String> factory;

    record TimingSample(String transactionId, String pattern, Double decisionMillis) { }

    record ModeResult(String mode, int transactions, int normal, int reverse, int finalized,
                      int wrongEnergy, int offsetLag, long elapsedMillis,
                      double producedRecordsPerSecond, double runRecordsPerSecond,
                      double observedFinalizationsPerSecond,
                      Double p50Millis, Double p95Millis, Double p99Millis,
                      Double normalP50Millis, Double normalP95Millis, Double normalP99Millis,
                      Double reverseP50Millis, Double reverseP95Millis, Double reverseP99Millis,
                      long sourceRows, long stateRows, long sessionRows, long sourceJsonBytes,
                      List<TimingSample> samples) { }

    record Report(String scope, int windowSeconds, int transactionsPerMode, String javaVersion,
                  String osName, String databaseVersion, int bPendingAtCommittedOffset,
                  List<ModeResult> results) { }

    @Test
    void sameKafkaPostgresBoundaryShowsCorrectnessLatencyAndStorageTradeoffs() throws Exception {
        List<ModeResult> results = new ArrayList<>();
        int bPendingAtCommittedOffset;
        // B runs first so its real timer can elapse while A and C are measured.
        try (CandidateAdapter b = new CandidateAdapter("B", WINDOW_SECONDS)) {
            Run bRun = start("B", b);
            try {
                results.add(measure("A", new CandidateAdapter("A", 0), TRANSACTIONS_PER_MODE / 2));
                results.add(measure("C", null, 120));
                if (WINDOW_SECONDS >= 60) {
                    assertThat(offset(bRun.admin, bRun.group, bRun.topic))
                            .isEqualTo(TRANSACTIONS_PER_MODE * 2);
                    assertThat(bRun.finalizedNanos).isEmpty();
                    bPendingAtCommittedOffset = TRANSACTIONS_PER_MODE;
                } else {
                    bPendingAtCommittedOffset = -1;
                }
                results.add(finish(bRun, b, TRANSACTIONS_PER_MODE,
                        Duration.ofSeconds(WINDOW_SECONDS + 60)));
            } finally {
                bRun.close();
            }
        }
        results.sort(Comparator.comparing(ModeResult::mode));
        assertThat(results).extracting(ModeResult::mode).containsExactly("A", "B", "C");
        assertThat(results.get(0).finalized()).isEqualTo(TRANSACTIONS_PER_MODE / 2);
        assertThat(results.get(1).finalized()).isEqualTo(TRANSACTIONS_PER_MODE);
        assertThat(results.get(2).finalized()).isEqualTo(TRANSACTIONS_PER_MODE);
        assertThat(results).allSatisfy(result -> {
            assertThat(result.wrongEnergy()).isZero();
            assertThat(result.offsetLag()).isZero();
            assertThat(result.sourceRows()).isEqualTo(TRANSACTIONS_PER_MODE * 2);
            assertThat(result.stateRows()).isEqualTo(TRANSACTIONS_PER_MODE);
            assertThat(result.sessionRows()).isEqualTo(result.finalized());
        });
        assertThat(results.get(1).p50Millis())
                .isGreaterThan(Math.max(0.0, WINDOW_SECONDS * 1000.0 - 10_000.0));
        Path output = Path.of(System.getProperty("transaction.test.performance-output"));
        Files.createDirectories(output.getParent());
        Files.writeString(output, mapper.writeValueAsString(new Report(
                "single-host-embedded-kafka-postgres-test-only-a-b-adapters",
                WINDOW_SECONDS, TRANSACTIONS_PER_MODE, System.getProperty("java.version"),
                System.getProperty("os.name"),
                jdbc.sql("SELECT version()").query(String.class).single(),
                bPendingAtCommittedOffset, results)));
    }

    private ModeResult measure(String mode, CandidateAdapter adapter, int expectedFinalized) throws Exception {
        try (CandidateAdapter closeable = adapter; Run run = start(mode, adapter)) {
            return finish(run, adapter, expectedFinalized, Duration.ofSeconds(60));
        }
    }

    private Run start(String mode, CandidateAdapter adapter) throws Exception {
        String topic = "transaction-performance-" + mode + "-" + UUID.randomUUID();
        String group = topic;
        String bootstrap = broker.getBrokersAsString();
        var admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap));
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        var producer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"));
        var container = factory.createContainer(topic);
        container.getContainerProperties().setGroupId(group);
        container.getContainerProperties().setPollTimeout(100);
        Map<String, Long> firstSentNanos = new ConcurrentHashMap<>();
        Map<String, Long> finalizedNanos = new ConcurrentHashMap<>();
        Set<UUID> lastEventIds = ConcurrentHashMap.newKeySet();
        AtomicInteger processed = new AtomicInteger();
        container.getContainerProperties().setMessageListener((MessageListener<String, String>) record -> {
            if (adapter == null) {
                productionListener.consume(record);
                UUID eventId = UUID.fromString(mapper.readTree(record.value()).get("eventId").textValue());
                if (lastEventIds.contains(eventId)) {
                    String id = mapper.readTree(record.value()).get("payload").get("transactionId").textValue();
                    finalizedNanos.put(id, System.nanoTime());
                }
            } else {
                adapter.accept(decoder.decode(record.key(), record.value()), finalizedNanos);
            }
            processed.incrementAndGet();
        });
        container.start();
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(container.getAssignedPartitions()).isNotEmpty());
        long startNanos = System.nanoTime();
        for (int index = 0; index < TRANSACTIONS_PER_MODE; index++) {
            String id = "bench-" + mode + "-" + index;
            boolean reverse = index % 2 == 1;
            int firstSeq = reverse ? 1 : 0;
            int lastSeq = reverse ? 0 : 1;
            firstSentNanos.put(id, System.nanoTime());
            producer.send(new ProducerRecord<>(topic, 0, "ST-BENCH", observed(id, firstSeq)))
                    .get(10, TimeUnit.SECONDS);
            UUID lastId = UUID.randomUUID();
            lastEventIds.add(lastId);
            producer.send(new ProducerRecord<>(topic, 0, "ST-BENCH", observed(id, lastSeq, lastId)))
                    .get(10, TimeUnit.SECONDS);
        }
        return new Run(mode, topic, group, admin, producer, container, firstSentNanos,
                finalizedNanos, processed, startNanos, System.nanoTime());
    }

    private ModeResult finish(Run run, CandidateAdapter adapter, int expectedFinalized, Duration timeout)
            throws Exception {
        await().atMost(timeout).untilAsserted(() -> {
            assertThat(run.processed.get()).isEqualTo(TRANSACTIONS_PER_MODE * 2);
            assertThat(run.finalizedNanos.size()).isEqualTo(expectedFinalized);
            assertThat(offset(run.admin, run.group, run.topic)).isEqualTo(TRANSACTIONS_PER_MODE * 2);
        });
        long endNanos = System.nanoTime();
        long committed = offset(run.admin, run.group, run.topic);
        int lag = (int) (TRANSACTIONS_PER_MODE * 2 - committed);
        int finalized = jdbc.sql("""
                SELECT count(*) FROM recovered_transaction_session
                WHERE transaction_id LIKE :prefix
                """).param("prefix", "bench-" + run.mode + "-%").query(Long.class).single().intValue();
        int wrongEnergy = jdbc.sql("""
                SELECT count(*) FROM recovered_transaction_session
                WHERE transaction_id LIKE :prefix AND energy_wh <> 2500
                """).param("prefix", "bench-" + run.mode + "-%").query(Long.class).single().intValue();
        Set<String> actualSessions = new HashSet<>(jdbc.sql("""
                SELECT transaction_id FROM recovered_transaction_session
                WHERE transaction_id LIKE :prefix
                """).param("prefix", "bench-" + run.mode + "-%").query(String.class).list());
        Set<String> expectedSessions = new HashSet<>();
        for (int index = 0; index < TRANSACTIONS_PER_MODE; index++) {
            if (!"A".equals(run.mode) || index % 2 == 0) {
                expectedSessions.add("bench-" + run.mode + "-" + index);
            }
        }
        assertThat(actualSessions).isEqualTo(expectedSessions);
        List<TimingSample> samples = new ArrayList<>();
        for (int index = 0; index < TRANSACTIONS_PER_MODE; index++) {
            String id = "bench-" + run.mode + "-" + index;
            Long finalAt = run.finalizedNanos.get(id);
            samples.add(new TimingSample(id, index % 2 == 0 ? "NORMAL" : "REVERSE",
                    finalAt == null ? null : (finalAt - run.firstSentNanos.get(id)) / 1_000_000.0));
        }
        List<Double> milliseconds = samples.stream().map(TimingSample::decisionMillis)
                .filter(java.util.Objects::nonNull).sorted().toList();
        List<Double> normalMillis = samples.stream().filter(sample -> "NORMAL".equals(sample.pattern()))
                .map(TimingSample::decisionMillis).filter(java.util.Objects::nonNull).sorted().toList();
        List<Double> reverseMillis = samples.stream().filter(sample -> "REVERSE".equals(sample.pattern()))
                .map(TimingSample::decisionMillis).filter(java.util.Objects::nonNull).sorted().toList();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(endNanos - run.startNanos);
        return new ModeResult(run.mode, TRANSACTIONS_PER_MODE, TRANSACTIONS_PER_MODE / 2,
                TRANSACTIONS_PER_MODE / 2, finalized, wrongEnergy, lag, elapsedMillis,
                (TRANSACTIONS_PER_MODE * 2) / ((run.lastSendNanos - run.startNanos) / 1_000_000_000.0),
                (TRANSACTIONS_PER_MODE * 2) / ((endNanos - run.startNanos) / 1_000_000_000.0),
                finalized / ((endNanos - run.startNanos) / 1_000_000_000.0),
                percentile(milliseconds, .50), percentile(milliseconds, .95), percentile(milliseconds, .99),
                percentile(normalMillis, .50), percentile(normalMillis, .95), percentile(normalMillis, .99),
                percentile(reverseMillis, .50), percentile(reverseMillis, .95), percentile(reverseMillis, .99),
                count("transaction_inbox_event", run.mode), count("transaction_inbox_state", run.mode),
                count("recovered_transaction_session", run.mode),
                jdbc.sql("""
                        SELECT coalesce(sum(pg_column_size(source_document)), 0)
                        FROM transaction_inbox_event WHERE transaction_id LIKE :prefix
                        """).param("prefix", "bench-" + run.mode + "-%").query(Long.class).single(),
                samples);
    }

    private long count(String table, String mode) {
        return jdbc.sql("SELECT count(*) FROM " + table + " WHERE transaction_id LIKE :prefix")
                .param("prefix", "bench-" + mode + "-%").query(Long.class).single();
    }

    private Double percentile(List<Double> sorted, double fraction) {
        if (sorted.isEmpty()) return null;
        return sorted.get((int) Math.ceil(fraction * sorted.size()) - 1);
    }

    private long offset(AdminClient admin, String group, String topic) throws Exception {
        try {
            var found = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS).get(new TopicPartition(topic, 0));
            return found == null ? 0 : found.offset();
        } catch (java.util.concurrent.ExecutionException exception) {
            if (exception.getCause() instanceof GroupIdNotFoundException) return 0;
            throw exception;
        }
    }

    private String observed(String id, int seq) {
        return observed(id, seq, UUID.randomUUID());
    }

    private String observed(String id, int seq, UUID eventId) {
        boolean start = seq == 0;
        String at = start ? "2026-01-01T10:00:00Z" : "2026-01-01T10:20:00Z";
        ObjectNode root = mapper.createObjectNode();
        root.put("eventId", eventId.toString());
        root.put("eventType", "OcppTransactionEventObserved");
        root.put("schemaVersion", 1);
        root.put("occurredAt", at);
        root.put("receivedAt", Instant.now().toString());
        root.put("stationId", "ST-BENCH");
        root.put("chargingStationId", "CS-BENCH");
        ObjectNode payload = root.putObject("payload");
        payload.put("transactionId", id);
        payload.put("seqNo", seq);
        payload.put("eventType", start ? "Started" : "Ended");
        payload.put("triggerReason", "TEST");
        payload.putObject("transactionInfo").put("transactionId", id);
        if (start) payload.putObject("evse").put("id", 1);
        ObjectNode meter = payload.putArray("meterValue").addObject();
        meter.put("timestamp", at);
        ObjectNode sample = meter.putArray("sampledValue").addObject();
        sample.put("value", start ? 10_000 : 12_500);
        sample.put("context", start ? "Transaction.Begin" : "Transaction.End");
        sample.put("measurand", "Energy.Active.Import.Register");
        sample.putObject("unitOfMeasure").put("unit", "Wh").put("multiplier", 0);
        return mapper.writeValueAsString(root);
    }

    private final class CandidateAdapter implements AutoCloseable {
        private final String mode;
        private final int windowSeconds;
        private final Map<String, Boolean> started = new ConcurrentHashMap<>();
        private final Map<String, List<ObservedTransactionRecord>> windows = new ConcurrentHashMap<>();
        private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        CandidateAdapter(String mode, int windowSeconds) {
            this.mode = mode;
            this.windowSeconds = windowSeconds;
        }

        void accept(ObservedTransactionRecord event, Map<String, Long> finalizedNanos) {
            boolean finalized = Boolean.TRUE.equals(transactions.execute(status -> {
                JdbcTransactionInbox.State state = inbox.ensureAndLock(event);
                if (!state.stationId().equals(event.stationId())) return false;
                if (!inbox.insertEvent(event)) {
                    if (!inbox.sameSource(event)) {
                        inbox.preserveConflict(event);
                        inbox.updateState(event, "HOLD_CONFLICT", "SOURCE_CONFLICT", null);
                    }
                    return false;
                }
                if ("A".equals(mode)) return applyArrival(event);
                return false;
            }));
            if (finalized) finalizedNanos.put(event.transactionId(), System.nanoTime());
            if ("B".equals(mode)) {
                windows.compute(event.transactionId(), (id, existing) -> {
                    if (existing == null) {
                        List<ObservedTransactionRecord> batch = new ArrayList<>();
                        batch.add(event);
                        scheduler.schedule(() -> flush(id, batch, finalizedNanos), windowSeconds,
                                TimeUnit.SECONDS);
                        return batch;
                    }
                    synchronized (existing) { existing.add(event); }
                    return existing;
                });
            }
        }

        private boolean applyArrival(ObservedTransactionRecord event) {
            if ("Started".equals(event.eventType())) {
                started.put(event.transactionId(), true);
                inbox.updateState(event, "OPEN", null, 1);
                return false;
            }
            if (!"Ended".equals(event.eventType()) || !Boolean.TRUE.equals(started.get(event.transactionId()))) {
                return false;
            }
            TransactionRecoveryRules.Outcome outcome = rules.evaluate(inbox.events(event));
            if (!"FINALIZED".equals(outcome.status())) {
                inbox.updateState(event, "HOLD", outcome.reason(), null);
                return false;
            }
            inbox.insertSession(event, outcome);
            inbox.updateState(event, "FINALIZED", null, outcome.evseId());
            return true;
        }

        private void flush(String id, List<ObservedTransactionRecord> batch,
                           Map<String, Long> finalizedNanos) {
            List<ObservedTransactionRecord> copy;
            synchronized (batch) { copy = new ArrayList<>(batch); }
            windows.remove(id, batch);
            copy.sort(Comparator.comparing(ObservedTransactionRecord::occurredAt));
            boolean sawStart = false;
            for (ObservedTransactionRecord event : copy) {
                if ("Started".equals(event.eventType())) sawStart = true;
                if ("Ended".equals(event.eventType()) && sawStart) {
                    boolean finalized = Boolean.TRUE.equals(transactions.execute(status -> {
                        inbox.ensureAndLock(event);
                        TransactionRecoveryRules.Outcome outcome = rules.evaluate(inbox.events(event));
                        if (!"FINALIZED".equals(outcome.status())) {
                            inbox.updateState(event, "HOLD", outcome.reason(), null);
                            return false;
                        }
                        inbox.insertSession(event, outcome);
                        inbox.updateState(event, "FINALIZED", null, outcome.evseId());
                        return true;
                    }));
                    if (finalized) finalizedNanos.put(id, System.nanoTime());
                }
            }
        }

        @Override public void close() {
            scheduler.shutdownNow();
        }
    }

    private record Run(String mode, String topic, String group, AdminClient admin,
                       KafkaProducer<String, String> producer,
                       org.springframework.kafka.listener.ConcurrentMessageListenerContainer<String, String> container,
                       Map<String, Long> firstSentNanos, Map<String, Long> finalizedNanos,
                       AtomicInteger processed, long startNanos, long lastSendNanos) implements AutoCloseable {
        @Override public void close() {
            container.stop();
            producer.close();
            admin.close();
        }
    }
}
