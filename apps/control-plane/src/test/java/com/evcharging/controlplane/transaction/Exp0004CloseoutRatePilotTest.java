package com.evcharging.controlplane.transaction;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
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
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Opt-in, test-only OCPP-derived closeout offered-rate pilot; never changes BILLABLE. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"transaction.ingress.enabled=true", "spring.kafka.listener.auto-startup=false",
                "transaction.recovery.experimental-candidate-calculation-enabled=true"})
@EmbeddedKafka(partitions = 1, topics = "transaction-rate-bootstrap",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        brokerProperties = "offsets.topic.num.partitions=1")
class Exp0004CloseoutRatePilotTest {
    private static final int RATE = 200;
    private static final int SECONDS = 5;
    private static final int EXPECTED = RATE * SECONDS;
    private static final BigDecimal WH = new BigDecimal("2500");
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

    @Test
    void measuresOcppDerivedCloseoutCommitToKafkaObservation() throws Exception {
        assumeTrue("true".equalsIgnoreCase(System.getenv("EVC_EXP0004_RATE_PILOT")));
        String run = UUID.randomUUID().toString();
        String prefix = "TX-RATE-" + run + "-";
        String inputTopic = "exp0004-rate-input-" + run;
        String outputTopic = "exp0004-rate-output-" + run;
        String brokers = broker.getBrokersAsString();
        createTables();
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", brokers));
             var input = producer(brokers);
             var output = producer(brokers)) {
            admin.createTopics(List.of(new NewTopic(inputTopic, 1, (short) 1),
                    new NewTopic(outputTopic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            var inbox = factory.createContainer(inputTopic);
            inbox.getContainerProperties().setGroupId("exp0004-rate-inbox-" + run);
            inbox.getContainerProperties().setPollTimeout(100);
            inbox.getContainerProperties().setMessageListener(
                    (MessageListener<String, String>) listener::consume);
            inbox.start();
            try {
                for (int i = 0; i < EXPECTED; i++) {
                    String transactionId = prefix + i;
                    send(input, inputTopic, transactionId, 0, "Started", 10000);
                    send(input, inputTopic, transactionId, 1, "Ended", 12500);
                }
                await().atMost(Duration.ofSeconds(120)).untilAsserted(() -> {
                    assertThat(candidateCount(prefix)).isEqualTo(EXPECTED);
                    assertThat(validCandidateCount(prefix)).isEqualTo(EXPECTED);
                });
                runRatePilot(run, prefix, outputTopic, brokers, output);
            } finally {
                inbox.stop();
            }
        }
    }

    private void runRatePilot(String run, String prefix, String topic, String brokers,
                              KafkaProducer<String, String> producer) throws Exception {
        var config = new HikariConfig();
        config.setJdbcUrl(POSTGRES.getJdbcUrl("postgres", "postgres"));
        config.setUsername("postgres");
        config.setPassword("postgres");
        config.setMaximumPoolSize(16);
        config.setMinimumIdle(8);
        config.setPoolName("exp0004-rate-writers");
        try (var pool = new HikariDataSource(config);
             var executor = Executors.newFixedThreadPool(3);
             var writers = Executors.newFixedThreadPool(8)) {
            var committedAt = new ConcurrentHashMap<UUID, Long>();
            var observedAt = new ConcurrentHashMap<UUID, Long>();
            var ackedAt = new ConcurrentHashMap<UUID, Long>();
            var markedAt = new ConcurrentHashMap<UUID, Long>();
            var sendAckDurations = Collections.synchronizedList(new ArrayList<Double>());
            var markDurations = Collections.synchronizedList(new ArrayList<Double>());
            Set<String> observedTransactions = ConcurrentHashMap.newKeySet();
            var committedCount = new AtomicInteger();
            var observedCount = new AtomicInteger();
            var physicalCount = new AtomicInteger();
            var peakBacklog = new AtomicInteger();
            var stopSampler = new AtomicBoolean();
            var samples = Collections.synchronizedList(new ArrayList<Map<String, Object>>());
            var consumerReady = new CountDownLatch(1);
            var startSignal = new CountDownLatch(1);

            Future<?> observer = executor.submit(() -> {
                var partition = new TopicPartition(topic, 0);
                try (var consumer = new KafkaConsumer<String, String>(Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                        ConsumerConfig.GROUP_ID_CONFIG, "exp0004-rate-reader-" + run,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"))) {
                    consumer.assign(List.of(partition));
                    consumer.seekToBeginning(List.of(partition));
                    consumer.poll(Duration.ofMillis(1));
                    consumerReady.countDown();
                    startSignal.await();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SECONDS + 60L);
                    while (observedAt.size() < EXPECTED && System.nanoTime() < deadline) {
                        for (var record : consumer.poll(Duration.ofMillis(100))) {
                            long now = System.nanoTime();
                            var body = mapper.readTree(record.value());
                            String transactionId = body.path("payload").path("transactionId").asText();
                            UUID eventId = UUID.fromString(body.path("eventId").asText());
                            assertThat(transactionId).startsWith(prefix);
                            assertThat(record.key()).isEqualTo("ST-TEST");
                            assertThat(body.path("eventType").asText())
                                    .isEqualTo("TransactionCloseoutSimulated");
                            assertThat(body.path("payload").path("energyWh").asInt()).isEqualTo(2500);
                            physicalCount.incrementAndGet();
                            if (observedAt.putIfAbsent(eventId, now) == null) {
                                observedTransactions.add(transactionId);
                                observedCount.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception exception) {
                    throw new IllegalStateException("Kafka observation failed", exception);
                }
            });
            assertThat(consumerReady.await(20, TimeUnit.SECONDS)).isTrue();

            Future<Integer> relay = executor.submit(() -> {
                int published = 0;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SECONDS + 60L);
                startSignal.await();
                while (published < EXPECTED && System.nanoTime() < deadline) {
                    int count = pollBatch(pool, producer, topic, prefix,
                            ackedAt, markedAt, sendAckDurations, markDurations);
                    published += count;
                    if (count == 0) LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
                }
                return published;
            });

            long startedNs = System.nanoTime();
            long plannedEndNs = startedNs + TimeUnit.SECONDS.toNanos(SECONDS);
            Future<?> sampler = executor.submit(() -> {
                while (!stopSampler.get()) {
                    int backlog = Math.max(0, committedCount.get() - observedCount.get());
                    peakBacklog.accumulateAndGet(backlog, Math::max);
                    var sample = new LinkedHashMap<String, Object>();
                    sample.put("elapsedMs", (System.nanoTime() - startedNs) / 1_000_000.0);
                    sample.put("committed", committedCount.get());
                    sample.put("observedUnique", observedCount.get());
                    sample.put("observedBacklog", backlog);
                    samples.add(sample);
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                }
            });
            startSignal.countDown();

            var writerTasks = new ArrayList<Future<?>>(EXPECTED);
            for (int i = 0; i < EXPECTED; i++) {
                long scheduledNs = startedNs + i * 1_000_000_000L / RATE;
                while (System.nanoTime() < scheduledNs) {
                    LockSupport.parkNanos(Math.min(scheduledNs - System.nanoTime(),
                            TimeUnit.MILLISECONDS.toNanos(1)));
                }
                String transactionId = prefix + i;
                writerTasks.add(writers.submit(() -> {
                    UUID eventId = commitCloseout(pool, transactionId);
                    committedAt.put(eventId, System.nanoTime());
                    committedCount.incrementAndGet();
                    return null;
                }));
            }
            for (var task : writerTasks) task.get(120, TimeUnit.SECONDS);
            while (System.nanoTime() < plannedEndNs) {
                LockSupport.parkNanos(Math.min(plannedEndNs - System.nanoTime(),
                        TimeUnit.MILLISECONDS.toNanos(1)));
            }
            int relayPublished = relay.get(70, TimeUnit.SECONDS);
            observer.get(70, TimeUnit.SECONDS);
            stopSampler.set(true);
            sampler.get(5, TimeUnit.SECONDS);

            long committedBeforeEnd = committedAt.values().stream()
                    .filter(value -> value <= plannedEndNs).count();
            long observedBeforeEnd = observedAt.values().stream()
                    .filter(value -> value <= plannedEndNs).count();
            long ackedBeforeEnd = ackedAt.values().stream()
                    .filter(value -> value <= plannedEndNs).count();
            long markedBeforeEnd = markedAt.values().stream()
                    .filter(value -> value <= plannedEndNs).count();
            long lastObservedNs = observedAt.values().stream().mapToLong(Long::longValue)
                    .max().orElse(plannedEndNs);
            var latencies = new ArrayList<Double>(EXPECTED);
            int negativeLatencies = 0;
            for (var entry : committedAt.entrySet()) {
                Long observedNs = observedAt.get(entry.getKey());
                if (observedNs != null) {
                    long delta = observedNs - entry.getValue();
                    if (delta < 0) negativeLatencies++;
                    latencies.add(delta / 1_000_000.0);
                }
            }
            Collections.sort(latencies);
            Collections.sort(sendAckDurations);
            Collections.sort(markDurations);
            long closeouts = count("exp0004_closeout", prefix);
            long outbox = count("exp0004_outbox", prefix);
            long pending = jdbc.sql("""
                    SELECT count(*) FROM exp0004_outbox
                    WHERE transaction_id LIKE :prefix AND published_at IS NULL
                    """).param("prefix", prefix + "%").query(Long.class).single();
            var result = mapper.createObjectNode();
            result.put("issue", 60);
            result.put("mode", "OCPP-derived test closeout + single batch-16 Polling Relay");
            result.put("ratePerSecond", RATE);
            result.put("seconds", SECONDS);
            result.put("planned", EXPECTED);
            result.put("candidateValid", validCandidateCount(prefix));
            result.put("committedBeforePlannedEnd", committedBeforeEnd);
            result.put("observedBeforePlannedEnd", observedBeforeEnd);
            result.put("backlogAtPlannedEnd", committedBeforeEnd - observedBeforeEnd);
            result.put("ackedBeforePlannedEnd", ackedBeforeEnd);
            result.put("markedBeforePlannedEnd", markedBeforeEnd);
            result.put("committedFinal", committedAt.size());
            result.put("relayPublished", relayPublished);
            result.put("closeouts", closeouts);
            result.put("outbox", outbox);
            result.put("pendingOutbox", pending);
            result.put("kafkaPhysical", physicalCount.get());
            result.put("kafkaUnique", observedAt.size());
            result.put("kafkaUniqueTransactions", observedTransactions.size());
            result.put("negativeLatencies", negativeLatencies);
            result.put("peakObservedBacklog", peakBacklog.get());
            result.put("endObservedBacklog", Math.max(0, committedCount.get() - observedCount.get()));
            result.put("drainAfterPlannedEndMs", Math.max(0, lastObservedNs - plannedEndNs) / 1_000_000.0);
            result.put("p50CommitToObservedMs", percentile(latencies, 0.50));
            result.put("p95CommitToObservedMs", percentile(latencies, 0.95));
            result.put("p99CommitToObservedMs", percentile(latencies, 0.99));
            result.put("p50PerRowSendAckMs", percentile(sendAckDurations, 0.50));
            result.put("p95PerRowSendAckMs", percentile(sendAckDurations, 0.95));
            result.put("p50PerRowDbMarkMs", percentile(markDurations, 0.50));
            result.put("p95PerRowDbMarkMs", percentile(markDurations, 0.95));
            result.put("javaVersion", System.getProperty("java.version"));
            result.put("postgresVersion", jdbc.sql("SHOW server_version").query(String.class).single());
            result.put("kafkaPartitions", 1);
            result.put("kafkaReplicas", 1);
            result.put("writerThreads", 8);
            result.put("poolConnectionsMax", 16);
            result.put("relayBatch", 16);
            result.set("samples", mapper.valueToTree(samples));
            Path output = Path.of("build", "experiment-results", "exp-0004-rate-pilot.json");
            Files.createDirectories(output.getParent());
            Files.writeString(output, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
            result.remove("samples");
            System.out.println("EXP0004_RATE_RESULT " + mapper.writeValueAsString(result));

            assertThat(committedBeforeEnd).as("offered-load generator validity").isGreaterThanOrEqualTo(950);
            assertThat(committedAt).hasSize(EXPECTED);
            assertThat(closeouts).isEqualTo(EXPECTED);
            assertThat(outbox).isEqualTo(EXPECTED);
            assertThat(pending).isZero();
            assertThat(relayPublished).isEqualTo(EXPECTED);
            assertThat(ackedAt).hasSize(EXPECTED);
            assertThat(markedAt).hasSize(EXPECTED);
            assertThat(physicalCount.get()).isEqualTo(EXPECTED);
            assertThat(observedAt).hasSize(EXPECTED);
            assertThat(observedTransactions).hasSize(EXPECTED);
            for (int i = 0; i < EXPECTED; i++) {
                assertThat(observedTransactions).contains(prefix + i);
            }
            assertThat(negativeLatencies).isZero();
        }
    }

    private UUID commitCloseout(HikariDataSource pool, String transactionId) throws Exception {
        try (Connection connection = pool.getConnection()) {
            connection.setAutoCommit(false);
            try {
                UUID eventId = Exp0004DirectPublishProcess.insertTestCloseout(
                        connection, transactionId, WH);
                try (var statement = connection.prepareStatement("""
                        INSERT INTO exp0004_outbox (event_id, transaction_id, station_id, payload)
                        VALUES (?, ?, 'ST-TEST', ?)
                        """)) {
                    statement.setObject(1, eventId);
                    statement.setString(2, transactionId);
                    statement.setString(3, Exp0004DirectPublishProcess.eventPayload(
                            eventId, transactionId, "2500"));
                    statement.executeUpdate();
                }
                connection.commit();
                return eventId;
            } catch (Exception failure) {
                connection.rollback();
                throw failure;
            }
        }
    }

    private int pollBatch(HikariDataSource pool, KafkaProducer<String, String> producer,
                          String topic, String prefix,
                          ConcurrentHashMap<UUID, Long> ackedAt,
                          ConcurrentHashMap<UUID, Long> markedAt,
                          List<Double> sendAckDurations, List<Double> markDurations) throws Exception {
        var rows = new ArrayList<OutboxRow>();
        try (var connection = pool.getConnection();
             var statement = connection.prepareStatement("""
                     SELECT event_id, station_id, payload FROM exp0004_outbox
                     WHERE transaction_id LIKE ? AND published_at IS NULL
                     ORDER BY transaction_id LIMIT 16
                     """)) {
            statement.setString(1, prefix + "%");
            try (var result = statement.executeQuery()) {
                while (result.next()) rows.add(new OutboxRow(
                        result.getObject("event_id", UUID.class),
                        result.getString("station_id"), result.getString("payload")));
            }
        }
        for (var row : rows) {
            long sendStartedNs = System.nanoTime();
            producer.send(new ProducerRecord<>(topic, row.stationId(), row.payload()))
                    .get(10, TimeUnit.SECONDS);
            long ackNs = System.nanoTime();
            ackedAt.put(row.eventId(), ackNs);
            sendAckDurations.add((ackNs - sendStartedNs) / 1_000_000.0);
            long markStartedNs = System.nanoTime();
            try (var connection = pool.getConnection();
                 var statement = connection.prepareStatement("""
                         UPDATE exp0004_outbox SET published_at = now()
                         WHERE event_id = ? AND published_at IS NULL
                         """)) {
                statement.setObject(1, row.eventId());
                assertThat(statement.executeUpdate()).isEqualTo(1);
            }
            long markedNs = System.nanoTime();
            markedAt.put(row.eventId(), markedNs);
            markDurations.add((markedNs - markStartedNs) / 1_000_000.0);
        }
        return rows.size();
    }

    private record OutboxRow(UUID eventId, String stationId, String payload) {}

    private void createTables() {
        jdbc.sql("""
                CREATE TABLE IF NOT EXISTS exp0004_closeout (
                    charging_station_id TEXT NOT NULL, transaction_id TEXT NOT NULL,
                    station_id TEXT NOT NULL, energy_wh NUMERIC(24,6) NOT NULL,
                    event_id UUID NOT NULL UNIQUE,
                    PRIMARY KEY (charging_station_id, transaction_id))
                """).update();
        jdbc.sql("""
                CREATE TABLE IF NOT EXISTS exp0004_outbox (
                    event_id UUID PRIMARY KEY, transaction_id TEXT NOT NULL UNIQUE,
                    station_id TEXT NOT NULL, payload TEXT NOT NULL,
                    published_at TIMESTAMPTZ)
                """).update();
    }

    private long candidateCount(String prefix) {
        return jdbc.sql("""
                SELECT count(*) FROM transaction_session_candidate
                WHERE charging_station_id = 'CS-TEST' AND transaction_id LIKE :prefix
                """).param("prefix", prefix + "%").query(Long.class).single();
    }

    private long validCandidateCount(String prefix) {
        return jdbc.sql("""
                SELECT count(*) FROM transaction_session_candidate c
                JOIN transaction_inbox_state s USING (charging_station_id, transaction_id)
                WHERE c.charging_station_id = 'CS-TEST' AND c.transaction_id LIKE :prefix
                  AND c.candidate_status = 'PROVISIONAL' AND s.status = 'PROVISIONAL'
                  AND c.energy_wh = 2500
                """).param("prefix", prefix + "%").query(Long.class).single();
    }

    private long count(String table, String prefix) {
        return jdbc.sql("SELECT count(*) FROM " + table + " WHERE transaction_id LIKE :prefix")
                .param("prefix", prefix + "%").query(Long.class).single();
    }

    private static double percentile(List<Double> sorted, double percentile) {
        if (sorted.isEmpty()) return Double.NaN;
        int index = Math.max(0, (int) Math.ceil(sorted.size() * percentile) - 1);
        return sorted.get(index);
    }

    private static KafkaProducer<String, String> producer(String brokers) {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"));
    }

    private void send(KafkaProducer<String, String> producer, String topic, String transactionId,
                      int seq, String stage, int wh) throws Exception {
        String time = seq == 0 ? "2026-01-01T10:00:00Z" : "2026-01-01T10:20:00Z";
        ObjectNode record = mapper.createObjectNode();
        record.put("eventId", UUID.randomUUID().toString());
        record.put("eventType", "OcppTransactionEventObserved");
        record.put("schemaVersion", 1);
        record.put("occurredAt", time);
        record.put("receivedAt", Instant.now().toString());
        record.put("stationId", "ST-TEST");
        record.put("chargingStationId", "CS-TEST");
        ObjectNode payload = record.putObject("payload");
        payload.put("transactionId", transactionId);
        payload.put("seqNo", seq);
        payload.put("eventType", stage);
        payload.put("triggerReason", "TEST");
        payload.putObject("transactionInfo").put("transactionId", transactionId);
        if (seq == 0) payload.putObject("evse").put("id", 1);
        ObjectNode meter = payload.putArray("meterValue").addObject();
        meter.put("timestamp", time);
        ObjectNode sample = meter.putArray("sampledValue").addObject();
        sample.put("value", wh);
        sample.put("context", seq == 0 ? "Transaction.Begin" : "Transaction.End");
        sample.put("measurand", "Energy.Active.Import.Register");
        sample.putObject("unitOfMeasure").put("unit", "Wh").put("multiplier", 0);
        producer.send(new ProducerRecord<>(topic, "ST-TEST", mapper.writeValueAsString(record)))
                .get(10, TimeUnit.SECONDS);
    }
}
