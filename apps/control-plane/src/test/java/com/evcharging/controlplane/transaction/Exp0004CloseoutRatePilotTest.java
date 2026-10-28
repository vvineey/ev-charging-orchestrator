package com.evcharging.controlplane.transaction;

import com.sun.management.OperatingSystemMXBean;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.math.BigDecimal;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
    private static final boolean EXTERNAL = "true".equalsIgnoreCase(System.getenv("EVC_EXP0004_EXTERNAL"));
    private static final String EXTERNAL_BROKERS = "127.0.0.1:19093";
    private static final String EXTERNAL_TOPIC = "exp0004.rate.outbox";
    private static final int RATE = Integer.parseInt(
            System.getenv().getOrDefault("EVC_EXP0004_RATE_PER_SECOND", "200"));
    private static final int SECONDS = Integer.parseInt(
            System.getenv().getOrDefault("EVC_EXP0004_SECONDS", "5"));
    private static final int EXPECTED = Math.multiplyExact(RATE, SECONDS);
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
        registry.add("spring.datasource.url", () -> EXTERNAL
                ? "jdbc:postgresql://127.0.0.1:15439/exp0003"
                : POSTGRES.getJdbcUrl("postgres", "postgres"));
        registry.add("spring.datasource.username", () -> "postgres");
        registry.add("spring.datasource.password", () -> EXTERNAL ? "exp0003_only" : "postgres");
        if (EXTERNAL) registry.add("spring.kafka.bootstrap-servers", () -> EXTERNAL_BROKERS);
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
        if (RATE < 1 || SECONDS < 1 || EXPECTED > 20_000) {
            throw new IllegalArgumentException("Rate and seconds must produce 1..20000 closeouts");
        }
        Path progress = Path.of("build", "experiment-results", "exp-0004-prep-progress.txt");
        Files.createDirectories(progress.getParent());
        Files.writeString(progress, "STARTED " + Instant.now() + " planned=" + EXPECTED + "\n");
        String relayMode = System.getenv().getOrDefault("EVC_EXP0004_RELAY_MODE", "SEQUENTIAL");
        if (!relayMode.equals("SEQUENTIAL") && !relayMode.equals("BATCH_ACK")
                && !(EXTERNAL && relayMode.equals("CDC"))) {
            throw new IllegalArgumentException("Unsupported relay mode: " + relayMode);
        }
        String run = UUID.randomUUID().toString();
        String prefix = "TX-RATE-" + run + "-";
        String inputTopic = "exp0004-rate-input-" + run;
        String outputTopic = EXTERNAL ? EXTERNAL_TOPIC : "exp0004-rate-output-" + run;
        String brokers = EXTERNAL ? EXTERNAL_BROKERS : broker.getBrokersAsString();
        createTables();
        if (EXTERNAL && relayMode.equals("CDC")) registerExternalConnector();
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
                    if ((i + 1) % 5000 == 0 || i + 1 == EXPECTED) {
                        System.out.println("EXP0004_PREP_SENT " + (i + 1));
                        Files.writeString(progress, "PREP_SENT " + Instant.now()
                                + " transactions=" + (i + 1) + "\n");
                    }
                }
                await().atMost(Duration.ofSeconds(120)).untilAsserted(() -> {
                    assertThat(candidateCount(prefix)).isEqualTo(EXPECTED);
                    assertThat(validCandidateCount(prefix)).isEqualTo(EXPECTED);
                });
                System.out.println("EXP0004_PREP_CANDIDATES_READY " + EXPECTED);
                Files.writeString(progress, "CANDIDATES_READY " + Instant.now()
                        + " transactions=" + EXPECTED + "\n");
                runRatePilot(run, prefix, outputTopic, brokers, output, relayMode);
            } finally {
                inbox.stop();
            }
        }
    }

    @Test
    void batchAckFailureBeforeMarkReplaysWithTheSameEventIds() throws Exception {
        assumeTrue("true".equalsIgnoreCase(System.getenv("EVC_EXP0004_BATCH_FAILURE_TEST")));
        String run = UUID.randomUUID().toString();
        String prefix = "TX-BATCH-FAIL-" + run + "-";
        String inputTopic = "exp0004-batch-fail-input-" + run;
        String outputTopic = "exp0004-batch-fail-output-" + run;
        String brokers = broker.getBrokersAsString();
        createTables();
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", brokers));
             var input = producer(brokers);
             var output = producer(brokers)) {
            admin.createTopics(List.of(new NewTopic(inputTopic, 1, (short) 1),
                    new NewTopic(outputTopic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            var inbox = factory.createContainer(inputTopic);
            inbox.getContainerProperties().setGroupId("exp0004-batch-fail-inbox-" + run);
            inbox.getContainerProperties().setPollTimeout(100);
            inbox.getContainerProperties().setMessageListener(
                    (MessageListener<String, String>) listener::consume);
            inbox.start();
            try {
                for (int i = 0; i < 16; i++) {
                    send(input, inputTopic, prefix + i, 0, "Started", 10000);
                    send(input, inputTopic, prefix + i, 1, "Ended", 12500);
                }
                await().atMost(Duration.ofSeconds(25)).untilAsserted(() ->
                        assertThat(validCandidateCount(prefix)).isEqualTo(16));
                var config = new HikariConfig();
                config.setJdbcUrl(POSTGRES.getJdbcUrl("postgres", "postgres"));
                config.setUsername("postgres");
                config.setPassword("postgres");
                config.setMaximumPoolSize(16);
                try (var pool = new HikariDataSource(config)) {
                    for (int i = 0; i < 16; i++) commitCloseout(pool, prefix + i);
                    var acked = new ConcurrentHashMap<UUID, Long>();
                    var marked = new ConcurrentHashMap<UUID, Long>();
                    var ackDurations = Collections.synchronizedList(new ArrayList<Double>());
                    var markDurations = Collections.synchronizedList(new ArrayList<Double>());
                    assertThatThrownBy(() -> pollBatch(pool, output, outputTopic, prefix,
                            "BATCH_ACK", true, acked, marked, ackDurations, markDurations))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("ACKED_BEFORE_MARK");
                    assertThat(acked).hasSize(16);
                    assertThat(marked).isEmpty();
                    assertThat(pendingCount(prefix)).isEqualTo(16);
                    var first = readOutputEventIds(outputTopic, 16);
                    assertThat(first).hasSize(16);
                    assertThat(first.stream().distinct().count()).isEqualTo(16);

                    assertThat(pollBatch(pool, output, outputTopic, prefix,
                            "BATCH_ACK", false, acked, marked, ackDurations, markDurations))
                            .isEqualTo(16);
                    var replayed = readOutputEventIds(outputTopic, 32);
                    assertThat(replayed).hasSize(32);
                    assertThat(replayed.subList(16, 32)).containsExactlyInAnyOrderElementsOf(first);
                    assertThat(replayed.stream().distinct().count()).isEqualTo(16);
                    assertThat(count("exp0004_closeout", prefix)).isEqualTo(16);
                    assertThat(count("exp0004_outbox", prefix)).isEqualTo(16);
                    assertThat(pendingCount(prefix)).isZero();

                    try (var consumerDatabase = EmbeddedPostgres.builder().start()) {
                        String consumerJdbcUrl = consumerDatabase.getJdbcUrl("postgres", "postgres");
                        try (var connection = DriverManager.getConnection(
                                consumerJdbcUrl, "postgres", "postgres");
                             var statement = connection.createStatement()) {
                            statement.execute("""
                                    CREATE TABLE exp0004_effect (
                                        event_id UUID PRIMARY KEY,
                                        transaction_id TEXT NOT NULL UNIQUE)
                                    """);
                        }
                        String groupId = "exp0004-batch-fail-effect-" + run;
                        assertThat(runConsumer("crash-after-db", outputTopic, consumerJdbcUrl,
                                groupId, 1)).isEqualTo(137);
                        assertThat(effectCount(consumerJdbcUrl)).isEqualTo(1);
                        assertThat(runConsumer("normal", outputTopic, consumerJdbcUrl,
                                groupId, 32)).isZero();
                        assertThat(effectCount(consumerJdbcUrl)).isEqualTo(16);
                        var offset = admin.listConsumerGroupOffsets(groupId)
                                .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS)
                                .get(new TopicPartition(outputTopic, 0));
                        assertThat(offset).isNotNull();
                        assertThat(offset.offset()).isEqualTo(32);
                    }
                }
            } finally {
                inbox.stop();
            }
        }
    }

    @Test
    void deliveryResumesAfterDispatcherUnavailable() throws Exception {
        assumeTrue(EXTERNAL && "true".equalsIgnoreCase(System.getenv("EVC_EXP0004_RECOVERY")));
        String mode = System.getenv("EVC_EXP0004_RELAY_MODE");
        if (!mode.equals("BATCH_ACK") && !mode.equals("CDC")) throw new IllegalArgumentException(mode);
        int backlogCount = Integer.parseInt(System.getenv().getOrDefault("EVC_EXP0004_BACKLOG_COUNT", "1"));
        int outageSeconds = Integer.parseInt(System.getenv().getOrDefault("EVC_EXP0004_OUTAGE_SECONDS", "1"));
        if (backlogCount < 1 || backlogCount > 1000 || outageSeconds < 1 || outageSeconds > 120) {
            throw new IllegalArgumentException("backlog count must be 1..1000 and outage seconds 1..120");
        }
        int expected = backlogCount + 1;
        String run = UUID.randomUUID().toString();
        String prefix = "TX-DISPATCH-" + run + "-";
        String inputTopic = "exp0004-recovery-input-" + run;
        createTables();
        if (mode.equals("CDC")) registerExternalConnector();
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", EXTERNAL_BROKERS));
             var input = producer(EXTERNAL_BROKERS);
             var output = producer(EXTERNAL_BROKERS)) {
            admin.createTopics(List.of(new NewTopic(inputTopic, 1, (short) 1),
                    new NewTopic(EXTERNAL_TOPIC, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            var inbox = factory.createContainer(inputTopic);
            inbox.getContainerProperties().setGroupId("exp0004-recovery-inbox-" + run);
            inbox.getContainerProperties().setPollTimeout(100);
            inbox.getContainerProperties().setMessageListener(
                    (MessageListener<String, String>) listener::consume);
            inbox.start();
            try {
                for (int index = 0; index < expected; index++) {
                    send(input, inputTopic, prefix + index, 0, "Started", 10000);
                    send(input, inputTopic, prefix + index, 1, "Ended", 12500);
                }
                await().atMost(Duration.ofSeconds(25)).untilAsserted(() ->
                        assertThat(validCandidateCount(prefix)).isEqualTo(expected));
                var config = new HikariConfig();
                config.setJdbcUrl("jdbc:postgresql://127.0.0.1:15439/exp0003");
                config.setUsername("postgres");
                config.setPassword("exp0003_only");
                config.setMaximumPoolSize(16);
                try (var pool = new HikariDataSource(config)) {
                    UUID first = commitCloseout(pool, prefix + 0);
                    var acked = new ConcurrentHashMap<UUID, Long>();
                    var marked = new ConcurrentHashMap<UUID, Long>();
                    var ackMs = new ArrayList<Double>();
                    var markMs = new ArrayList<Double>();
                    if (mode.equals("BATCH_ACK")) {
                        assertThat(pollBatch(pool, output, EXTERNAL_TOPIC, prefix, mode, false,
                                acked, marked, ackMs, markMs)).isEqualTo(1);
                    }
                    assertThat(readExternalEventIds(EXTERNAL_TOPIC, 1, Duration.ofSeconds(15)))
                            .containsExactly(first);
                    assertThat(endOffset(admin, EXTERNAL_TOPIC)).isEqualTo(1);

                    if (mode.equals("CDC")) composeExternal("stop");
                    ObjectNode slotBeforeBacklog = mode.equals("CDC") ? cdcSlotSnapshot() : null;
                    if (slotBeforeBacklog != null) assertThat(slotBeforeBacklog.path("active").asBoolean()).isFalse();
                    long unavailableStartNs = System.nanoTime();
                    var expectedIds = new ArrayList<UUID>();
                    expectedIds.add(first);
                    for (int index = 1; index < expected; index++) {
                        expectedIds.add(commitCloseout(pool, prefix + index));
                    }
                    Thread.sleep(TimeUnit.SECONDS.toMillis(outageSeconds));
                    assertThat(endOffset(admin, EXTERNAL_TOPIC)).isEqualTo(1);
                    ObjectNode slotDuringBacklog = mode.equals("CDC") ? cdcSlotSnapshot() : null;
                    if (slotDuringBacklog != null) {
                        assertThat(slotDuringBacklog.path("active").asBoolean()).isFalse();
                        assertThat(slotDuringBacklog.path("confirmedFlushLsn").asText())
                                .isEqualTo(slotBeforeBacklog.path("confirmedFlushLsn").asText());
                        assertThat(slotDuringBacklog.path("walStatus").asText()).isNotEqualTo("lost");
                    }

                    long resumeStartedNs = System.nanoTime();
                    if (mode.equals("CDC")) {
                        composeExternal("start");
                        waitForExternalConnectorRunning();
                    } else {
                        int delivered = 0;
                        while (delivered < backlogCount) {
                            int batch = pollBatch(pool, output, EXTERNAL_TOPIC, prefix, mode, false,
                                    acked, marked, ackMs, markMs);
                            assertThat(batch).isPositive();
                            delivered += batch;
                        }
                    }
                    var ids = readExternalEventIds(EXTERNAL_TOPIC, expected, Duration.ofSeconds(90));
                    long observedNs = System.nanoTime();
                    assertThat(ids).containsExactlyInAnyOrderElementsOf(expectedIds);
                    Thread.sleep(1000);
                    assertThat(endOffset(admin, EXTERNAL_TOPIC)).isEqualTo(expected);
                    assertThat(count("exp0004_closeout", prefix)).isEqualTo(expected);
                    assertThat(count("exp0004_outbox", prefix)).isEqualTo(expected);
                    if (mode.equals("BATCH_ACK")) assertThat(pendingCount(prefix)).isZero();
                    ObjectNode slotAfterKafkaObserved = mode.equals("CDC") ? cdcSlotSnapshot() : null;
                    ObjectNode slotAfterConfirmedLsnAdvance = null;
                    long confirmedLsnAdvanceObservedNs = 0;
                    if (slotAfterKafkaObserved != null) {
                        assertThat(slotAfterKafkaObserved.path("active").asBoolean()).isTrue();
                        assertThat(slotAfterKafkaObserved.path("walStatus").asText()).isNotEqualTo("lost");
                        await().atMost(Duration.ofSeconds(75)).pollInterval(Duration.ofSeconds(1))
                                .untilAsserted(() -> assertThat(cdcSlotSnapshot()
                                        .path("confirmedFlushLsn").asText())
                                        .isNotEqualTo(slotDuringBacklog.path("confirmedFlushLsn").asText()));
                        slotAfterConfirmedLsnAdvance = cdcSlotSnapshot();
                        confirmedLsnAdvanceObservedNs = System.nanoTime();
                    }

                    String consumerJdbcUrl = POSTGRES.getJdbcUrl("postgres", "postgres");
                    try (var connection = DriverManager.getConnection(consumerJdbcUrl, "postgres", "postgres");
                         var statement = connection.createStatement()) {
                        statement.execute("CREATE TABLE IF NOT EXISTS exp0004_effect (event_id UUID PRIMARY KEY, transaction_id TEXT NOT NULL UNIQUE)");
                    }
                    int consumerExit = runExternalConsumer(EXTERNAL_BROKERS, EXTERNAL_TOPIC,
                            consumerJdbcUrl, run, expected);
                    assertThat(consumerExit).isZero();
                    assertThat(effectCount(consumerJdbcUrl)).isEqualTo(expected);
                    var result = mapper.createObjectNode();
                    result.put("mode", mode);
                    result.put("testOnly", true);
                    result.put("backlogCount", backlogCount);
                    result.put("outageSeconds", outageSeconds);
                    result.put("candidateValid", validCandidateCount(prefix));
                    result.put("closeouts", count("exp0004_closeout", prefix));
                    result.put("outbox", count("exp0004_outbox", prefix));
                    result.put("kafkaWhileUnavailable", 1);
                    result.put("kafkaAfterResume", endOffset(admin, EXTERNAL_TOPIC));
                    result.put("independentEffectCount", effectCount(consumerJdbcUrl));
                    result.put("unavailableWindowMs", (resumeStartedNs - unavailableStartNs) / 1_000_000.0);
                    result.put("resumeToKafkaObservationMs", (observedNs - resumeStartedNs) / 1_000_000.0);
                    if (mode.equals("CDC")) {
                        result.set("slotBeforeBacklog", slotBeforeBacklog);
                        result.set("slotDuringBacklog", slotDuringBacklog);
                        result.set("slotAfterKafkaObserved", slotAfterKafkaObserved);
                        result.set("slotAfterConfirmedLsnAdvance", slotAfterConfirmedLsnAdvance);
                        result.put("resumeToConfirmedLsnAdvanceMs",
                                (confirmedLsnAdvanceObservedNs - resumeStartedNs) / 1_000_000.0);
                    }
                    Path outputPath = Path.of("build", "experiment-results", "exp0004-recovery-"
                            + System.getenv("EVC_EXP0004_RUN_ID") + ".json");
                    Files.createDirectories(outputPath.getParent());
                    Files.writeString(outputPath, mapper.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(result));
                }
            } finally {
                inbox.stop();
            }
        }
    }

    private void runRatePilot(String run, String prefix, String topic, String brokers,
                              KafkaProducer<String, String> producer, String relayMode) throws Exception {
        var config = new HikariConfig();
        config.setJdbcUrl(EXTERNAL ? "jdbc:postgresql://127.0.0.1:15439/exp0003"
                : POSTGRES.getJdbcUrl("postgres", "postgres"));
        config.setUsername("postgres");
        config.setPassword(EXTERNAL ? "exp0003_only" : "postgres");
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
            var cpuAtPlannedEndNs = new AtomicLong(-1);
            var peakJvmHeapUsedBytes = new AtomicLong();
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

            Future<Integer> relay = relayMode.equals("CDC") ? null : executor.submit(() -> {
                int published = 0;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SECONDS + 60L);
                startSignal.await();
                while (published < EXPECTED && System.nanoTime() < deadline) {
                    int count = pollBatch(pool, producer, topic, prefix, relayMode, false,
                            ackedAt, markedAt, sendAckDurations, markDurations);
                    published += count;
                    if (count == 0) LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
                }
                return published;
            });

            OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            long cpuStartNs = os.getProcessCpuTime();
            Map<String, Long> dbAtStart = databaseCounters();
            long walBytesAtStart = walBytes();
            long inputStartedAtEpochMillis = System.currentTimeMillis();
            long startedNs = System.nanoTime();
            long plannedEndNs = startedNs + TimeUnit.SECONDS.toNanos(SECONDS);
            Future<?> sampler = executor.submit(() -> {
                while (!stopSampler.get()) {
                    long now = System.nanoTime();
                    if (now >= plannedEndNs) cpuAtPlannedEndNs.compareAndSet(-1, os.getProcessCpuTime());
                    long heapUsed = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
                    peakJvmHeapUsedBytes.accumulateAndGet(heapUsed, Math::max);
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
            int relayPublished = relay == null ? 0 : relay.get(70, TimeUnit.SECONDS);
            observer.get(70, TimeUnit.SECONDS);
            stopSampler.set(true);
            sampler.get(5, TimeUnit.SECONDS);
            Map<String, Long> dbAfterDrain = databaseCounters();
            long walBytesAfterDrain = walBytes();

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
            long pending = pendingCount(prefix);
            var result = mapper.createObjectNode();
            result.put("issue", 60);
            result.put("mode", relayMode.equals("CDC")
                    ? "OCPP-derived test closeout + Debezium CDC poll 50ms"
                    : "OCPP-derived test closeout + single batch-16 Polling Relay");
            result.put("comparisonIssue", 62);
            if (EXPECTED == 20_000) result.put("durationIssue", 64);
            result.put("relayMode", relayMode);
            result.put("ratePerSecond", RATE);
            result.put("seconds", SECONDS);
            result.put("inputStartedAtEpochMillis", inputStartedAtEpochMillis);
            result.put("inputPlannedEndAtEpochMillis", inputStartedAtEpochMillis + SECONDS * 1000L);
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
            if (relayMode.equals("CDC")) result.putNull("pendingOutbox");
            else result.put("pendingOutbox", pending);
            result.put("unmarkedOutboxRows", pending);
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
            result.put("javaTempDir", System.getProperty("java.io.tmpdir"));
            result.put("postgresVersion", jdbc.sql("SHOW server_version").query(String.class).single());
            result.put("kafkaPartitions", 1);
            result.put("kafkaReplicas", 1);
            result.put("writerThreads", 8);
            result.put("poolConnectionsMax", 16);
            result.put("relayBatch", 16);
            result.put("testJvmCpuOneCorePercentDuringInput", 100.0
                    * (cpuAtPlannedEndNs.get() - cpuStartNs) / TimeUnit.SECONDS.toNanos(SECONDS));
            result.put("testJvmPeakHeapUsedBytesDuringInput", peakJvmHeapUsedBytes.get());
            var databaseDeltas = mapper.createObjectNode();
            for (var entry : dbAtStart.entrySet()) {
                databaseDeltas.put(entry.getKey(), dbAfterDrain.get(entry.getKey()) - entry.getValue());
            }
            result.set("databaseCountersInputAndDrain", databaseDeltas);
            result.put("databaseWalBytesInputAndDrain", walBytesAfterDrain - walBytesAtStart);
            result.put("externalCompose", EXTERNAL);
            if (EXTERNAL && relayMode.equals("CDC")) {
                result.put("cdcRetainedWalBytesAfterDrain", retainedWalBytes());
                result.put("cdcUnconfirmedWalBytesAfterDrain", unconfirmedWalBytes());
            } else {
                result.putNull("cdcRetainedWalBytesAfterDrain");
                result.putNull("cdcUnconfirmedWalBytesAfterDrain");
            }
            if (EXTERNAL) {
                String consumerJdbcUrl = POSTGRES.getJdbcUrl("postgres", "postgres");
                try (var connection = DriverManager.getConnection(consumerJdbcUrl, "postgres", "postgres");
                     var statement = connection.createStatement()) {
                    statement.execute("CREATE TABLE IF NOT EXISTS exp0004_effect (event_id UUID PRIMARY KEY, transaction_id TEXT NOT NULL UNIQUE)");
                }
                int consumerExit = runExternalConsumer(brokers, topic, consumerJdbcUrl, run, EXPECTED);
                result.put("independentConsumerExit", consumerExit);
                result.put("independentEffectCount", effectCount(consumerJdbcUrl));
                result.put("independentEffectDrainAfterPlannedEndMs",
                        Math.max(0, System.nanoTime() - plannedEndNs) / 1_000_000.0);
                assertThat(consumerExit).isZero();
                assertThat(effectCount(consumerJdbcUrl)).isEqualTo(EXPECTED);
            }
            result.set("samples", mapper.valueToTree(samples));
            String runId = System.getenv().getOrDefault("EVC_EXP0004_RUN_ID", "local");
            Path output = Path.of("build", "experiment-results",
                    "exp-0004-rate-" + RATE + "x" + SECONDS
                            + "-" + relayMode.toLowerCase()
                            + (EXTERNAL ? "-" + runId : "") + ".json");
            Files.createDirectories(output.getParent());
            Files.writeString(output, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
            result.remove("samples");
            System.out.println("EXP0004_RATE_RESULT " + mapper.writeValueAsString(result));

            assertThat(committedBeforeEnd).as("offered-load generator validity")
                    .isGreaterThanOrEqualTo((long) Math.ceil(EXPECTED * 0.95));
            assertThat(committedAt).hasSize(EXPECTED);
            assertThat(closeouts).isEqualTo(EXPECTED);
            assertThat(outbox).isEqualTo(EXPECTED);
            if (relayMode.equals("CDC")) {
                assertThat(pending).isEqualTo(EXPECTED);
                assertThat(relayPublished).isZero();
                assertThat(ackedAt).isEmpty();
                assertThat(markedAt).isEmpty();
            } else {
                assertThat(pending).isZero();
                assertThat(relayPublished).isEqualTo(EXPECTED);
                assertThat(ackedAt).hasSize(EXPECTED);
                assertThat(markedAt).hasSize(EXPECTED);
            }
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
                          String topic, String prefix, String relayMode, boolean failAfterBatchAck,
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
        if (relayMode.equals("BATCH_ACK")) {
            var sends = new ArrayList<Future<?>>(rows.size());
            for (var row : rows) {
                sends.add(sendAndMeasure(producer, topic, row, ackedAt, sendAckDurations));
            }
            for (var send : sends) send.get(10, TimeUnit.SECONDS);
            if (failAfterBatchAck) throw new IllegalStateException("ACKED_BEFORE_MARK");
            for (var row : rows) markPublished(pool, row, markedAt, markDurations);
        } else {
            for (var row : rows) {
                sendAndMeasure(producer, topic, row, ackedAt, sendAckDurations)
                        .get(10, TimeUnit.SECONDS);
                markPublished(pool, row, markedAt, markDurations);
            }
        }
        return rows.size();
    }

    private Future<?> sendAndMeasure(KafkaProducer<String, String> producer, String topic,
                                     OutboxRow row, ConcurrentHashMap<UUID, Long> ackedAt,
                                     List<Double> sendAckDurations) {
        long sendStartedNs = System.nanoTime();
        return producer.send(new ProducerRecord<>(topic, row.stationId(), row.payload()),
                (metadata, failure) -> {
                    if (failure == null) {
                        long ackNs = System.nanoTime();
                        ackedAt.put(row.eventId(), ackNs);
                        sendAckDurations.add((ackNs - sendStartedNs) / 1_000_000.0);
                    }
                });
    }

    private void markPublished(HikariDataSource pool, OutboxRow row,
                               ConcurrentHashMap<UUID, Long> markedAt,
                               List<Double> markDurations) throws Exception {
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
                    aggregatetype TEXT NOT NULL DEFAULT 'exp0004-closeout',
                    type TEXT NOT NULL DEFAULT 'TransactionCloseoutSimulated',
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

    private long pendingCount(String prefix) {
        return jdbc.sql("""
                SELECT count(*) FROM exp0004_outbox
                WHERE transaction_id LIKE :prefix AND published_at IS NULL
                """).param("prefix", prefix + "%").query(Long.class).single();
    }

    private List<UUID> readOutputEventIds(String topic, int expected) throws Exception {
        var partition = new TopicPartition(topic, 0);
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "exp0004-batch-fail-reader-" + UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"))) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            var ids = new ArrayList<UUID>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (ids.size() < expected && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(100))) {
                    ids.add(UUID.fromString(mapper.readTree(record.value()).path("eventId").asText()));
                }
            }
            return ids;
        }
    }

    private int runConsumer(String mode, String topic, String jdbcUrl, String groupId,
                            int expectedRecords) throws Exception {
        String classpath = System.getProperty("transaction.test.runtime-classpath");
        assertThat(classpath).isNotBlank();
        Path output = Path.of("build", "experiment-results", "exp0004-batch-consumer-" + mode + ".log");
        Files.createDirectories(output.getParent());
        var process = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", classpath, Exp0004IndependentConsumerProcess.class.getName(),
                mode, broker.getBrokersAsString(), topic, jdbcUrl, groupId,
                Integer.toString(expectedRecords))
                .redirectErrorStream(true)
                .redirectOutput(output.toFile())
                .start();
        if (!process.waitFor(45, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("independent consumer did not finish");
        }
        return process.exitValue();
    }

    private long effectCount(String jdbcUrl) throws Exception {
        try (var connection = DriverManager.getConnection(jdbcUrl, "postgres", "postgres");
             var statement = connection.createStatement();
             var result = statement.executeQuery("SELECT count(*) FROM exp0004_effect")) {
            result.next();
            return result.getLong(1);
        }
    }

    private int runExternalConsumer(String brokers, String topic, String jdbcUrl,
                                    String run, int expectedRecords) throws Exception {
        String classpath = System.getProperty("transaction.test.runtime-classpath");
        Path output = Path.of("build", "experiment-results", "exp0004-independent-" + run + ".log");
        Files.createDirectories(output.getParent());
        var process = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", classpath, Exp0004IndependentConsumerProcess.class.getName(),
                "normal", brokers, topic, jdbcUrl, "exp0004-effect-" + run,
                Integer.toString(expectedRecords))
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        if (!process.waitFor(120, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("independent consumer did not finish; see " + output);
        }
        return process.exitValue();
    }

    private List<UUID> readExternalEventIds(String topic, int expected, Duration timeout) throws Exception {
        var partition = new TopicPartition(topic, 0);
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, EXTERNAL_BROKERS,
                ConsumerConfig.GROUP_ID_CONFIG, "exp0004-recovery-reader-" + UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"))) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            var ids = new ArrayList<UUID>();
            long deadline = System.nanoTime() + timeout.toNanos();
            while (ids.size() < expected && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(100))) {
                    var event = mapper.readTree(record.value());
                    assertThat(record.key()).isEqualTo("ST-TEST");
                    assertThat(event.path("payload").path("energyWh").asInt()).isEqualTo(2500);
                    ids.add(UUID.fromString(event.path("eventId").asText()));
                }
            }
            return ids;
        }
    }

    private long endOffset(AdminClient admin, String topic) throws Exception {
        var partition = new TopicPartition(topic, 0);
        return admin.listOffsets(Map.of(partition, OffsetSpec.latest())).all()
                .get(10, TimeUnit.SECONDS).get(partition).offset();
    }

    private void composeExternal(String action) throws Exception {
        var process = new ProcessBuilder("docker", "compose", "-f",
                System.getProperty("exp0003.cdc.compose-file"), action, "connect")
                .redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes());
        if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
            process.destroyForcibly();
            throw new IllegalStateException("docker compose " + action + " connect failed: " + output);
        }
    }

    private void waitForExternalConnectorRunning() throws Exception {
        var http = HttpClient.newHttpClient();
        var uri = URI.create("http://127.0.0.1:18083/connectors/exp0004-outbox-cdc/status");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (System.nanoTime() < deadline) {
            try {
                var response = http.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(3))
                        .GET().build(), HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    var status = mapper.readTree(response.body());
                    var tasks = status.path("tasks");
                    if (status.path("connector").path("state").asText().equals("RUNNING")
                            && tasks.isArray() && tasks.size() > 0
                            && tasks.get(0).path("state").asText().equals("RUNNING")) return;
                    if (tasks.isArray() && tasks.size() > 0
                            && tasks.get(0).path("state").asText().equals("FAILED")) {
                        throw new IllegalStateException(response.body());
                    }
                }
            } catch (IOException ignored) {
                // Connect REST is unavailable during container startup.
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("CDC connector did not resume");
    }

    private long retainedWalBytes() {
        return jdbc.sql("""
                SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint
                FROM pg_replication_slots WHERE slot_name = 'exp0004_outbox_slot'
                """).query(Long.class).single();
    }

    private ObjectNode cdcSlotSnapshot() {
        return jdbc.sql("""
                SELECT active, restart_lsn::text, confirmed_flush_lsn::text, wal_status,
                       safe_wal_size::bigint AS safe_wal_size_bytes,
                       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint
                           AS retained_wal_bytes,
                       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
                           AS unconfirmed_wal_bytes,
                       (SELECT COALESCE(sum(size), 0)::bigint FROM pg_ls_waldir()) AS pg_wal_disk_bytes
                FROM pg_replication_slots WHERE slot_name = 'exp0004_outbox_slot'
                """).query((row, number) -> {
                    var snapshot = mapper.createObjectNode();
                    snapshot.put("active", row.getBoolean("active"));
                    snapshot.put("restartLsn", row.getString("restart_lsn"));
                    snapshot.put("confirmedFlushLsn", row.getString("confirmed_flush_lsn"));
                    snapshot.put("walStatus", row.getString("wal_status"));
                    long safeWalSize = row.getLong("safe_wal_size_bytes");
                    if (row.wasNull()) snapshot.putNull("safeWalSizeBytes");
                    else snapshot.put("safeWalSizeBytes", safeWalSize);
                    snapshot.put("retainedWalBytes", row.getLong("retained_wal_bytes"));
                    snapshot.put("unconfirmedWalBytes", row.getLong("unconfirmed_wal_bytes"));
                    snapshot.put("pgWalDiskBytes", row.getLong("pg_wal_disk_bytes"));
                    return snapshot;
                }).single();
    }

    private long unconfirmedWalBytes() {
        return jdbc.sql("""
                SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
                FROM pg_replication_slots WHERE slot_name = 'exp0004_outbox_slot'
                """).query(Long.class).single();
    }

    private Map<String, Long> databaseCounters() {
        return jdbc.sql("""
                SELECT xact_commit, blks_read, blks_hit, tup_inserted, tup_updated
                FROM pg_stat_database WHERE datname = current_database()
                """).query((row, number) -> Map.of(
                        "xactCommit", row.getLong("xact_commit"),
                        "blocksRead", row.getLong("blks_read"),
                        "blocksHit", row.getLong("blks_hit"),
                        "tuplesInserted", row.getLong("tup_inserted"),
                        "tuplesUpdated", row.getLong("tup_updated"))).single();
    }

    private long walBytes() {
        return jdbc.sql("SELECT wal_bytes::bigint FROM pg_stat_wal").query(Long.class).single();
    }

    private void registerExternalConnector() throws Exception {
        var http = HttpClient.newHttpClient();
        var connectorPath = Path.of(System.getProperty("exp0003.cdc.connector-file"))
                .resolveSibling("connector-exp0004.json");
        String body = Files.readString(connectorPath);
        var base = URI.create("http://127.0.0.1:18083");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (System.nanoTime() < deadline) {
            try {
                var ready = http.send(HttpRequest.newBuilder(base.resolve("/connectors"))
                        .timeout(Duration.ofSeconds(3)).GET().build(),
                        HttpResponse.BodyHandlers.discarding());
                if (ready.statusCode() == 200) break;
            } catch (IOException ignored) {
                // Connect may not have bound its REST port yet.
            }
            Thread.sleep(250);
        }
        var created = http.send(HttpRequest.newBuilder(base.resolve("/connectors"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
        if (created.statusCode() != 201) throw new IllegalStateException(created.body());
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (System.nanoTime() < deadline) {
            var response = http.send(HttpRequest.newBuilder(base.resolve("/connectors/exp0004-outbox-cdc/status"))
                    .timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                var state = mapper.readTree(response.body());
                if (state.path("tasks").isArray() && state.path("tasks").size() > 0
                        && state.path("tasks").get(0).path("state").asText().equals("RUNNING")) {
                    var config = http.send(HttpRequest.newBuilder(
                            base.resolve("/connectors/exp0004-outbox-cdc/config")).GET().build(),
                            HttpResponse.BodyHandlers.ofString());
                    assertThat(mapper.readTree(config.body()).path("poll.interval.ms").asText()).isEqualTo("50");
                    return;
                }
                if (state.path("tasks").isArray() && state.path("tasks").size() > 0
                        && state.path("tasks").get(0).path("state").asText().equals("FAILED")) {
                    throw new IllegalStateException(response.body());
                }
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("EXP-0004 CDC connector did not become RUNNING");
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
