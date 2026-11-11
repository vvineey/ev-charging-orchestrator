package com.evcharging.controlplane.transaction;

import com.sun.management.OperatingSystemMXBean;
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
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Local E5 pilot: all modes consume the same Kafka record shape and preserve source events in the
 * same PostgreSQL tables. A/B are test-only, safety-equalized adapters, not production services.
 * B uses a real 300-second wait; no logical-time value is reported as wall-clock latency.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"transaction.ingress.enabled=true", "spring.kafka.listener.auto-startup=false",
                "transaction.recovery.experimental-candidate-calculation-enabled=true"})
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
    @TempDir Path directory;

    record TimingSample(String transactionId, String pattern, Double decisionMillis) { }

    record ModeResult(String mode, int transactions, int normal, int reverse, int candidateCount,
                      int wrongEnergy, int offsetLag, long elapsedMillis,
                      double producedRecordsPerSecond, double runRecordsPerSecond,
                      double observedCandidatesPerSecond,
                      Double p50Millis, Double p95Millis, Double p99Millis,
                      Double normalP50Millis, Double normalP95Millis, Double normalP99Millis,
                      Double reverseP50Millis, Double reverseP95Millis, Double reverseP99Millis,
                      long sourceRows, long stateRows, long candidateRows, long sourceJsonBytes,
                      List<TimingSample> samples) { }

    record Report(String scope, int windowSeconds, int transactionsPerMode, String javaVersion,
                  String osName, String databaseVersion, int bPendingAtCommittedOffset,
                  List<ModeResult> results) { }

    record TableBytes(long source, long state, long candidate) { }

    record DatabaseActivity(long blocksRead, long blocksHit, long tuplesReturned,
                            long tuplesFetched, long tuplesInserted, long tuplesUpdated) { }

    record DatabaseIo(long reads, long writes, long fsyncs, long hits) { }

    record IsolatedReport(String scope, String mode, String run, int requestedRecordsPerSecond,
                          int windowSeconds, boolean restoreFromInbox, String javaVersion, String osName,
                          String databaseVersion, long processCpuNanos, Long postgresCpuNanos,
                          Long jvmSampledPeakRssBytes, Long postgresSampledPeakSummedRssBytes,
                          int postgresSampledPeakProcessCount,
                          long heapUsedBeforeBytes, long heapUsedAfterBytes,
                          TableBytes before, TableBytes after,
                          DatabaseActivity databaseBefore, DatabaseActivity databaseAfter,
                          DatabaseIo ioBefore, DatabaseIo ioAfter,
                          ModeResult result) { }

    @Test
    @EnabledIfSystemProperty(named = "transaction.test.isolated-mode", matches = "A|B|C")
    void isolatedCandidateResourcePilot() throws Exception {
        String mode = System.getProperty("transaction.test.isolated-mode");
        String runLabel = System.getProperty("transaction.test.isolated-run");
        int requestedRate = Integer.parseInt(System.getProperty("transaction.test.isolated-rate"));
        boolean restoreFromInbox = Boolean.getBoolean("transaction.test.restore-from-inbox");
        assertThat(requestedRate).isPositive();
        TableBytes before = tableBytes();
        DatabaseActivity databaseBefore = databaseActivity();
        DatabaseIo ioBefore = databaseIo();
        long heapBefore = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        var operatingSystem = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        long cpuBefore = operatingSystem.getProcessCpuTime();
        int expectedCandidates = "A".equals(mode) ? TRANSACTIONS_PER_MODE / 2 : TRANSACTIONS_PER_MODE;
        ModeResult result;
        try (ProcessSampler processes = new ProcessSampler(
                jdbc.sql("SELECT pg_backend_pid()").query(Integer.class).single());
             CandidateAdapter candidate = "C".equals(mode) ? null
                : new CandidateAdapter(mode, WINDOW_SECONDS, inbox, rules, transactions,
                        jdbc, restoreFromInbox);
             Run started = start(mode, candidate, requestedRate)) {
            result = finish(started, candidate, expectedCandidates,
                    Duration.ofSeconds(WINDOW_SECONDS + 60));
            writeIsolatedReport(mode, runLabel, requestedRate, restoreFromInbox, before,
                    databaseBefore, ioBefore, heapBefore, cpuBefore, operatingSystem,
                    processes, result);
        }
    }

    private void writeIsolatedReport(String mode, String runLabel, int requestedRate,
                                      boolean restoreFromInbox, TableBytes before,
                                      DatabaseActivity databaseBefore, DatabaseIo ioBefore,
                                      long heapBefore, long cpuBefore,
                                      OperatingSystemMXBean operatingSystem,
                                      ProcessSampler processes, ModeResult result) throws IOException {
        long cpuNanos = operatingSystem.getProcessCpuTime() - cpuBefore;
        long heapAfter = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        TableBytes after = tableBytes();
        DatabaseActivity databaseAfter = databaseActivity();
        DatabaseIo ioAfter = databaseIo();
        processes.sample();
        Path output = Path.of(System.getProperty("transaction.test.isolated-output-dir"),
                "exp-0002-isolated-" + mode + (restoreFromInbox ? "-restored-" : "-baseline-")
                        + runLabel + ".json");
        Files.createDirectories(output.getParent());
        Files.writeString(output, mapper.writeValueAsString(new IsolatedReport(
                "single-candidate-embedded-resource-pilot-not-final-e5", mode, runLabel,
                requestedRate, WINDOW_SECONDS, restoreFromInbox, System.getProperty("java.version"),
                System.getProperty("os.name"), jdbc.sql("SELECT version()").query(String.class).single(),
                cpuNanos, processes.postgresCpuNanos(), processes.jvmPeakRssBytes(),
                processes.postgresPeakRssBytes(), processes.postgresPeakProcessCount(),
                heapBefore, heapAfter, before, after,
                databaseBefore, databaseAfter, ioBefore, ioAfter, result)));
    }

    private TableBytes tableBytes() {
        return new TableBytes(relationBytes("transaction_inbox_event"),
                relationBytes("transaction_inbox_state"),
                relationBytes("transaction_session_candidate"));
    }

    private DatabaseActivity databaseActivity() {
        return jdbc.sql("""
                SELECT blks_read, blks_hit, tup_returned, tup_fetched,
                       tup_inserted, tup_updated
                FROM pg_stat_database WHERE datname = current_database()
                """).query((rs, row) -> new DatabaseActivity(
                rs.getLong("blks_read"), rs.getLong("blks_hit"),
                rs.getLong("tup_returned"), rs.getLong("tup_fetched"),
                rs.getLong("tup_inserted"), rs.getLong("tup_updated"))).single();
    }

    private DatabaseIo databaseIo() {
        return jdbc.sql("""
                SELECT COALESCE(SUM(reads), 0) AS reads, COALESCE(SUM(writes), 0) AS writes,
                       COALESCE(SUM(fsyncs), 0) AS fsyncs, COALESCE(SUM(hits), 0) AS hits
                FROM pg_stat_io
                """).query((rs, row) -> new DatabaseIo(
                rs.getLong("reads"), rs.getLong("writes"),
                rs.getLong("fsyncs"), rs.getLong("hits"))).single();
    }

    private static final class ProcessSampler implements AutoCloseable {
        private final ProcessHandle postgres;
        private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        private final Map<Long, Long> firstCpu = new ConcurrentHashMap<>();
        private final Map<Long, Long> lastCpu = new ConcurrentHashMap<>();
        private final AtomicLong jvmPeakRss = new AtomicLong(-1);
        private final AtomicLong postgresPeakRss = new AtomicLong(-1);
        private final AtomicInteger postgresPeakProcessCount = new AtomicInteger();

        ProcessSampler(int backendPid) {
            postgres = ProcessHandle.of(backendPid).flatMap(ProcessHandle::parent)
                    .orElseThrow(() -> new IllegalStateException("PostgreSQL postmaster not found"));
            assertThat(postgres.info().command().orElse("")).contains("postgres");
            sample(true);
            scheduler.scheduleAtFixedRate(this::sample, 1, 1, TimeUnit.SECONDS);
        }

        synchronized void sample() {
            sample(false);
        }

        private synchronized void sample(boolean baseline) {
            List<ProcessHandle> pg = postgresProcesses();
            postgresPeakProcessCount.accumulateAndGet(pg.size(), Math::max);
            List<Long> pids = new ArrayList<>(pg.stream().map(ProcessHandle::pid).toList());
            pids.add(ProcessHandle.current().pid());
            try {
                Process ps = new ProcessBuilder("ps", "-o", "pid=,rss=,time=", "-p",
                        pids.stream().map(String::valueOf).reduce((left, right) -> left + "," + right)
                                .orElseThrow()).start();
                String output = new String(ps.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                if (ps.waitFor() != 0) return;
                long postgresRss = 0;
                for (String line : output.split("\\R")) {
                    String[] columns = line.trim().split("\\s+");
                    if (columns.length != 3) continue;
                    long pid = Long.parseLong(columns[0]);
                    long rss = Long.parseLong(columns[1]) * 1024;
                    if (pid == ProcessHandle.current().pid()) {
                        jvmPeakRss.accumulateAndGet(rss, Math::max);
                    } else {
                        postgresRss += rss;
                        long cpuNanos = processCpuNanos(columns[2]);
                        if (baseline) firstCpu.put(pid, cpuNanos);
                        lastCpu.put(pid, cpuNanos);
                    }
                }
                postgresPeakRss.accumulateAndGet(postgresRss, Math::max);
            } catch (IOException exception) {
                // A non-Unix host can still run the experiment without sampled RSS.
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }

        private long processCpuNanos(String time) {
            String[] daysAndTime = time.split("-", 2);
            long days = daysAndTime.length == 2 ? Long.parseLong(daysAndTime[0]) : 0;
            String[] components = daysAndTime[daysAndTime.length - 1].split(":");
            double seconds = 0;
            for (String component : components) {
                seconds = seconds * 60 + Double.parseDouble(component);
            }
            return Math.round((seconds + days * 86_400.0) * 1_000_000_000);
        }

        private List<ProcessHandle> postgresProcesses() {
            try (var descendants = postgres.descendants()) {
                List<ProcessHandle> result = new ArrayList<>();
                result.add(postgres);
                result.addAll(descendants.toList());
                return result;
            }
        }

        // ps TIME is cumulative per process; retain the last sample when a child exits.
        Long postgresCpuNanos() {
            if (lastCpu.isEmpty()) return null;
            return lastCpu.entrySet().stream().mapToLong(entry ->
                    Math.max(0, entry.getValue() - firstCpu.getOrDefault(entry.getKey(), 0L)))
                    .sum();
        }

        Long jvmPeakRssBytes() {
            return jvmPeakRss.get() < 0 ? null : jvmPeakRss.get();
        }

        Long postgresPeakRssBytes() {
            return postgresPeakRss.get() < 0 ? null : postgresPeakRss.get();
        }

        int postgresPeakProcessCount() {
            return postgresPeakProcessCount.get();
        }

        @Override
        public void close() {
            scheduler.shutdownNow();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private long relationBytes(String table) {
        return jdbc.sql("SELECT pg_total_relation_size(CAST(:tableName AS regclass))")
                .param("tableName", table).query(Long.class).single();
    }

    @Test
    void sameKafkaPostgresBoundaryShowsCorrectnessLatencyAndStorageTradeoffs() throws Exception {
        List<ModeResult> results = new ArrayList<>();
        int bPendingAtCommittedOffset;
        // B runs first so its real timer can elapse while A and C are measured.
        try (CandidateAdapter b = adapter("B", WINDOW_SECONDS)) {
            Run bRun = start("B", b);
            try {
                results.add(measure("A", adapter("A", 0), TRANSACTIONS_PER_MODE / 2));
                results.add(measure("C", null, 120));
                if (WINDOW_SECONDS >= 60) {
                    assertThat(offset(bRun.admin, bRun.group, bRun.topic))
                            .isEqualTo(TRANSACTIONS_PER_MODE * 2);
                    assertThat(bRun.candidateNanos).isEmpty();
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
        assertThat(results.get(0).candidateCount()).isEqualTo(TRANSACTIONS_PER_MODE / 2);
        assertThat(results.get(1).candidateCount()).isEqualTo(TRANSACTIONS_PER_MODE);
        assertThat(results.get(2).candidateCount()).isEqualTo(TRANSACTIONS_PER_MODE);
        assertThat(results).allSatisfy(result -> {
            assertThat(result.wrongEnergy()).isZero();
            assertThat(result.offsetLag()).isZero();
            assertThat(result.sourceRows()).isEqualTo(TRANSACTIONS_PER_MODE * 2);
            assertThat(result.stateRows()).isEqualTo(TRANSACTIONS_PER_MODE);
            assertThat(result.candidateRows()).isEqualTo(result.candidateCount());
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

    @Test
    void committedStartThenConsumerRestartExposesVolatileCandidateState() throws Exception {
        for (String mode : List.of("A", "B", "C")) {
            verifyCommittedStartAcrossConsumerRestart(mode);
        }
    }

    @Test
    void actualJvmTerminationAfterCommittedStartPreservesOnlyDurableCandidateProgress()
            throws Exception {
        verifyJvmTerminationAfterCommittedStart(false);
    }

    @Test
    void durableSourceRestoresTestOnlyCandidatesAfterJvmTermination() throws Exception {
        verifyJvmTerminationAfterCommittedStart(true);
    }

    @Test
    void restoredWindowRespectsExpiryAndIgnoresDuplicateSource() {
        Map<String, Long> candidateNanos = new ConcurrentHashMap<>();
        try (CandidateAdapter b = new CandidateAdapter("B", 1, inbox, rules, transactions,
                jdbc, true)) {
            String expiredId = "expired-window-" + UUID.randomUUID();
            String start = observed(expiredId, 0);
            b.accept(decoder.decode("ST-BENCH", start), candidateNanos);
            b.accept(decoder.decode("ST-BENCH", start), candidateNanos);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(b.windows).isEmpty());
            String end = observed(expiredId, 1);
            b.accept(decoder.decode("ST-BENCH", end), candidateNanos);
            b.accept(decoder.decode("ST-BENCH", end), candidateNanos);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(b.windows).isEmpty());
            assertThat(countForTransaction("transaction_inbox_event", expiredId)).isEqualTo(2);
            assertThat(countForTransaction("transaction_session_candidate", expiredId)).isZero();

            String withinId = "within-window-" + UUID.randomUUID();
            String withinStart = observed(withinId, 0);
            String withinEnd = observed(withinId, 1);
            b.accept(decoder.decode("ST-BENCH", withinStart), candidateNanos);
            b.accept(decoder.decode("ST-BENCH", withinStart), candidateNanos);
            b.accept(decoder.decode("ST-BENCH", withinEnd), candidateNanos);
            b.accept(decoder.decode("ST-BENCH", withinEnd), candidateNanos);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(countForTransaction("transaction_session_candidate", withinId))
                            .isEqualTo(1));
            assertThat(countForTransaction("transaction_inbox_event", withinId)).isEqualTo(2);
        }
    }

    @Test
    void restoredArrivalStillRequiresStartBeforeEnd() {
        String id = "reverse-arrival-" + UUID.randomUUID();
        try (CandidateAdapter a = new CandidateAdapter("A", 0, inbox, rules, transactions,
                jdbc, true)) {
            a.accept(decoder.decode("ST-BENCH", observed(id, 1)), new ConcurrentHashMap<>());
            a.accept(decoder.decode("ST-BENCH", observed(id, 0)), new ConcurrentHashMap<>());
        }
        assertThat(countForTransaction("transaction_inbox_event", id)).isEqualTo(2);
        assertThat(countForTransaction("transaction_session_candidate", id)).isZero();
    }

    @Test
    void jvmHaltBetweenDatabaseCommitAndOffsetCommitReplaysAllThreeCandidates()
            throws Exception {
        verifyJvmHaltBeforeOffsetCommit(false);
        verifyJvmHaltBeforeOffsetCommit(true);
    }

    private void verifyJvmHaltBeforeOffsetCommit(boolean restoreFromInbox) throws Exception {
        String runId = UUID.randomUUID().toString();
        Map<String, String> topics = Map.of(
                "A", "transaction-gap-A-" + runId,
                "B", "transaction-gap-B-" + runId,
                "C", "transaction-gap-C-" + runId);
        Map<String, String> ids = Map.of(
                "A", "gap-A-" + runId,
                "B", "gap-B-" + runId,
                "C", "gap-C-" + runId);
        String bootstrap = broker.getBrokersAsString();
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap));
             var producer = new KafkaProducer<String, String>(Map.of(
                     ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                     ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.ACKS_CONFIG, "all"))) {
            admin.createTopics(topics.values().stream().map(topic -> new NewTopic(topic, 1, (short) 1))
                    .toList()).all().get(10, TimeUnit.SECONDS);
            Process first = startCandidateProcess(topics, 30,
                    (restoreFromInbox ? "restored-" : "baseline-") + "gap-before-halt",
                    restoreFromInbox, true);
            try {
                for (String mode : List.of("A", "B", "C")) {
                    producer.send(new ProducerRecord<>(topics.get(mode), 0, "ST-BENCH",
                            observed(ids.get(mode), 0))).get(10, TimeUnit.SECONDS);
                }
                await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                    assertThat(first.isAlive()).isTrue();
                    for (String mode : List.of("A", "B", "C")) {
                        assertThat(countForTransaction("transaction_inbox_event", ids.get(mode)))
                                .isEqualTo(1);
                        assertThat(offset(admin, topics.get(mode), topics.get(mode)))
                                .as("uncommitted offset for %s", mode).isZero();
                    }
                });
            } finally {
                first.destroyForcibly();
                assertThat(first.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
            assertThat(first.exitValue()).isNotZero();

            Process second = startCandidateProcess(topics, 30,
                    (restoreFromInbox ? "restored-" : "baseline-") + "gap-after-halt",
                    restoreFromInbox);
            try {
                for (String mode : List.of("A", "B", "C")) {
                    producer.send(new ProducerRecord<>(topics.get(mode), 0, "ST-BENCH",
                            observed(ids.get(mode), 1))).get(10, TimeUnit.SECONDS);
                }
                await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                    assertThat(second.isAlive()).isTrue();
                    for (String mode : List.of("A", "B", "C")) {
                        assertThat(offset(admin, topics.get(mode), topics.get(mode))).isEqualTo(2);
                        assertThat(countForTransaction("transaction_inbox_event", ids.get(mode)))
                                .isEqualTo(2);
                    }
                });
                await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
                    for (String mode : List.of("A", "B", "C")) {
                        assertThat(countForTransaction("transaction_session_candidate", ids.get(mode)))
                                .as("provisional candidate after replay for %s", mode)
                                .isEqualTo(!restoreFromInbox && "A".equals(mode) ? 0 : 1);
                    }
                });
                for (String mode : List.of("A", "B", "C")) {
                    if (!restoreFromInbox && "A".equals(mode)) continue;
                    assertThat(jdbc.sql("""
                            SELECT energy_wh FROM transaction_session_candidate
                            WHERE transaction_id = :id
                            """).param("id", ids.get(mode)).query(Integer.class).single())
                            .isEqualTo(2500);
                }
            } finally {
                second.destroyForcibly();
                assertThat(second.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    void restoredWindowTimerFiresAfterJvmRestartWithoutAnotherRecord() throws Exception {
        String runId = UUID.randomUUID().toString();
        String bId = "idle-B-" + runId;
        Map<String, String> topics = Map.of(
                "A", "transaction-idle-A-" + runId,
                "B", "transaction-idle-B-" + runId,
                "C", "transaction-idle-C-" + runId);
        Path marker = directory.resolve("restored-b-window-fired.txt");
        String bootstrap = broker.getBrokersAsString();
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap));
             var producer = new KafkaProducer<String, String>(Map.of(
                     ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                     ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.ACKS_CONFIG, "all"))) {
            admin.createTopics(topics.values().stream().map(topic -> new NewTopic(topic, 1, (short) 1))
                    .toList()).all().get(10, TimeUnit.SECONDS);
            Process first = startCandidateProcess(topics, 30, "idle-before-halt", false);
            try {
                producer.send(new ProducerRecord<>(topics.get("B"), 0, "ST-BENCH",
                        observed(bId, 0))).get(10, TimeUnit.SECONDS);
                await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                    assertThat(first.isAlive()).isTrue();
                    assertThat(offset(admin, topics.get("B"), topics.get("B"))).isEqualTo(1);
                    assertThat(countForTransaction("transaction_inbox_event", bId)).isEqualTo(1);
                });
            } finally {
                first.destroyForcibly();
                assertThat(first.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
            assertThat(first.exitValue()).isNotZero();

            Process second = startCandidateProcess(topics, 30, "idle-after-halt", true,
                    false, bId, marker);
            try {
                await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
                    assertThat(second.isAlive()).isTrue();
                    assertThat(Files.exists(marker)).isTrue();
                    assertThat(Files.readString(marker)).contains(bId);
                });
                assertThat(offset(admin, topics.get("B"), topics.get("B"))).isEqualTo(1);
                assertThat(countForTransaction("transaction_inbox_event", bId)).isEqualTo(1);
                assertThat(countForTransaction("transaction_session_candidate", bId)).isZero();
            } finally {
                second.destroyForcibly();
                assertThat(second.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private void verifyJvmTerminationAfterCommittedStart(boolean restoreFromInbox) throws Exception {
        String runId = UUID.randomUUID().toString();
        Map<String, String> topics = Map.of(
                "A", "transaction-jvm-A-" + runId,
                "B", "transaction-jvm-B-" + runId,
                "C", "transaction-jvm-C-" + runId);
        Map<String, String> ids = Map.of(
                "A", "jvm-restart-A-" + runId,
                "B", "jvm-restart-B-" + runId,
                "C", "jvm-restart-C-" + runId);
        String bootstrap = broker.getBrokersAsString();
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap));
             var producer = new KafkaProducer<String, String>(Map.of(
                     ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                     ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.ACKS_CONFIG, "all"))) {
            admin.createTopics(topics.values().stream().map(topic -> new NewTopic(topic, 1, (short) 1))
                    .toList()).all().get(10, TimeUnit.SECONDS);
            Process first = startCandidateProcess(topics, 30,
                    (restoreFromInbox ? "restored-" : "baseline-") + "before-halt",
                    restoreFromInbox);
            try {
                for (String mode : List.of("A", "B", "C")) {
                    producer.send(new ProducerRecord<>(topics.get(mode), 0, "ST-BENCH",
                            observed(ids.get(mode), 0))).get(10, TimeUnit.SECONDS);
                }
                await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                    assertThat(first.isAlive()).isTrue();
                    for (String mode : List.of("A", "B", "C")) {
                        assertThat(offset(admin, topics.get(mode), topics.get(mode))).isEqualTo(1);
                    }
                });
                for (String mode : List.of("A", "B", "C")) {
                    assertThat(countForTransaction("transaction_inbox_event", ids.get(mode))).isEqualTo(1);
                    assertThat(countForTransaction("transaction_session_candidate", ids.get(mode))).isZero();
                }
            } finally {
                first.destroyForcibly();
                assertThat(first.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
            assertThat(first.exitValue()).isNotZero();

            Process second = startCandidateProcess(topics, 30,
                    (restoreFromInbox ? "restored-" : "baseline-") + "after-halt",
                    restoreFromInbox);
            try {
                for (String mode : List.of("A", "B", "C")) {
                    producer.send(new ProducerRecord<>(topics.get(mode), 0, "ST-BENCH",
                            observed(ids.get(mode), 1))).get(10, TimeUnit.SECONDS);
                }
                await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                    assertThat(second.isAlive()).isTrue();
                    for (String mode : List.of("A", "B", "C")) {
                        assertThat(offset(admin, topics.get(mode), topics.get(mode))).isEqualTo(2);
                    }
                });
                await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                        assertThat(countForTransaction("transaction_session_candidate", ids.get("C")))
                                .isEqualTo(1));
                if (restoreFromInbox) {
                    await().atMost(Duration.ofSeconds(35)).untilAsserted(() ->
                            assertThat(countForTransaction("transaction_session_candidate", ids.get("B")))
                                    .isEqualTo(1));
                } else {
                    // B has received only Ended in this JVM. Let the same 30-second window elapse.
                    Thread.sleep(32_000);
                }
                for (String mode : List.of("A", "B", "C")) {
                    assertThat(countForTransaction("transaction_inbox_event", ids.get(mode))).isEqualTo(2);
                    assertThat(countForTransaction("transaction_session_candidate", ids.get(mode)))
                            .as("candidate %s after JVM halt", mode)
                            .isEqualTo(restoreFromInbox || "C".equals(mode) ? 1 : 0);
                }
                for (String mode : List.of("A", "B", "C")) {
                    if (restoreFromInbox || "C".equals(mode)) {
                        assertThat(jdbc.sql("""
                                SELECT energy_wh FROM transaction_session_candidate
                                WHERE transaction_id = :id
                                """).param("id", ids.get(mode)).query(Integer.class).single())
                                .isEqualTo(2500);
                    }
                }
            } finally {
                second.destroyForcibly();
                assertThat(second.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private Process startCandidateProcess(Map<String, String> topics, int windowSeconds,
                                          String phase, boolean restoreFromInbox) throws IOException {
        return startCandidateProcess(topics, windowSeconds, phase, restoreFromInbox, false);
    }

    private Process startCandidateProcess(Map<String, String> topics, int windowSeconds,
                                          String phase, boolean restoreFromInbox,
                                          boolean pauseAfterStart) throws IOException {
        return startCandidateProcess(topics, windowSeconds, phase, restoreFromInbox,
                pauseAfterStart, "", null);
    }

    private Process startCandidateProcess(Map<String, String> topics, int windowSeconds,
                                          String phase, boolean restoreFromInbox,
                                          boolean pauseAfterStart, String bRestorePrefix,
                                          Path windowMarker) throws IOException {
        var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java")
                .toString(), "-cp", System.getProperty("transaction.test.runtime-classpath"),
                TransactionCandidateRestartProcess.class.getName(),
                "--spring.kafka.listener.auto-startup=false",
                "--transaction.ingress.enabled=true",
                "--transaction.recovery.experimental-candidate-calculation-enabled=true",
                "--transaction.test.topic-a=" + topics.get("A"),
                "--transaction.test.topic-b=" + topics.get("B"),
                "--transaction.test.topic-c=" + topics.get("C"),
                "--transaction.test.window-seconds=" + windowSeconds,
                "--transaction.test.restore-from-inbox=" + restoreFromInbox,
                "--transaction.test.pause-after-start=" + pauseAfterStart,
                "--transaction.test.b-restore-prefix=" + bRestorePrefix,
                "--transaction.test.window-marker-file=" +
                        (windowMarker == null ? "" : windowMarker.toAbsolutePath()),
                "--spring.kafka.consumer.properties[session.timeout.ms]=6000",
                "--spring.kafka.consumer.properties[heartbeat.interval.ms]=1000");
        builder.environment().put("SPRING_DATASOURCE_URL", POSTGRES.getJdbcUrl("postgres", "postgres"));
        builder.environment().put("SPRING_DATASOURCE_USERNAME", "postgres");
        builder.environment().put("SPRING_DATASOURCE_PASSWORD", "postgres");
        builder.environment().put("SPRING_KAFKA_BOOTSTRAPSERVERS", broker.getBrokersAsString());
        builder.redirectErrorStream(true).redirectOutput(directory.resolve(phase + ".log").toFile());
        return builder.start();
    }

    private void verifyCommittedStartAcrossConsumerRestart(String mode) throws Exception {
        String transactionId = "restart-" + mode + "-" + UUID.randomUUID();
        String topic = "transaction-restart-" + mode + "-" + UUID.randomUUID();
        String group = topic;
        String bootstrap = broker.getBrokersAsString();
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap));
             var producer = new KafkaProducer<String, String>(Map.of(
                     ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                     ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.ACKS_CONFIG, "all"))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);

            // The first record is durably stored and its Kafka offset is committed before restart.
            try (CandidateAdapter initial = adapterForRestart(mode)) {
                var first = restartContainer(topic, group, initial);
                try {
                    first.start();
                    producer.send(new ProducerRecord<>(topic, 0, "ST-BENCH",
                            observed(transactionId, 0))).get(10, TimeUnit.SECONDS);
                    await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                            assertThat(offset(admin, group, topic)).isEqualTo(1));
                    assertThat(countForTransaction("transaction_inbox_event", transactionId)).isEqualTo(1);
                    assertThat(countForTransaction("transaction_session_candidate", transactionId)).isZero();
                    if ("B".equals(mode)) assertThat(initial.windows).containsKey(transactionId);
                } finally {
                    first.stop();
                }
            }

            // A new adapter has no Started flag or scheduled B timer; C rereads the stored source.
            try (CandidateAdapter restarted = adapterForRestart(mode)) {
                var second = restartContainer(topic, group, restarted);
                try {
                    second.start();
                    producer.send(new ProducerRecord<>(topic, 0, "ST-BENCH",
                            observed(transactionId, 1))).get(10, TimeUnit.SECONDS);
                    await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                            assertThat(offset(admin, group, topic)).isEqualTo(2));
                    if ("B".equals(mode)) {
                        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                                assertThat(restarted.windows).isEmpty());
                    }
                    assertThat(countForTransaction("transaction_inbox_event", transactionId)).isEqualTo(2);
                    long candidates = countForTransaction("transaction_session_candidate", transactionId);
                    assertThat(candidates).as("candidate %s after adapter restart", mode)
                            .isEqualTo("C".equals(mode) ? 1 : 0);
                    if ("C".equals(mode)) {
                        assertThat(jdbc.sql("""
                                SELECT energy_wh FROM transaction_session_candidate
                                WHERE transaction_id = :id
                                """).param("id", transactionId).query(Integer.class).single())
                                .isEqualTo(2500);
                    }
                } finally {
                    second.stop();
                }
            }
        }
    }

    private CandidateAdapter adapterForRestart(String mode) {
        return "C".equals(mode) ? null : adapter(mode, 5);
    }

    private CandidateAdapter adapter(String mode, int windowSeconds) {
        return new CandidateAdapter(mode, windowSeconds, inbox, rules, transactions);
    }

    private ConcurrentMessageListenerContainer<String, String> restartContainer(
            String topic, String group, CandidateAdapter adapter) {
        var container = factory.createContainer(topic);
        container.getContainerProperties().setGroupId(group);
        container.getContainerProperties().setPollTimeout(100);
        Map<String, Long> candidateNanos = new ConcurrentHashMap<>();
        container.getContainerProperties().setMessageListener((MessageListener<String, String>) record -> {
            if (adapter == null) productionListener.consume(record);
            else adapter.accept(decoder.decode(record.key(), record.value()), candidateNanos);
        });
        return container;
    }

    private long countForTransaction(String table, String transactionId) {
        return jdbc.sql("SELECT count(*) FROM " + table + " WHERE transaction_id = :id")
                .param("id", transactionId).query(Long.class).single();
    }

    private ModeResult measure(String mode, CandidateAdapter adapter, int expectedCandidates) throws Exception {
        try (CandidateAdapter closeable = adapter; Run run = start(mode, adapter)) {
            return finish(run, adapter, expectedCandidates, Duration.ofSeconds(60));
        }
    }

    private Run start(String mode, CandidateAdapter adapter) throws Exception {
        return start(mode, adapter, 0);
    }

    private Run start(String mode, CandidateAdapter adapter, int requestedRate) throws Exception {
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
        Map<String, Long> candidateNanos = new ConcurrentHashMap<>();
        Set<UUID> lastEventIds = ConcurrentHashMap.newKeySet();
        AtomicInteger processed = new AtomicInteger();
        container.getContainerProperties().setMessageListener((MessageListener<String, String>) record -> {
            if (adapter == null) {
                productionListener.consume(record);
                UUID eventId = UUID.fromString(mapper.readTree(record.value()).get("eventId").textValue());
                if (lastEventIds.contains(eventId)) {
                    String id = mapper.readTree(record.value()).get("payload").get("transactionId").textValue();
                    candidateNanos.put(id, System.nanoTime());
                }
            } else {
                adapter.accept(decoder.decode(record.key(), record.value()), candidateNanos);
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
            pace(startNanos, index * 2, requestedRate);
            firstSentNanos.put(id, System.nanoTime());
            producer.send(new ProducerRecord<>(topic, 0, "ST-BENCH", observed(id, firstSeq)))
                    .get(10, TimeUnit.SECONDS);
            UUID lastId = UUID.randomUUID();
            lastEventIds.add(lastId);
            pace(startNanos, index * 2 + 1, requestedRate);
            producer.send(new ProducerRecord<>(topic, 0, "ST-BENCH", observed(id, lastSeq, lastId)))
                    .get(10, TimeUnit.SECONDS);
        }
        return new Run(mode, topic, group, admin, producer, container, firstSentNanos,
                candidateNanos, processed, startNanos, System.nanoTime());
    }

    private void pace(long startNanos, int recordIndex, int requestedRate) {
        if (requestedRate <= 0) return;
        long due = startNanos + TimeUnit.SECONDS.toNanos(recordIndex) / requestedRate;
        long remaining;
        while ((remaining = due - System.nanoTime()) > 0) LockSupport.parkNanos(remaining);
    }

    private ModeResult finish(Run run, CandidateAdapter adapter, int expectedCandidates, Duration timeout)
            throws Exception {
        await().atMost(timeout).untilAsserted(() -> {
            assertThat(run.processed.get()).isEqualTo(TRANSACTIONS_PER_MODE * 2);
            assertThat(run.candidateNanos.size()).isEqualTo(expectedCandidates);
            assertThat(offset(run.admin, run.group, run.topic)).isEqualTo(TRANSACTIONS_PER_MODE * 2);
        });
        long endNanos = System.nanoTime();
        long committed = offset(run.admin, run.group, run.topic);
        int lag = (int) (TRANSACTIONS_PER_MODE * 2 - committed);
        int candidateCount = jdbc.sql("""
                SELECT count(*) FROM transaction_session_candidate
                WHERE transaction_id LIKE :prefix
                """).param("prefix", "bench-" + run.mode + "-%").query(Long.class).single().intValue();
        int wrongEnergy = jdbc.sql("""
                SELECT count(*) FROM transaction_session_candidate
                WHERE transaction_id LIKE :prefix AND energy_wh <> 2500
                """).param("prefix", "bench-" + run.mode + "-%").query(Long.class).single().intValue();
        Set<String> actualCandidates = new HashSet<>(jdbc.sql("""
                SELECT transaction_id FROM transaction_session_candidate
                WHERE transaction_id LIKE :prefix
                """).param("prefix", "bench-" + run.mode + "-%").query(String.class).list());
        Set<String> expectedCandidatesSet = new HashSet<>();
        for (int index = 0; index < TRANSACTIONS_PER_MODE; index++) {
            if (!"A".equals(run.mode) || index % 2 == 0) {
                expectedCandidatesSet.add("bench-" + run.mode + "-" + index);
            }
        }
        assertThat(actualCandidates).isEqualTo(expectedCandidatesSet);
        List<TimingSample> samples = new ArrayList<>();
        for (int index = 0; index < TRANSACTIONS_PER_MODE; index++) {
            String id = "bench-" + run.mode + "-" + index;
            Long candidateAt = run.candidateNanos.get(id);
            samples.add(new TimingSample(id, index % 2 == 0 ? "NORMAL" : "REVERSE",
                    candidateAt == null ? null : (candidateAt - run.firstSentNanos.get(id)) / 1_000_000.0));
        }
        List<Double> milliseconds = samples.stream().map(TimingSample::decisionMillis)
                .filter(java.util.Objects::nonNull).sorted().toList();
        List<Double> normalMillis = samples.stream().filter(sample -> "NORMAL".equals(sample.pattern()))
                .map(TimingSample::decisionMillis).filter(java.util.Objects::nonNull).sorted().toList();
        List<Double> reverseMillis = samples.stream().filter(sample -> "REVERSE".equals(sample.pattern()))
                .map(TimingSample::decisionMillis).filter(java.util.Objects::nonNull).sorted().toList();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(endNanos - run.startNanos);
        return new ModeResult(run.mode, TRANSACTIONS_PER_MODE, TRANSACTIONS_PER_MODE / 2,
                TRANSACTIONS_PER_MODE / 2, candidateCount, wrongEnergy, lag, elapsedMillis,
                (TRANSACTIONS_PER_MODE * 2) / ((run.lastSendNanos - run.startNanos) / 1_000_000_000.0),
                (TRANSACTIONS_PER_MODE * 2) / ((endNanos - run.startNanos) / 1_000_000_000.0),
                candidateCount / ((endNanos - run.startNanos) / 1_000_000_000.0),
                percentile(milliseconds, .50), percentile(milliseconds, .95), percentile(milliseconds, .99),
                percentile(normalMillis, .50), percentile(normalMillis, .95), percentile(normalMillis, .99),
                percentile(reverseMillis, .50), percentile(reverseMillis, .95), percentile(reverseMillis, .99),
                count("transaction_inbox_event", run.mode), count("transaction_inbox_state", run.mode),
                count("transaction_session_candidate", run.mode),
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

    static final class CandidateAdapter implements AutoCloseable {
        private record WindowEntry(String eventType, Instant occurredAt,
                                   ObservedTransactionRecord lookup) { }
        private record StoredWindowEntry(String eventType, Instant occurredAt,
                                         Instant receivedAt) { }

        private final String mode;
        private final int windowSeconds;
        private final JdbcTransactionInbox inbox;
        private final TransactionRecoveryRules rules;
        private final TransactionTemplate transactions;
        private final JdbcClient jdbc;
        private final boolean restoreFromInbox;
        private final Consumer<String> onWindowCompleted;
        private final Map<String, Boolean> started = new ConcurrentHashMap<>();
        private final Map<String, List<WindowEntry>> windows = new ConcurrentHashMap<>();
        private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        CandidateAdapter(String mode, int windowSeconds, JdbcTransactionInbox inbox,
                         TransactionRecoveryRules rules, TransactionTemplate transactions) {
            this(mode, windowSeconds, inbox, rules, transactions, null, false);
        }

        CandidateAdapter(String mode, int windowSeconds, JdbcTransactionInbox inbox,
                         TransactionRecoveryRules rules, TransactionTemplate transactions,
                         JdbcClient jdbc, boolean restoreFromInbox) {
            this(mode, windowSeconds, inbox, rules, transactions, jdbc, restoreFromInbox,
                    transactionId -> { });
        }

        CandidateAdapter(String mode, int windowSeconds, JdbcTransactionInbox inbox,
                         TransactionRecoveryRules rules, TransactionTemplate transactions,
                         JdbcClient jdbc, boolean restoreFromInbox,
                         Consumer<String> onWindowCompleted) {
            this.mode = mode;
            this.windowSeconds = windowSeconds;
            this.inbox = inbox;
            this.rules = rules;
            this.transactions = transactions;
            this.jdbc = jdbc;
            this.restoreFromInbox = restoreFromInbox;
            this.onWindowCompleted = onWindowCompleted;
        }

        void restorePendingWindows(String transactionIdPrefix,
                                   Map<String, Long> candidateNanos) {
            if (!"B".equals(mode) || !restoreFromInbox || transactionIdPrefix.isBlank()) return;
            // The prefix confines this test-only scan to this consumer's synthetic transactions.
            List<ObservedTransactionRecord> pending = jdbc.sql("""
                    SELECT DISTINCT ON (e.charging_station_id, e.transaction_id)
                      e.first_event_id, e.charging_station_id, e.transaction_id,
                      e.seq_no, e.event_type, e.occurred_at, e.first_received_at,
                      e.source_document::text AS source_text, s.station_id
                    FROM transaction_inbox_event e
                    JOIN transaction_inbox_state s
                      ON s.charging_station_id = e.charging_station_id
                     AND s.transaction_id = e.transaction_id
                    WHERE e.transaction_id LIKE :transactionPrefix
                      AND e.first_received_at >= :cutoff
                      AND NOT EXISTS (
                        SELECT 1 FROM transaction_session_candidate c
                        WHERE c.charging_station_id = e.charging_station_id
                          AND c.transaction_id = e.transaction_id)
                    ORDER BY e.charging_station_id, e.transaction_id,
                             e.first_received_at DESC, e.seq_no DESC
                    """)
                    .param("transactionPrefix", transactionIdPrefix + "%")
                    .param("cutoff", OffsetDateTime.ofInstant(Instant.now().minusSeconds(windowSeconds),
                            java.time.ZoneOffset.UTC))
                    .query((rs, row) -> new ObservedTransactionRecord(
                            rs.getObject("first_event_id", UUID.class), rs.getString("station_id"),
                            rs.getString("charging_station_id"), rs.getString("transaction_id"),
                            rs.getInt("seq_no"), rs.getString("event_type"),
                            rs.getObject("occurred_at", OffsetDateTime.class).toInstant(),
                            rs.getObject("first_received_at", OffsetDateTime.class).toInstant(),
                            rs.getString("source_text"), null))
                    .list();
            pending.forEach(event -> restoreWindow(event, candidateNanos));
        }

        void accept(ObservedTransactionRecord event, Map<String, Long> candidateNanos) {
            AtomicBoolean inserted = new AtomicBoolean();
            boolean created = Boolean.TRUE.equals(transactions.execute(status -> {
                JdbcTransactionInbox.State state = inbox.ensureAndLock(event);
                if (!state.stationId().equals(event.stationId())) return false;
                if (!inbox.insertEvent(event)) {
                    if (!inbox.sameSource(event)) {
                        inbox.preserveConflict(event);
                        inbox.updateState(event, "HOLD_CONFLICT", "SOURCE_CONFLICT", null);
                    }
                    return false;
                }
                inserted.set(true);
                if ("A".equals(mode)) return applyArrival(event);
                return false;
            }));
            if (created) candidateNanos.put(event.transactionId(), System.nanoTime());
            if ("B".equals(mode)) {
                if (restoreFromInbox) {
                    if (inserted.get()) restoreWindow(event, candidateNanos);
                    return;
                }
                windows.compute(event.transactionId(), (id, existing) -> {
                    if (existing == null) {
                        List<WindowEntry> batch = new ArrayList<>();
                        batch.add(new WindowEntry(event.eventType(), event.occurredAt(), event));
                        scheduler.schedule(() -> flush(id, batch, candidateNanos), windowSeconds,
                                TimeUnit.SECONDS);
                        return batch;
                    }
                    synchronized (existing) {
                        existing.add(new WindowEntry(event.eventType(), event.occurredAt(), event));
                    }
                    return existing;
                });
            }
        }

        private void restoreWindow(ObservedTransactionRecord event,
                                   Map<String, Long> candidateNanos) {
            // The source rows and receipt times are durable; the timer and batch are not.
            List<StoredWindowEntry> stored = jdbc.sql("""
                    SELECT event_type, occurred_at, first_received_at
                    FROM transaction_inbox_event
                    WHERE charging_station_id = :chargingStationId AND transaction_id = :transactionId
                    ORDER BY first_received_at, seq_no
                    """)
                    .param("chargingStationId", event.chargingStationId())
                    .param("transactionId", event.transactionId())
                    .query((rs, row) -> new StoredWindowEntry(rs.getString("event_type"),
                            rs.getObject("occurred_at", OffsetDateTime.class).toInstant(),
                            rs.getObject("first_received_at", OffsetDateTime.class).toInstant()))
                    .list();
            Instant anchor = stored.getFirst().receivedAt();
            long windowMillis = TimeUnit.SECONDS.toMillis(windowSeconds);
            long bucket = Math.floorDiv(Duration.between(anchor, event.receivedAt()).toMillis(),
                    windowMillis);
            String key = event.transactionId() + ":" + bucket;
            windows.compute(key, (id, existing) -> {
                if (existing != null) {
                    synchronized (existing) {
                        existing.add(new WindowEntry(event.eventType(), event.occurredAt(), event));
                    }
                    return existing;
                }
                List<WindowEntry> batch = new ArrayList<>();
                for (StoredWindowEntry row : stored) {
                    long rowBucket = Math.floorDiv(Duration.between(anchor, row.receivedAt()).toMillis(),
                            windowMillis);
                    if (rowBucket == bucket) {
                        batch.add(new WindowEntry(row.eventType(), row.occurredAt(), event));
                    }
                }
                Instant deadline = anchor.plusMillis((bucket + 1) * windowMillis);
                long delayMillis = Math.max(0, Duration.between(Instant.now(), deadline).toMillis());
                scheduler.schedule(() -> flush(id, batch, candidateNanos), delayMillis,
                        TimeUnit.MILLISECONDS);
                return batch;
            });
        }

        private boolean applyArrival(ObservedTransactionRecord event) {
            if ("Started".equals(event.eventType())) {
                started.put(event.transactionId(), true);
                inbox.updateState(event, "OPEN", null, 1);
                return false;
            }
            boolean sawStart = Boolean.TRUE.equals(started.get(event.transactionId()))
                    || restoreFromInbox && inbox.events(event).stream()
                    .anyMatch(stored -> "Started".equals(stored.eventType()));
            if (!"Ended".equals(event.eventType()) || !sawStart) {
                return false;
            }
            TransactionRecoveryRules.Outcome outcome = rules.evaluate(inbox.events(event));
            if (!"CALCULATED".equals(outcome.status())) {
                inbox.updateState(event, "HOLD", outcome.reason(), null);
                return false;
            }
            inbox.insertCandidate(event, outcome);
            inbox.updateState(event, "PROVISIONAL", null, outcome.evseId());
            return true;
        }

        private void flush(String id, List<WindowEntry> batch,
                           Map<String, Long> candidateNanos) {
            List<WindowEntry> copy;
            synchronized (batch) { copy = new ArrayList<>(batch); }
            windows.remove(id, batch);
            copy.sort(Comparator.comparing(WindowEntry::occurredAt));
            boolean sawStart = false;
            for (WindowEntry entry : copy) {
                ObservedTransactionRecord event = entry.lookup();
                if ("Started".equals(entry.eventType())) sawStart = true;
                if ("Ended".equals(entry.eventType()) && sawStart) {
                    boolean created = Boolean.TRUE.equals(transactions.execute(status -> {
                        inbox.ensureAndLock(event);
                        TransactionRecoveryRules.Outcome outcome = rules.evaluate(inbox.events(event));
                        if (!"CALCULATED".equals(outcome.status())) {
                            inbox.updateState(event, "HOLD", outcome.reason(), null);
                            return false;
                        }
                        inbox.insertCandidate(event, outcome);
                        inbox.updateState(event, "PROVISIONAL", null, outcome.evseId());
                        return true;
                    }));
                    if (created) candidateNanos.put(event.transactionId(), System.nanoTime());
                }
            }
            onWindowCompleted.accept(copy.getFirst().lookup().transactionId());
        }

        @Override public void close() {
            scheduler.shutdownNow();
        }
    }

    private record Run(String mode, String topic, String group, AdminClient admin,
                       KafkaProducer<String, String> producer,
                       org.springframework.kafka.listener.ConcurrentMessageListenerContainer<String, String> container,
                       Map<String, Long> firstSentNanos, Map<String, Long> candidateNanos,
                       AtomicInteger processed, long startNanos, long lastSendNanos) implements AutoCloseable {
        @Override public void close() {
            container.stop();
            producer.close();
            admin.close();
        }
    }
}
