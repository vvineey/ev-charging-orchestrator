package com.evcharging.controlplane.transaction;

import com.sun.management.OperatingSystemMXBean;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Opt-in bounded-memory CDC soak experiment. Never use this as an operational load generator. */
class Exp0003OutboxSoakTest {
    private static final String BOOTSTRAP = "127.0.0.1:19093";
    private static final String TOPIC = "exp0003.outbox";
    private static final String CONNECT = "http://127.0.0.1:18083";
    private static final int MAX_PENDING = 100_000;
    private static final long MIN_DISK_FREE_BYTES = 10L * 1024 * 1024 * 1024;
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void maintainsFixedInputWithBoundedMemoryAndMinuteCheckpoints() throws Exception {
        assumeTrue(Boolean.getBoolean("exp0003.soak.enabled"), "Explicit EXP-0003 isolated soak opt-in required");
        int rate = Integer.parseInt(System.getProperty("exp0003.soak.rate"));
        int seconds = Integer.parseInt(System.getProperty("exp0003.soak.seconds"));
        String pollMs = System.getProperty("exp0003.soak.cdc-poll-ms");
        if (rate < 1 || seconds < 60 || (long) rate * seconds > 5_000_000 || !pollMs.equals("50")) {
            throw new IllegalArgumentException("Soak requires 1..5000000 events, >=60 seconds, CDC poll 50ms");
        }
        int expected = Math.multiplyExact(rate, seconds);
        Path resultPath = Path.of(System.getProperty("exp0003.soak.output"));
        Path checkpointPath = Path.of(System.getProperty("exp0003.soak.checkpoints"));
        Files.createDirectories(resultPath.getParent());
        Files.deleteIfExists(checkpointPath);

        var poolConfig = new HikariConfig();
        poolConfig.setJdbcUrl("jdbc:postgresql://127.0.0.1:15439/exp0003");
        poolConfig.setUsername("postgres");
        poolConfig.setPassword("exp0003_only");
        poolConfig.setMaximumPoolSize(16);
        poolConfig.setMinimumIdle(16);
        poolConfig.setPoolName("exp0003-soak-input");
        try (var dataSource = new HikariDataSource(poolConfig)) {
            Exp0003OutboxFixture.createSchema(dataSource);
            try (var admin = AdminClient.create(Map.of("bootstrap.servers", BOOTSTRAP))) {
                admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            }
            waitForRestReady();
            registerConnector(pollMs);
            waitForConnector();
            verifyConnectorPollInterval(pollMs);
            waitForSlot(dataSource);

            var committed = new AtomicInteger();
            var observedUnique = new AtomicInteger();
            var observedPhysical = new AtomicInteger();
            var appliedEffects = new AtomicInteger();
            var duplicateRecords = new AtomicInteger();
            var maxBacklog = new AtomicInteger();
            var fatal = new AtomicReference<Throwable>();
            var commitService = new Histogram();
            var scheduleSlip = new Histogram();
            var lifetimeLatency = new Histogram();
            var minuteLatency = new AtomicReference<>(new Histogram());
            var pending = new ConcurrentHashMap<Integer, Timing>();
            var negativeLatency = new AtomicInteger();
            var startSignal = new CountDownLatch(1);
            var consumerReady = new CountDownLatch(1);
            var writers = new ThreadPoolExecutor(8, 8, 0, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(1_000), new ThreadPoolExecutor.AbortPolicy());
            writers.prestartAllCoreThreads();
            var reader = Executors.newSingleThreadExecutor();
            var sampler = Executors.newSingleThreadScheduledExecutor();
            var os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            var plannedEndRef = new AtomicLong();
            try {
                Future<?> reading = reader.submit(() -> {
                    var partition = new TopicPartition(TOPIC, 0);
                    try (var consumer = new KafkaConsumer<String, String>(Map.of(
                            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP,
                            ConsumerConfig.GROUP_ID_CONFIG, "exp0003-soak-" + UUID.randomUUID(),
                            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"));
                            Connection effectConnection = dataSource.getConnection();
                            PreparedStatement effectInsert = effectConnection.prepareStatement("""
                                    INSERT INTO exp0003_effect(event_id, work_key) VALUES (?, ?)
                                    ON CONFLICT (event_id) DO NOTHING
                                    """)) {
                        effectConnection.setAutoCommit(false);
                        consumer.assign(List.of(partition));
                        consumer.seekToBeginning(List.of(partition));
                        consumer.poll(Duration.ofMillis(1));
                        consumerReady.countDown();
                        startSignal.await();
                        long deadline = plannedEndRef.get() + TimeUnit.MINUTES.toNanos(5);
                        var seen = new BitSet(expected);
                        while (observedUnique.get() < expected && System.nanoTime() < deadline && fatal.get() == null) {
                            int pendingEffects = 0;
                            for (var record : consumer.poll(Duration.ofMillis(100))) {
                                long observedNs = System.nanoTime();
                                observedPhysical.incrementAndGet();
                                var payload = mapper.readTree(record.value());
                                String key = payload.path("workKey").asText();
                                int index = workIndex(key);
                                if (index < 0 || index >= expected || !key.equals(workKey(index))) {
                                    throw new IllegalStateException("Unexpected work key at Kafka offset " + record.offset());
                                }
                                UUID id = UUID.nameUUIDFromBytes(("exp0003:" + key).getBytes(StandardCharsets.UTF_8));
                                if (!key.equals(record.key()) || !id.toString().equals(payload.path("eventId").asText())
                                        || payload.path("version").asInt() != 1) {
                                    throw new IllegalStateException("Unexpected Kafka payload at offset " + record.offset());
                                }
                                if (seen.get(index)) {
                                    duplicateRecords.incrementAndGet();
                                    continue;
                                }
                                seen.set(index);
                                pair(pending, index, observedNs, true, lifetimeLatency,
                                        minuteLatency, negativeLatency);
                                observedUnique.incrementAndGet();
                                effectInsert.setObject(1, id);
                                effectInsert.setString(2, key);
                                effectInsert.addBatch();
                                pendingEffects++;
                            }
                            if (pendingEffects > 0) {
                                int[] counts = effectInsert.executeBatch();
                                effectConnection.commit();
                                for (int count : counts) {
                                    if (count == 1 || count == PreparedStatement.SUCCESS_NO_INFO) {
                                        appliedEffects.incrementAndGet();
                                    }
                                }
                            }
                            updateBacklog(maxBacklog, committed.get(), observedUnique.get());
                        }
                        if (observedUnique.get() != expected) {
                            throw new IllegalStateException("Kafka drain incomplete: " + observedUnique.get() + "/" + expected);
                        }
                    } catch (Throwable failure) {
                        fatal.compareAndSet(null, failure);
                        throw new IllegalStateException("CDC soak consumer failed", failure);
                    } finally {
                        consumerReady.countDown();
                    }
                });
                assertThat(consumerReady.await(30, TimeUnit.SECONDS)).isTrue();
                if (fatal.get() != null) throw new IllegalStateException("Consumer startup failed", fatal.get());
                long cpuStartNs = os.getProcessCpuTime();
                long startEpochMs = System.currentTimeMillis();
                long startNs = System.nanoTime();
                long plannedEndNs = startNs + TimeUnit.SECONDS.toNanos(seconds);
                plannedEndRef.set(plannedEndNs);
                sampler.scheduleAtFixedRate(() -> {
                    try {
                        writeCheckpoint(checkpointPath, dataSource, os, startNs, rate, seconds,
                                committed, observedUnique, observedPhysical, appliedEffects, maxBacklog,
                                pending, lifetimeLatency, minuteLatency.getAndSet(new Histogram()),
                                commitService, scheduleSlip, fatal);
                    } catch (Throwable failure) {
                        fatal.compareAndSet(null, failure);
                    }
                }, 60, 60, TimeUnit.SECONDS);
                startSignal.countDown();

                int submitted = 0;
                for (int index = 0; index < expected && fatal.get() == null; index++) {
                    long scheduledNs = startNs + (index * 1_000_000_000L) / rate;
                    while (System.nanoTime() < scheduledNs && fatal.get() == null) {
                        LockSupport.parkNanos(Math.min(scheduledNs - System.nanoTime(),
                                TimeUnit.MILLISECONDS.toNanos(1)));
                    }
                    int workIndex = index;
                    try {
                        writers.execute(() -> {
                            try {
                                long attemptNs = System.nanoTime();
                                scheduleSlip.record(Math.max(0, attemptNs - scheduledNs));
                                UUID id = Exp0003OutboxFixture.commitWork(dataSource, workKey(workIndex), false);
                                long committedNs = System.nanoTime();
                                commitService.record(committedNs - attemptNs);
                                if (!id.equals(UUID.nameUUIDFromBytes(("exp0003:" + workKey(workIndex))
                                        .getBytes(StandardCharsets.UTF_8)))) {
                                    throw new IllegalStateException("Unexpected committed event ID");
                                }
                                pair(pending, workIndex, committedNs, false, lifetimeLatency,
                                        minuteLatency, negativeLatency);
                                committed.incrementAndGet();
                                updateBacklog(maxBacklog, committed.get(), observedUnique.get());
                                if (pending.size() > MAX_PENDING) {
                                    throw new IllegalStateException("More than " + MAX_PENDING + " pending timings");
                                }
                            } catch (Throwable failure) {
                                fatal.compareAndSet(null, failure);
                            }
                        });
                        submitted++;
                    } catch (Throwable failure) {
                        fatal.compareAndSet(null, failure);
                    }
                }
                writers.shutdown();
                while (System.nanoTime() < plannedEndNs && fatal.get() == null) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                int committedAtPlannedEnd = committed.get();
                assertThat(writers.awaitTermination(300, TimeUnit.SECONDS)).isTrue();
                if (fatal.get() != null) throw new IllegalStateException("Soak run failed", fatal.get());
                assertThat(submitted).isEqualTo(expected);
                assertThat(committedAtPlannedEnd).isGreaterThanOrEqualTo((int) Math.ceil(expected * 0.95));
                reading.get(5, TimeUnit.MINUTES);
                sampler.shutdown();
                assertThat(sampler.awaitTermination(2, TimeUnit.MINUTES)).isTrue();
                writeCheckpoint(checkpointPath, dataSource, os, startNs, rate, seconds,
                        committed, observedUnique, observedPhysical, appliedEffects, maxBacklog,
                        pending, lifetimeLatency, minuteLatency.getAndSet(new Histogram()),
                        commitService, scheduleSlip, fatal);
                if (fatal.get() != null) throw new IllegalStateException("Soak checkpoint failed", fatal.get());

                long brokerEndOffset;
                try (var admin = AdminClient.create(Map.of("bootstrap.servers", BOOTSTRAP))) {
                    brokerEndOffset = admin.listOffsets(Map.of(new TopicPartition(TOPIC, 0), OffsetSpec.latest()))
                            .all().get(10, TimeUnit.SECONDS).get(new TopicPartition(TOPIC, 0)).offset();
                }
                long workCount = Exp0003OutboxFixture.count(dataSource, "exp0003_work");
                long outboxCount = Exp0003OutboxFixture.count(dataSource, "exp0003_outbox");
                long effectCount = Exp0003OutboxFixture.count(dataSource, "exp0003_effect");
                long missingEffects = Exp0003OutboxFixture.missingEffects(dataSource);
                long ghostEffects = Exp0003OutboxFixture.ghostEffects(dataSource);
                assertThat(workCount).isEqualTo(expected);
                assertThat(outboxCount).isEqualTo(expected);
                assertThat(brokerEndOffset).isEqualTo(expected);
                assertThat(observedPhysical.get()).isEqualTo(expected);
                assertThat(observedUnique.get()).isEqualTo(expected);
                assertThat(effectCount).isEqualTo(expected);
                assertThat(appliedEffects.get()).isEqualTo(expected);
                assertThat(duplicateRecords.get()).isZero();
                assertThat(missingEffects).isZero();
                assertThat(ghostEffects).isZero();
                assertThat(negativeLatency.get()).isZero();
                assertThat(pending).isEmpty();

                var result = new LinkedHashMap<String, Object>();
                result.put("status", "PASSED");
                result.put("scope", "isolated synthetic CDC soak");
                result.put("codeCommit", System.getProperty("exp0003.soak.code-commit"));
                result.put("inputStartedAtEpochMillis", startEpochMs);
                result.put("ratePerSecond", rate);
                result.put("durationSeconds", seconds);
                result.put("cdcPollIntervalMs", pollMs);
                result.put("logicalStations", 2000);
                result.put("logicalPortsPerStation", 5);
                result.put("scheduledEvents", expected);
                result.put("submittedEvents", submitted);
                result.put("committedAtPlannedEnd", committedAtPlannedEnd);
                result.put("workRows", workCount);
                result.put("outboxRows", outboxCount);
                result.put("kafkaPhysicalRecords", brokerEndOffset);
                result.put("observedUniqueEvents", observedUnique.get());
                result.put("effectRows", effectCount);
                result.put("duplicateRecords", duplicateRecords.get());
                result.put("missingEffects", missingEffects);
                result.put("ghostEffects", ghostEffects);
                result.put("maxObservedBacklog", maxBacklog.get());
                result.put("latencyResolutionMs", 1);
                result.put("commitToKafkaP50Ms", lifetimeLatency.percentile(0.50));
                result.put("commitToKafkaP95Ms", lifetimeLatency.percentile(0.95));
                result.put("commitToKafkaP99Ms", lifetimeLatency.percentile(0.99));
                result.put("commitToKafkaMaxMs", lifetimeLatency.maxMillis());
                result.put("scheduleSlipP95Ms", scheduleSlip.percentile(0.95));
                result.put("commitServiceP95Ms", commitService.percentile(0.95));
                result.put("testJvmCpuOneCorePercent", 100.0 * (os.getProcessCpuTime() - cpuStartNs)
                        / TimeUnit.SECONDS.toNanos(seconds));
                result.put("finalRetainedWalBytes", retainedWalBytes(dataSource));
                result.put("finalDatabaseSizeBytes", databaseSizeBytes(dataSource));
                result.put("checkpointFile", checkpointPath.getFileName().toString());
                Files.writeString(resultPath, mapper.writeValueAsString(result));
            } finally {
                startSignal.countDown();
                sampler.shutdownNow();
                writers.shutdownNow();
                reader.shutdownNow();
            }
        }
    }

    private void writeCheckpoint(Path path, DataSource dataSource, OperatingSystemMXBean os,
            long startNs, int rate, int seconds, AtomicInteger committed, AtomicInteger observedUnique,
            AtomicInteger observedPhysical, AtomicInteger appliedEffects, AtomicInteger maxBacklog,
            ConcurrentHashMap<Integer, Timing> pending, Histogram lifetime, Histogram minute,
            Histogram commitService, Histogram scheduleSlip, AtomicReference<Throwable> fatal) throws Exception {
        long sampledAtEpochMillis = System.currentTimeMillis();
        double elapsedSeconds = (System.nanoTime() - startNs) / 1_000_000_000.0;
        int committedSnapshot = committed.get();
        int observedSnapshot = observedUnique.get();
        int physicalSnapshot = observedPhysical.get();
        int effectsSnapshot = appliedEffects.get();
        int maxBacklogSnapshot = maxBacklog.get();
        int pendingSnapshot = pending.size();
        double processCpuSeconds = os.getProcessCpuTime() / 1_000_000_000.0;
        long heapUsedBytes = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long dockerFreeBytes = dockerFreeBytes();
        var checkpoint = new LinkedHashMap<String, Object>();
        checkpoint.put("sampledAtEpochMillis", sampledAtEpochMillis);
        checkpoint.put("elapsedSeconds", elapsedSeconds);
        checkpoint.put("expectedCommittedByNow", Math.min((long) rate * seconds,
                (long) Math.floor(elapsedSeconds * rate)));
        checkpoint.put("committed", committedSnapshot);
        checkpoint.put("observedUnique", observedSnapshot);
        checkpoint.put("observedPhysical", physicalSnapshot);
        checkpoint.put("appliedEffects", effectsSnapshot);
        checkpoint.put("observedBacklog", Math.max(0, committedSnapshot - observedSnapshot));
        checkpoint.put("maxObservedBacklog", maxBacklogSnapshot);
        checkpoint.put("pendingTimings", pendingSnapshot);
        checkpoint.put("minuteLatencyCount", minute.count());
        checkpoint.put("minuteCommitToKafkaP95Ms", minute.percentile(0.95));
        checkpoint.put("cumulativeCommitToKafkaP50Ms", lifetime.percentile(0.50));
        checkpoint.put("cumulativeCommitToKafkaP95Ms", lifetime.percentile(0.95));
        checkpoint.put("cumulativeCommitToKafkaP99Ms", lifetime.percentile(0.99));
        checkpoint.put("scheduleSlipP95Ms", scheduleSlip.percentile(0.95));
        checkpoint.put("commitServiceP95Ms", commitService.percentile(0.95));
        checkpoint.put("testJvmProcessCpuSeconds", processCpuSeconds);
        checkpoint.put("testJvmHeapUsedBytes", heapUsedBytes);
        checkpoint.put("cdcRetainedWalBytes", retainedWalBytes(dataSource));
        checkpoint.put("databaseSizeBytes", databaseSizeBytes(dataSource));
        checkpoint.put("dockerFreeBytes", dockerFreeBytes);
        Files.writeString(path, mapper.writeValueAsString(checkpoint) + "\n",
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        if (dockerFreeBytes < MIN_DISK_FREE_BYTES) {
            fatal.compareAndSet(null, new IllegalStateException("Docker free disk below 10 GiB"));
        }
        if (elapsedSeconds >= 300 && elapsedSeconds < 360
                && committedSnapshot < (long) rate * 300 * 0.90) {
            fatal.compareAndSet(null, new IllegalStateException("First five-minute input below 90% target"));
        }
        if (pendingSnapshot > MAX_PENDING) {
            fatal.compareAndSet(null, new IllegalStateException("Pending timings exceeded " + MAX_PENDING));
        }
    }

    private long dockerFreeBytes() throws Exception {
        var command = new ProcessBuilder("docker", "compose", "-f",
                System.getProperty("exp0003.cdc.compose-file"), "exec", "-T", "postgres",
                "df", "-Pk", "/").redirectErrorStream(true).start();
        String output = new String(command.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!command.waitFor(10, TimeUnit.SECONDS) || command.exitValue() != 0) {
            throw new IOException("Docker disk check failed: " + output);
        }
        String[] lines = output.strip().split("\\R");
        if (lines.length < 2) throw new IOException("Unexpected Docker df output: " + output);
        String[] columns = lines[1].trim().split("\\s+");
        if (columns.length < 4) throw new IOException("Unexpected Docker df columns: " + output);
        return Long.parseLong(columns[3]) * 1024;
    }

    private static void pair(ConcurrentHashMap<Integer, Timing> pending, int index, long atNs,
            boolean observation, Histogram lifetime, AtomicReference<Histogram> minute,
            AtomicInteger negativeLatency) {
        pending.compute(index, (ignored, first) -> {
            if (first == null) return new Timing(observation, atNs);
            if (first.observation == observation) throw new IllegalStateException("Repeated timing side " + index);
            long delta = observation ? atNs - first.atNs : first.atNs - atNs;
            if (delta < 0) negativeLatency.incrementAndGet();
            else {
                lifetime.record(delta);
                minute.get().record(delta);
            }
            return null;
        });
    }

    private static void updateBacklog(AtomicInteger maxBacklog, int committed, int observed) {
        maxBacklog.accumulateAndGet(Math.max(0, committed - observed), Math::max);
    }

    private record Timing(boolean observation, long atNs) {}

    /** One-millisecond buckets up to 60 seconds; overflow and true maximum remain visible. */
    private static final class Histogram {
        private final long[] buckets = new long[60_002];
        private long count;
        private long maxNs;

        synchronized void record(long nanoseconds) {
            if (nanoseconds < 0) throw new IllegalArgumentException("negative histogram input");
            int bucket = (int) Math.min(buckets.length - 1, TimeUnit.NANOSECONDS.toMillis(nanoseconds));
            buckets[bucket]++;
            count++;
            maxNs = Math.max(maxNs, nanoseconds);
        }

        synchronized long count() { return count; }

        synchronized long maxMillis() { return TimeUnit.NANOSECONDS.toMillis(maxNs); }

        synchronized long percentile(double fraction) {
            if (count == 0) return -1;
            long target = (long) Math.ceil(count * fraction);
            long seen = 0;
            for (int i = 0; i < buckets.length; i++) {
                seen += buckets[i];
                if (seen >= target) return i;
            }
            throw new IllegalStateException("Histogram count mismatch");
        }
    }

    private static String workKey(int index) {
        int portIndex = index % 10_000;
        return "station-%04d-port-%d-work-%06d".formatted(portIndex / 5, portIndex % 5 + 1, index);
    }

    private static int workIndex(String key) {
        int position = key.lastIndexOf("-work-");
        if (position < 0) return -1;
        try { return Integer.parseInt(key.substring(position + 6)); }
        catch (NumberFormatException invalid) { return -1; }
    }

    private long retainedWalBytes(DataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection();
                var statement = connection.createStatement();
                var rows = statement.executeQuery("""
                        SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
                        FROM pg_replication_slots WHERE slot_name = 'exp0003_outbox_slot'
                        """)) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }

    private long databaseSizeBytes(DataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection();
                var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT pg_database_size(current_database())")) {
            rows.next();
            return rows.getLong(1);
        }
    }

    private void registerConnector(String pollMs) throws Exception {
        var request = (ObjectNode) mapper.readTree(Files.readString(
                Path.of(System.getProperty("exp0003.cdc.connector-file"))));
        ((ObjectNode) request.path("config")).put("poll.interval.ms", pollMs);
        var response = http.send(HttpRequest.newBuilder(URI.create(CONNECT + "/connectors"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(request))).build(),
                HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 201) throw new IllegalStateException(
                "Connector registration HTTP " + response.statusCode() + ": " + response.body());
    }

    private void verifyConnectorPollInterval(String pollMs) throws Exception {
        var response = http.send(HttpRequest.newBuilder(
                        URI.create(CONNECT + "/connectors/exp0003-outbox-cdc/config"))
                        .timeout(Duration.ofSeconds(3)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(mapper.readTree(response.body()).path("poll.interval.ms").asText()).isEqualTo(pollMs);
    }

    private void waitForRestReady() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            try {
                var response = http.send(HttpRequest.newBuilder(URI.create(CONNECT + "/connectors"))
                                .timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() == 200) return;
            } catch (IOException ignored) { /* Connect starts after Kafka. */ }
            Thread.sleep(250);
        }
        throw new IllegalStateException("Connect REST API did not become ready within 60 seconds");
    }

    private void waitForConnector() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (System.nanoTime() < deadline) {
            try {
                var response = http.send(HttpRequest.newBuilder(
                                URI.create(CONNECT + "/connectors/exp0003-outbox-cdc/status"))
                                .timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    var status = mapper.readTree(response.body());
                    var tasks = status.path("tasks");
                    String taskState = tasks.isArray() && tasks.size() > 0
                            ? tasks.get(0).path("state").asText() : "STARTING";
                    if (status.path("connector").path("state").asText().equals("RUNNING")
                            && taskState.equals("RUNNING")) return;
                    if (taskState.equals("FAILED")) throw new IllegalStateException(response.body());
                }
            } catch (IOException ignored) { /* Connect can briefly close its socket. */ }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Connect did not become RUNNING within 90 seconds");
    }

    private void waitForSlot(DataSource dataSource) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try (Connection connection = dataSource.getConnection();
                    var statement = connection.createStatement();
                    var rows = statement.executeQuery("""
                            SELECT count(*) FROM pg_replication_slots
                            WHERE slot_name = 'exp0003_outbox_slot'
                            """)) {
                rows.next();
                if (rows.getLong(1) == 1) return;
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("CDC slot was not created before input");
    }
}
